use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, SpawnHandle,
    WrapFuture, clock::interval,
};
use chrono::{DateTime, Utc};
use futures_util::FutureExt as _;
use futures_util::future::join_all;
use tracing::{Instrument, debug, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::{Heater, Thermostat},
    system::coordinator_actor::CoordinatorActor,
    system::devices::{DeviceType, Devices},
    system::messages::{EvaluateMessage, GetSensorReadingMessage, SensorReading, StopMessage},
    system::thermostat::decision::{
        Decision, HeaterCommand, Presence, PresenceError, Reason, decide, person_present,
    },
    telemetry::{consumer_span, producer_span, record_error},
};

#[derive(Debug)]
pub struct ThermostatActor {
    coordinator_actor_addr: Addr<CoordinatorActor>,
    devices: Devices,
    config: Thermostat,
    /// The tick's work in flight, if any: the evaluation and the switch that follows it. The
    /// next tick cancels it, so that a slow one can
    /// neither hold the thermostat for several intervals nor finish after a newer one and switch
    /// the heaters on older inputs.
    in_flight: Option<SpawnHandle>,
    /// The command of the last evaluation that completed, which the deadband repeats. `None`
    /// before the first one, so that a start inside the deadband turns the heaters off.
    last_command: Option<HeaterCommand>,
}

impl ThermostatActor {
    #[instrument(name = "ThermostatActor::new", skip_all, fields(
        thermostat.name = %config.name,
    ))]
    pub fn new(
        coordinator_actor_addr: Addr<CoordinatorActor>,
        devices: Devices,
        config: Thermostat,
    ) -> Self {
        Self {
            coordinator_actor_addr,
            devices,
            config,
            in_flight: None,
            last_command: None,
        }
    }

    /// Cancels the tick's work in flight, if any. A cancelled tick leaves the heaters as they
    /// are, so the caller asserts them anew.
    fn cancel_in_flight(&mut self, ctx: &mut Context<Self>) {
        if let Some(in_flight) = self.in_flight.take() {
            warn!("The previous tick's work is still running, cancelling it");
            ctx.cancel_future(in_flight);
        }
    }

    /// Records a decision on the tick's span and in the log, the one place that names its fields.
    fn record_decision(span: &tracing::Span, name: &str, decision: Decision) {
        span.record(
            "thermostat.command",
            tracing::field::display(decision.command),
        );
        span.record(
            "thermostat.reason",
            tracing::field::display(decision.reason),
        );

        info!(
            "Thermostat '{name}': {} ({})",
            decision.command, decision.reason
        );
    }

    /// One tick's inputs and decision. The handler switches the heaters once it has remembered
    /// the command, so that a tick cancelled mid-switch still leaves the command it decided for
    /// the deadband to repeat.
    ///
    /// A camera failure is recorded on the span but does not fail the tick: it is a normal `Off`
    /// decision.
    async fn evaluate(
        coordinator_actor_addr: Addr<CoordinatorActor>,
        devices: Devices,
        config: Thermostat,
        last_command: Option<HeaterCommand>,
    ) -> Decision {
        let span = tracing::Span::current();
        let now = Utc::now();

        // Outside the active period nothing is queried and the heaters are switched off.
        if !config.is_active(now) {
            let decision = Decision::new(HeaterCommand::Off, Reason::OutsideActivePeriod);
            Self::record_decision(&span, &config.name, decision);

            return decision;
        }

        // The two inputs are independent, so they are fetched together.
        let (reading, presence) = tokio::join!(
            Self::query_reading(&coordinator_actor_addr, &config),
            Self::query_presence(&devices, &config, now),
        );

        if let Ok(person_detected) = presence {
            span.record("thermostat.person_detected", person_detected);
        }
        if let Some(reading) = &reading {
            span.record("thermostat.temperature", reading.temperature);
            span.record(
                "thermostat.reading_age_s",
                (now - reading.received_at).num_seconds(),
            );
        }

        let decision = decide(now, presence, reading.as_ref(), last_command, &config);
        Self::record_decision(&span, &config.name, decision);

        decision
    }

    /// The heaters are independent, so they are switched together: the time taken is that of
    /// the slowest plug rather than the sum, which matters for the tick deadline. A plug that
    /// could not be switched is recorded and asserted again next tick.
    async fn switch_heaters(devices: Devices, heaters: Vec<Heater>, on: bool) {
        join_all(
            heaters
                .iter()
                .map(|heater| Self::switch_heater(&devices, heater, on)),
        )
        .await;
    }

    async fn query_reading(
        coordinator_actor_addr: &Addr<CoordinatorActor>,
        config: &Thermostat,
    ) -> Option<SensorReading> {
        let result = coordinator_actor_addr
            .send(GetSensorReadingMessage {
                span_context: tracing::Span::current().context(),
                location: config.sensor_location.clone(),
            })
            .await;

        match result {
            Ok(reading) => reading,
            Err(e) => {
                record_error!(&e);
                None
            }
        }
    }

    #[instrument(name = "ThermostatActor::query_presence", skip_all, fields(
        camera.ip_address = %config.camera_ip_address,
    ))]
    async fn query_presence(
        devices: &Devices,
        config: &Thermostat,
        now: DateTime<Utc>,
    ) -> Presence {
        let window_start = now - chrono::Duration::seconds(config.presence_window_s as i64);

        // The camera is only ever queried here, and a tick cancels the previous one, so it has
        // no session to share.
        let result = async {
            let handler = devices
                .config()
                .client()
                .c120(config.camera_ip_address.clone())
                .await?;

            handler.get_detections(window_start, now).await
        }
        .await;

        match result {
            Ok(detections) => {
                debug!(
                    "Thermostat '{}': {} detections in the presence window",
                    config.name,
                    detections.len()
                );
                Ok(person_present(&detections, window_start))
            }
            Err(e) => {
                record_error!(&e);
                Err(PresenceError)
            }
        }
    }

    #[instrument(name = "ThermostatActor::switch_heater", skip_all, fields(
        heater.name = %heater.name,
        heater.ip_address = %heater.ip_address,
        heater.on = on,
    ))]
    async fn switch_heater(devices: &Devices, heater: &Heater, on: bool) {
        let result = devices
            .run(
                DeviceType::PlugEnergyMonitoring,
                &heater.ip_address,
                |handler| {
                    if on {
                        handler.on().boxed_local()
                    } else {
                        handler.off().boxed_local()
                    }
                },
            )
            .await;

        match result {
            Ok(()) => info!(
                "Switched heater '{}' {}",
                heater.name,
                if on { "on" } else { "off" }
            ),
            Err(e) => record_error!(&e),
        }
    }
}

impl Actor for ThermostatActor {
    type Context = Context<Self>;

    #[instrument(name = "ThermostatActor::started", skip_all, fields(
        thermostat.name = %self.config.name,
    ))]
    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();
        let interval_duration = Duration::from_secs(self.config.interval_s);
        let thermostat_name = self.config.name.clone();

        if self.config.heaters.is_empty() {
            warn!("Thermostat '{thermostat_name}' has no heaters configured");
        }

        let fut = async move {
            let mut interval = interval(interval_duration);

            loop {
                interval.tick().await;

                let span = producer_span!(
                    name = "ThermostatActor::IntervalTick",
                    message = EvaluateMessage,
                    destination = "ThermostatActor",
                    thermostat.name = %thermostat_name,
                );
                let _enter = span.enter();

                if let Err(e) = addr.try_send(EvaluateMessage {
                    span_context: span.context(),
                }) {
                    record_error!(&e);
                }
            }
        }
        .into_actor(self);

        ctx.spawn(fut);
    }

    #[instrument(name = "ThermostatActor::stopped", level = "error", skip_all, fields(
        thermostat.name = %self.config.name,
    ))]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<StopMessage> for ThermostatActor {
    type Result = ();

    /// Stops without touching the heaters: the next start asserts them on its first tick.
    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}

impl Handler<EvaluateMessage> for ThermostatActor {
    type Result = ();

    fn handle(&mut self, message: EvaluateMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "ThermostatActor::Handler<EvaluateMessage>",
            message = EvaluateMessage,
            parent = message.span_context,
            destination = "ThermostatActor",
            thermostat.name = %self.config.name,
            thermostat.person_detected = tracing::field::Empty,
            thermostat.temperature = tracing::field::Empty,
            thermostat.reading_age_s = tracing::field::Empty,
            thermostat.command = tracing::field::Empty,
            thermostat.reason = tracing::field::Empty,
        );
        let _enter = span.enter();

        // Every tick asserts the heaters anew, so whatever the previous one left running is
        // cancelled before it can switch them on older inputs.
        self.cancel_in_flight(ctx);

        let switch_span = span.clone();

        let fut = Self::evaluate(
            self.coordinator_actor_addr.clone(),
            self.devices.clone(),
            self.config.clone(),
            self.last_command,
        )
        .instrument(span.clone())
        .into_actor(self)
        .then(move |decision, act, _| {
            // Remembered before the switch, which can be cancelled by the next tick.
            act.last_command = Some(decision.command);

            Self::switch_heaters(
                act.devices.clone(),
                act.config.heaters.clone(),
                decision.command == HeaterCommand::On,
            )
            .instrument(switch_span)
            .into_actor(act)
        })
        .map(|_, act, _| act.in_flight = None);

        self.in_flight = Some(ctx.spawn(fut));
    }
}
