use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    SpawnHandle, WrapFuture, clock::interval,
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

/// Bound on switching the heaters off at a stop. It covers a wait for a slow request already
/// running on a plug plus the switch itself, and has to leave room inside the actors' shutdown
/// timeout in `main` for the other actors to drain after the thermostats, the API's graceful
/// stop among them.
const STOP_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug)]
pub struct ThermostatActor {
    coordinator_actor_addr: Addr<CoordinatorActor>,
    devices: Devices,
    config: Thermostat,
    /// The tick's work in flight, if any: an evaluation, or the switch-off outside the active
    /// period. The next tick cancels it, so that a slow one can neither hold the thermostat for
    /// several intervals nor finish after a newer one and switch the heaters on older inputs; a
    /// stop cancels it, so that it cannot switch the heaters on after they were switched off.
    in_flight: Option<SpawnHandle>,
    /// Set by the stop, so that ticks still queued are ignored while the heaters are switched off.
    stopping: bool,
    /// The heaters still to be switched off for the stretch outside the active period, once the
    /// first tick outside it has tried; `None` before that, at the start and after every tick
    /// inside the period, so that each heater gets the Off once per period end and once at a
    /// start outside the period, where a run that crashed may have left it on. A heater that
    /// could not be switched is kept for the next tick, the ones that were are left alone.
    pending_off: Option<Vec<Heater>>,
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
            stopping: false,
            pending_off: None,
        }
    }

    /// Cancels the tick's work in flight, if any. A cancelled evaluation leaves the heaters as
    /// they are and a cancelled switch-off leaves the unswitched heaters pending, so the caller
    /// asserts them anew.
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

    /// Records why a tick did nothing, the counterpart of `record_decision` for a skipped tick.
    fn record_skip(span: &tracing::Span, reason: Reason) {
        span.record("thermostat.reason", tracing::field::display(reason));
        debug!("Skipping this tick ({reason})");
    }

    /// Switches the given heaters off in the current span. Resolves to the heaters that could
    /// not be switched.
    fn switch_off(&self, heaters: Vec<Heater>) -> impl Future<Output = Vec<Heater>> + 'static {
        let devices = self.devices.clone();

        async move { Self::switch_heaters(&devices, heaters, false).await }
            .instrument(tracing::Span::current())
    }

    /// One tick: read the inputs, decide, and switch the heaters.
    ///
    /// Camera and plug failures are recorded on the span but do not fail the tick: a camera error
    /// is a normal `Off` decision and a plug that could not be switched is retried next tick.
    async fn evaluate(
        coordinator_actor_addr: Addr<CoordinatorActor>,
        devices: Devices,
        config: Thermostat,
    ) {
        let span = tracing::Span::current();
        let now = Utc::now();

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

        let decision = decide(now, presence, reading.as_ref(), &config);
        Self::record_decision(&span, &config.name, decision);

        let on = match decision.command {
            HeaterCommand::On => true,
            HeaterCommand::Off => false,
            HeaterCommand::Keep => return,
        };

        Self::switch_heaters(&devices, config.heaters, on).await;
    }

    /// The heaters are independent, so they are switched together: the time taken is that of
    /// the slowest plug rather than the sum, which matters for the tick deadline and even more
    /// for the shutdown budget. Returns the heaters that could not be switched.
    async fn switch_heaters(devices: &Devices, heaters: Vec<Heater>, on: bool) -> Vec<Heater> {
        let switched = join_all(
            heaters
                .iter()
                .map(|heater| Self::switch_heater(devices, heater, on)),
        )
        .await;

        heaters
            .into_iter()
            .zip(switched)
            .filter(|(_, switched)| !switched)
            .map(|(heater, _)| heater)
            .collect()
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
    async fn switch_heater(devices: &Devices, heater: &Heater, on: bool) -> bool {
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
            Ok(()) => {
                info!(
                    "Switched heater '{}' {}",
                    heater.name,
                    if on { "on" } else { "off" }
                );
                true
            }
            Err(e) => {
                record_error!(&e);
                false
            }
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
    type Result = ResponseActFuture<Self, ()>;

    /// Switches the heaters off before stopping, so that a service that does not come back
    /// cannot leave them running with nobody checking presence or temperature. A failure or a
    /// timeout is recorded and otherwise ignored; there is no next tick to retry on.
    #[instrument(name = "ThermostatActor::Handler<StopMessage>", skip_all, fields(
        thermostat.name = %self.config.name,
    ))]
    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        self.stopping = true;

        // An evaluation still in flight could switch the heaters on after they were switched off.
        // Unlike at a tick, cutting it short here is expected, so nothing is logged.
        if let Some(in_flight) = self.in_flight.take() {
            ctx.cancel_future(in_flight);
        }

        let switch_off = self.switch_off(self.config.heaters.clone());

        Box::pin(
            async move {
                if let Err(e) = tokio::time::timeout(STOP_TIMEOUT, switch_off).await {
                    record_error!(&e);
                }
            }
            .instrument(tracing::Span::current())
            .into_actor(self)
            .map(|_, _, ctx| ctx.stop()),
        )
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

        if self.stopping {
            Self::record_skip(&span, Reason::Stopping);
            return;
        }

        // Every handled tick asserts the heaters anew, so whatever the previous one left running
        // is cancelled before it can switch them on older inputs.
        self.cancel_in_flight(ctx);

        // Outside the active period nothing is queried. Each heater is switched off once, by the
        // first tick outside it, and then left alone; one that could not be switched is retried
        // on the next tick, as nobody checks presence or temperature until the period starts.
        if let Some(period) = &self.config.active_period
            && !period.contains(Utc::now())
        {
            let heaters = self
                .pending_off
                .clone()
                .unwrap_or_else(|| self.config.heaters.clone());

            if heaters.is_empty() {
                Self::record_skip(&span, Reason::OutsideActivePeriod);
                return;
            }

            Self::record_decision(
                &span,
                &self.config.name,
                Decision::new(HeaterCommand::Off, Reason::OutsideActivePeriod),
            );

            let fut = self
                .switch_off(heaters)
                .into_actor(self)
                .map(|not_switched, act, _| {
                    act.in_flight = None;
                    act.pending_off = Some(not_switched);
                });

            self.in_flight = Some(ctx.spawn(fut));
            return;
        }

        self.pending_off = None;

        let fut = Self::evaluate(
            self.coordinator_actor_addr.clone(),
            self.devices.clone(),
            self.config.clone(),
        )
        .instrument(span.clone())
        .into_actor(self)
        .map(|_, act, _| act.in_flight = None);

        self.in_flight = Some(ctx.spawn(fut));
    }
}
