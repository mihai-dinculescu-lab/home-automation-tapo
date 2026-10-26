use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    SpawnHandle, WrapFuture, clock::interval,
};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use tracing::{Instrument, debug, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::{Heater, Tapo, Thermostat},
    system::coordinator_actor::CoordinatorActor,
    system::messages::{EvaluateMessage, GetSensorReadingMessage, SensorReading, StopMessage},
    system::thermostat::decision::{
        HeaterCommand, Presence, PresenceError, decide, person_present,
    },
    telemetry::{consumer_span, producer_span, record_error},
};

/// Bound on switching the heaters off at a stop. It has to leave room inside the actors'
/// shutdown timeout in `main` for the other actors to drain after the thermostats.
const STOP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct ThermostatActor {
    coordinator_actor_addr: Addr<CoordinatorActor>,
    tapo: Tapo,
    config: Thermostat,
    /// The evaluation in flight, if any. The next tick cancels it, so that a slow one can
    /// neither hold the thermostat for several intervals nor finish after a newer one and switch
    /// the heaters on older inputs; a stop cancels it, so that it cannot switch the heaters on
    /// after they were switched off.
    evaluating: Option<SpawnHandle>,
    /// Set by the stop, so that ticks still queued are ignored while the heaters are switched off.
    stopping: bool,
}

impl ThermostatActor {
    #[instrument(name = "ThermostatActor::new", skip_all, fields(
        thermostat.name = %config.name,
    ))]
    pub fn new(
        coordinator_actor_addr: Addr<CoordinatorActor>,
        tapo: Tapo,
        config: Thermostat,
    ) -> Self {
        Self {
            coordinator_actor_addr,
            tapo,
            config,
            evaluating: None,
            stopping: false,
        }
    }

    /// One tick: read the inputs, decide, and switch the heaters.
    ///
    /// Camera and plug failures are recorded on the span but do not fail the tick: a camera error
    /// is a normal `Off` decision and a plug that could not be switched is retried next tick.
    async fn evaluate(
        coordinator_actor_addr: Addr<CoordinatorActor>,
        tapo: Tapo,
        config: Thermostat,
    ) {
        let span = tracing::Span::current();
        let now = Utc::now();

        // The two inputs are independent, so they are fetched together.
        let (reading, presence) = tokio::join!(
            Self::query_reading(&coordinator_actor_addr, &config),
            Self::query_presence(&tapo, &config, now),
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

        span.record(
            "thermostat.command",
            tracing::field::display(decision.command),
        );
        span.record(
            "thermostat.reason",
            tracing::field::display(decision.reason),
        );

        info!(
            "Thermostat '{}': {} ({})",
            config.name, decision.command, decision.reason
        );

        let on = match decision.command {
            HeaterCommand::On => true,
            HeaterCommand::Off => false,
            HeaterCommand::Keep => return,
        };

        Self::switch_heaters(&tapo, &config.heaters, on).await;
    }

    /// The heaters are independent, so they are switched together: the time taken is that of
    /// the slowest plug rather than the sum, which matters for the tick deadline and even more
    /// for the shutdown budget.
    async fn switch_heaters(tapo: &Tapo, heaters: &[Heater], on: bool) {
        join_all(
            heaters
                .iter()
                .map(|heater| Self::switch_heater(tapo, heater, on)),
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
    async fn query_presence(tapo: &Tapo, config: &Thermostat, now: DateTime<Utc>) -> Presence {
        let window_start = now - chrono::Duration::seconds(config.presence_window_s as i64);

        let result = async {
            let handler = tapo.client().c120(config.camera_ip_address.clone()).await?;

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
    async fn switch_heater(tapo: &Tapo, heater: &Heater, on: bool) {
        let result = async {
            let handler = tapo.client().p110(heater.ip_address.clone()).await?;

            if on {
                handler.on().await
            } else {
                handler.off().await
            }
        }
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
        if let Some(evaluating) = self.evaluating.take() {
            ctx.cancel_future(evaluating);
        }

        let tapo = self.tapo.clone();
        let heaters = self.config.heaters.clone();

        Box::pin(
            async move {
                let switch_off = Self::switch_heaters(&tapo, &heaters, false);

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
            debug!("The thermostat is stopping, skipping this tick");
            return;
        }

        // A cancelled evaluation leaves the heaters as they are; this tick asserts them anew.
        if let Some(evaluating) = self.evaluating.take() {
            warn!("The previous evaluation is still running, cancelling it");
            ctx.cancel_future(evaluating);
        }

        let fut = Self::evaluate(
            self.coordinator_actor_addr.clone(),
            self.tapo.clone(),
            self.config.clone(),
        )
        .instrument(span.clone())
        .into_actor(self)
        .map(|_, act, _| act.evaluating = None);

        self.evaluating = Some(ctx.spawn(fut));
    }
}
