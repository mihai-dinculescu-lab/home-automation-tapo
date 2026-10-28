use std::time::Duration;

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, Recipient, SpawnHandle,
    WrapFuture,
};
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use tracing::{Instrument, debug, info, info_span, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::{
    settings::{Heater, Tapo, Thermostat},
    system::devices::{DeviceSessions, DeviceType, Error as DeviceError},
    system::messages::{
        GetSensorReadingMessage, SensorReading, StopMessage, ThermostatDecisionMessage,
    },
    system::thermostat::decision::{
        Decision, Evaluation, HeaterCommand, HeaterSwitch, Presence, PresenceError, Reason,
        SwitchOutcome, decide, person_present,
    },
    telemetry::record_error,
};

#[derive(Debug)]
pub struct ThermostatActor {
    reading_recipient: Recipient<GetSensorReadingMessage>,
    decision_recipient: Recipient<ThermostatDecisionMessage>,
    /// The topic the decisions are published on.
    decisions_publish_topic: String,
    device_sessions: DeviceSessions,
    /// The credentials the camera is queried with; it has no session to share, so it is not
    /// reached through `device_sessions`.
    tapo: Tapo,
    config: Thermostat,
    /// The tick's work in flight, if any: the evaluation and the switch that follows it. The
    /// next tick cancels it, so that a slow one cannot hold the thermostat for several intervals.
    /// A switch already sent to a heater's session still completes, but the next tick's command
    /// queues behind it, so the newest command still lands last.
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
        reading_recipient: Recipient<GetSensorReadingMessage>,
        decision_recipient: Recipient<ThermostatDecisionMessage>,
        decisions_publish_topic: String,
        device_sessions: DeviceSessions,
        tapo: Tapo,
        config: Thermostat,
    ) -> Self {
        Self {
            reading_recipient,
            decision_recipient,
            decisions_publish_topic,
            device_sessions,
            tapo,
            config,
            in_flight: None,
            last_command: None,
        }
    }

    /// Evaluates and switches the heaters, cancelling whatever the previous tick left running.
    fn tick(&mut self, ctx: &mut Context<Self>) {
        let span = info_span!(
            "ThermostatActor::tick",
            thermostat.name = %self.config.name,
        );
        let _enter = span.enter();

        // Every tick asserts the heaters anew, so whatever the previous one left running is
        // cancelled before it can switch them on older inputs.
        self.cancel_in_flight(ctx);

        let switch_span = span.clone();
        let publish_span = span.clone();

        // Both futures are polled outside the tick's span, and their own spans are only created
        // on the first poll, so each is instrumented with the tick's span to be traced under it.
        let fut = Self::evaluate(
            self.reading_recipient.clone(),
            self.tapo.clone(),
            self.config.clone(),
            self.last_command,
        )
        .instrument(span.clone())
        .into_actor(self)
        .then(move |evaluation, act, _| {
            // Remembered before the switch, which can be cancelled by the next tick.
            act.last_command = Some(evaluation.decision.command);

            let switch = Self::switch_heaters(
                act.device_sessions.clone(),
                act.config.heaters.clone(),
                evaluation.decision.command == HeaterCommand::On,
            );

            async move { (evaluation, switch.await) }
                .instrument(switch_span)
                .into_actor(act)
        })
        .map(move |(evaluation, heaters), act, _| {
            act.in_flight = None;

            let _enter = publish_span.enter();
            act.publish_decision(evaluation, heaters);
        });

        self.in_flight = Some(ctx.spawn(fut));
    }

    /// Sends the tick's decision to be published, once the heaters have been switched, so that
    /// it carries how each switch ended.
    fn publish_decision(&self, evaluation: Evaluation, heaters: Vec<HeaterSwitch>) {
        let result = self.decision_recipient.try_send(ThermostatDecisionMessage {
            span_context: tracing::Span::current().context(),
            topic_name: self.decisions_publish_topic.clone(),
            thermostat_name: self.config.name.clone(),
            evaluation,
            heaters,
        });

        if let Err(e) = result {
            record_error!(&e);
        }
    }

    /// Cancels the tick's work in flight, if any. A switch the cancelled tick already sent still
    /// completes, one it had not sent yet does not, so the caller asserts the heaters anew.
    fn cancel_in_flight(&mut self, ctx: &mut Context<Self>) {
        if let Some(in_flight) = self.in_flight.take() {
            warn!("The previous tick's work is still running, cancelling it");
            ctx.cancel_future(in_flight);
        }
    }

    /// Records a decision on the evaluation's span and in the log, the one place that names its
    /// fields. The log line carries the target the tick ran under, so that it shows which
    /// period's rule decided; there is none outside every period.
    fn record_decision(span: &tracing::Span, name: &str, target: Option<f64>, decision: Decision) {
        span.record(
            "thermostat.command",
            tracing::field::display(decision.command),
        );
        span.record(
            "thermostat.reason",
            tracing::field::display(decision.reason),
        );

        let target = target.map_or(String::new(), |target| format!(", target {target} °C"));

        info!(
            "Thermostat '{name}': {} ({}){target}",
            decision.command, decision.reason
        );
    }

    /// One tick's decision and the inputs it was made on. The tick switches the heaters once it
    /// has remembered the command, so that a tick cancelled mid-switch still leaves the command
    /// it decided for the deadband to repeat.
    ///
    /// Only what the period needs is queried: outside every period nothing is, and inside one
    /// the camera is only consulted when the period requires presence.
    ///
    /// A camera failure is recorded on the camera's span but does not fail the tick: it is a
    /// normal `Off` decision.
    #[instrument(name = "ThermostatActor::evaluate", skip_all, fields(
        thermostat.name = %config.name,
        thermostat.target_temperature = tracing::field::Empty,
        thermostat.require_presence = tracing::field::Empty,
        thermostat.person_detected = tracing::field::Empty,
        thermostat.temperature = tracing::field::Empty,
        thermostat.reading_age_s = tracing::field::Empty,
        thermostat.command = tracing::field::Empty,
        thermostat.reason = tracing::field::Empty,
    ))]
    async fn evaluate(
        reading_recipient: Recipient<GetSensorReadingMessage>,
        tapo: Tapo,
        config: Thermostat,
        last_command: Option<HeaterCommand>,
    ) -> Evaluation {
        let span = tracing::Span::current();
        let now = Utc::now();

        let Some(period) = config.active_period(now) else {
            let decision = Decision::new(HeaterCommand::Off, Reason::OutsideActivePeriod);
            Self::record_decision(&span, &config.name, None, decision);

            return Evaluation {
                decided_at: now,
                active_period: None,
                presence: None,
                reading: None,
                decision,
            };
        };

        span.record("thermostat.target_temperature", period.target_temperature);
        span.record("thermostat.require_presence", period.require_presence);

        // The two inputs are independent, so they are fetched together.
        let (reading, presence) =
            tokio::join!(Self::query_reading(&reading_recipient, &config), async {
                if period.require_presence {
                    Some(Self::query_presence(&tapo, &config, now).await)
                } else {
                    None
                }
            });

        let decision = decide(now, presence, reading.as_ref(), last_command, &config);

        let evaluation = Evaluation {
            decided_at: now,
            active_period: Some(period.clone()),
            presence,
            reading,
            decision,
        };

        if let Some(Ok(person_detected)) = evaluation.presence {
            span.record("thermostat.person_detected", person_detected);
        }
        if let Some(reading) = &evaluation.reading {
            span.record("thermostat.temperature", reading.temperature);
        }
        if let Some(reading_age_s) = evaluation.reading_age_s() {
            span.record("thermostat.reading_age_s", reading_age_s);
        }

        Self::record_decision(
            &span,
            &config.name,
            Some(period.target_temperature),
            decision,
        );

        evaluation
    }

    /// The heaters are independent, so they are switched together: the time taken is that of
    /// the slowest plug rather than the sum, which matters for the tick deadline. A plug that
    /// could not be switched is recorded and asserted again next tick. Returns how each switch
    /// ended, in the order of `heaters`.
    #[instrument(name = "ThermostatActor::switch_heaters", skip_all, fields(
        heater.on = on,
    ))]
    async fn switch_heaters(
        device_sessions: DeviceSessions,
        heaters: Vec<Heater>,
        on: bool,
    ) -> Vec<HeaterSwitch> {
        join_all(heaters.iter().map(|heater| async {
            HeaterSwitch {
                name: heater.name.clone(),
                outcome: Self::switch_heater(&device_sessions, heater, on).await,
            }
        }))
        .await
    }

    #[instrument(name = "ThermostatActor::query_reading", skip_all, fields(
        sensor.location = %config.sensor_location,
    ))]
    async fn query_reading(
        reading_recipient: &Recipient<GetSensorReadingMessage>,
        config: &Thermostat,
    ) -> Option<SensorReading> {
        let result = reading_recipient
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

        // The camera is only ever queried here, and a tick cancels the previous one, so it has
        // no session to share.
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
    async fn switch_heater(
        device_sessions: &DeviceSessions,
        heater: &Heater,
        on: bool,
    ) -> SwitchOutcome {
        let device_type = DeviceType::PlugEnergyMonitoring;

        let result = if on {
            device_sessions
                .turn_on(device_type, &heater.ip_address)
                .await
        } else {
            device_sessions
                .turn_off(device_type, &heater.ip_address)
                .await
        };

        match result {
            Ok(()) => {
                info!(
                    "Switched heater '{}' {}",
                    heater.name,
                    if on { "on" } else { "off" }
                );
                SwitchOutcome::Switched
            }
            // A tick that lands while the service stops; the next start asserts the heaters.
            Err(DeviceError::ShuttingDown) => {
                warn!("Shutting down, heater '{}' was not switched", heater.name);
                SwitchOutcome::ShuttingDown
            }
            Err(e) => {
                record_error!(&e);
                SwitchOutcome::Failed
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
        if self.config.heaters.is_empty() {
            warn!(
                "Thermostat '{}' has no heaters configured",
                self.config.name
            );
        }

        // `run_interval` waits a full interval before its first tick, so the first one is
        // scheduled at once: a start asserts the heaters straight away. It runs after `started`
        // returns, so that its trace is its own rather than part of this span.
        let interval = Duration::from_secs(self.config.interval_s);
        ctx.run_later(Duration::ZERO, |act, ctx| act.tick(ctx));
        ctx.run_interval(interval, |act, ctx| act.tick(ctx));
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
