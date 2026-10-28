use std::time::{Duration, Instant};

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
        SensorReading, SensorReadingMessage, StopMessage, ThermostatDecisionMessage,
    },
    system::thermostat::decision::{
        Decision, Evaluation, HeaterCommand, HeaterSwitch, Presence, PresenceError, Reason,
        SwitchOutcome, decide, person_present,
    },
    telemetry::{consumer_span, record_error, set_parent_context},
};

#[derive(Debug)]
pub struct ThermostatActor {
    decision_recipient: Recipient<ThermostatDecisionMessage>,
    /// The topic the decisions are published on.
    decisions_publish_topic: String,
    device_sessions: DeviceSessions,
    /// The credentials the camera is queried with; it has no session to share, so it is not
    /// reached through `device_sessions`.
    tapo: Tapo,
    config: Thermostat,
    /// The latest reading for `sensor_location`. `None` until the first one arrives, since the
    /// readings are not retained.
    reading: Option<SensorReading>,
    /// The tick's work in flight, if any: the evaluation and the switch that follows it. The
    /// next tick cancels it, so that a slow one cannot hold the thermostat for several intervals.
    /// A switch already sent to a heater's session still completes, but the next tick's command
    /// queues behind it, so the newest command still lands last.
    in_flight: Option<SpawnHandle>,
    /// When the last tick started, `None` before the first one.
    last_tick: Option<Instant>,
    /// The next tick and when it is due: an interval after the last one, or sooner for a reading.
    next_tick: Option<(SpawnHandle, Instant)>,
    /// The command of the last evaluation that completed, which the deadband repeats. `None`
    /// before the first one, so that a start inside the deadband turns the heaters off.
    last_command: Option<HeaterCommand>,
}

impl ThermostatActor {
    #[instrument(name = "ThermostatActor::new", skip_all, fields(
        thermostat.name = %config.name,
    ))]
    pub fn new(
        decision_recipient: Recipient<ThermostatDecisionMessage>,
        decisions_publish_topic: String,
        device_sessions: DeviceSessions,
        tapo: Tapo,
        config: Thermostat,
    ) -> Self {
        Self {
            decision_recipient,
            decisions_publish_topic,
            device_sessions,
            tapo,
            config,
            reading: None,
            in_flight: None,
            last_tick: None,
            next_tick: None,
            last_command: None,
        }
    }

    /// Schedules the next tick in `delay`, replacing the one scheduled before. A tick started by
    /// a reading carries the reading's span context in `cause`, so that it is traced under it.
    fn schedule_tick(
        &mut self,
        ctx: &mut Context<Self>,
        delay: Duration,
        cause: Option<opentelemetry::Context>,
    ) {
        if let Some((next_tick, _)) = self.next_tick.take() {
            ctx.cancel_future(next_tick);
        }

        let handle = ctx.run_later(delay, |act, ctx| act.tick(ctx, cause));
        self.next_tick = Some((handle, Instant::now() + delay));
    }

    /// Evaluates and switches the heaters, cancelling whatever the previous tick left running.
    ///
    /// Runs on every reading for the thermostat's sensor, at least `min_interval_s` after the
    /// last tick, and an interval after the last tick when no reading comes first, so that a
    /// sensor that falls silent still gets the heaters asserted, and switched off once its
    /// reading goes stale. A tick started by a reading is traced under it, through `cause`.
    fn tick(&mut self, ctx: &mut Context<Self>, cause: Option<opentelemetry::Context>) {
        let span = info_span!(
            "ThermostatActor::tick",
            thermostat.name = %self.config.name,
        );
        if let Some(cause) = cause {
            set_parent_context(&span, cause);
        }
        let _enter = span.enter();

        // Every tick asserts the heaters anew, so whatever the previous one left running is
        // cancelled before it can switch them on older inputs.
        self.cancel_in_flight(ctx);

        // On the monotonic clock, so that a step of the wall clock cannot move the ticks.
        self.last_tick = Some(Instant::now());
        self.schedule_tick(ctx, Duration::from_secs(self.config.interval_s), None);

        let switch_span = span.clone();
        let publish_span = span.clone();

        // Both futures are polled outside the tick's span, and their own spans are only created
        // on the first poll, so each is instrumented with the tick's span to be traced under it.
        let fut = Self::evaluate(
            self.reading.clone(),
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
    /// Only what the period needs is used: outside every period nothing is, not even `reading`,
    /// and inside one the camera is only consulted when the period requires presence.
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
        reading: Option<SensorReading>,
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

        let presence = if period.require_presence {
            Some(Self::query_presence(&tapo, &config, now).await)
        } else {
            None
        };

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

        // The first tick runs at once, so that a start asserts the heaters straight away, and
        // schedules the next one. It runs after `started` returns, so that its trace is its own
        // rather than part of this span.
        self.schedule_tick(ctx, Duration::ZERO, None);
    }

    #[instrument(name = "ThermostatActor::stopped", level = "error", skip_all, fields(
        thermostat.name = %self.config.name,
    ))]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<SensorReadingMessage> for ThermostatActor {
    type Result = ();

    /// Keeps the reading and ticks on it at once, so that it is evaluated fresh whenever the
    /// sensor publishes, unless the last tick was less than `min_interval_s` ago: the tick then
    /// runs once that has passed, so that it neither repeats the last one on nearly the same
    /// inputs nor cancels a switch still running. The tick is traced under the latest reading it
    /// evaluates.
    fn handle(&mut self, message: SensorReadingMessage, ctx: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "ThermostatActor::Handler<SensorReadingMessage>",
            message = SensorReadingMessage,
            parent = message.span_context,
            destination = "ThermostatActor",
            thermostat.name = %self.config.name,
            sensor.location = %message.reading.location,
            sensor.temperature = message.reading.temperature,
        );
        let _enter = span.enter();

        self.reading = Some(message.reading);

        let delay = reading_tick_delay(
            Instant::now(),
            self.last_tick,
            self.next_tick.as_ref().map(|(_, next_due)| *next_due),
            Duration::from_secs(self.config.min_interval_s),
        );

        if !delay.is_zero() {
            debug!(
                "Thermostat '{}': the last tick was too recent, evaluating the reading in {:.1} s",
                self.config.name,
                delay.as_secs_f64()
            );
        }

        // The tick replaces the one scheduled before, so that a tick already due by then is
        // traced under this reading, which it evaluates, rather than under the one it was
        // scheduled for.
        self.schedule_tick(ctx, delay, Some(span.context()));
    }
}

impl Handler<StopMessage> for ThermostatActor {
    type Result = ();

    /// Stops without touching the heaters: the next start asserts them on its first tick.
    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}

/// How long after `now` a reading's tick runs: once `min_interval` has passed since the last
/// tick, at once if it already has, or when the next tick is due if that is sooner, such as one
/// already waiting for an earlier reading.
fn reading_tick_delay(
    now: Instant,
    last_tick: Option<Instant>,
    next_tick: Option<Instant>,
    min_interval: Duration,
) -> Duration {
    let after_min_interval = last_tick.map_or(Duration::ZERO, |last_tick| {
        (last_tick + min_interval).saturating_duration_since(now)
    });

    next_tick.map_or(after_min_interval, |next_tick| {
        next_tick
            .saturating_duration_since(now)
            .min(after_min_interval)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN_INTERVAL: Duration = Duration::from_secs(60);
    const INTERVAL: Duration = Duration::from_secs(300);

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    #[test]
    fn reading_before_any_tick_ticks_at_once() {
        let now = Instant::now();

        assert_eq!(
            reading_tick_delay(now, None, None, MIN_INTERVAL),
            Duration::ZERO
        );
    }

    #[test]
    fn reading_min_interval_after_the_last_tick_ticks_at_once() {
        let last_tick = Instant::now();

        for since_last_tick in [MIN_INTERVAL, secs(200)] {
            assert_eq!(
                reading_tick_delay(
                    last_tick + since_last_tick,
                    Some(last_tick),
                    Some(last_tick + INTERVAL),
                    MIN_INTERVAL
                ),
                Duration::ZERO,
                "{since_last_tick:?} after the last tick"
            );
        }
    }

    #[test]
    fn reading_within_min_interval_waits_for_the_rest_of_it() {
        let last_tick = Instant::now();

        assert_eq!(
            reading_tick_delay(
                last_tick + secs(20),
                Some(last_tick),
                Some(last_tick + INTERVAL),
                MIN_INTERVAL
            ),
            secs(40)
        );
    }

    #[test]
    fn reading_within_min_interval_runs_with_a_tick_due_sooner() {
        let last_tick = Instant::now();

        assert_eq!(
            reading_tick_delay(
                last_tick + secs(20),
                Some(last_tick),
                Some(last_tick + secs(50)),
                MIN_INTERVAL
            ),
            secs(30)
        );
    }
}
