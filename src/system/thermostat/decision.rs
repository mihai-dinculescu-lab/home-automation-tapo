use chrono::{DateTime, Duration, Utc};
use derive_more::Display;
use serde::Serialize;
use tapo::responses::{DetectionResult, RecordingType};

use crate::settings::{ActivePeriod, Thermostat};
use crate::system::messages::SensorReading;

/// The camera could not be queried, so presence is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresenceError;

/// Whether a person is present, or `Err` when the camera could not be queried.
pub type Presence = Result<bool, PresenceError>;

/// `Display` names it in the logs and on the spans, `Serialize` in the published decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HeaterCommand {
    On,
    Off,
}

/// Why the heaters were commanded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// The tick fell outside every active period.
    OutsideActivePeriod,
    /// The period requires presence and the camera could not be queried, or was not.
    CameraError,
    NobodyPresent,
    NoReading,
    StaleReading,
    AboveTarget,
    BelowTarget,
    /// The temperature is within the deadband, so the last command is repeated.
    WithinDeadband,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decision {
    pub command: HeaterCommand,
    pub reason: Reason,
}

impl Decision {
    pub fn new(command: HeaterCommand, reason: Reason) -> Self {
        Self { command, reason }
    }
}

/// A tick's decision with the inputs it was made on. Only what the period needed is queried, so
/// outside every period all of them are `None`.
#[derive(Debug, Clone)]
pub struct Evaluation {
    pub decided_at: DateTime<Utc>,
    /// The period the tick ran under; `None` outside every period.
    pub active_period: Option<ActivePeriod>,
    /// `None` when the camera was not consulted.
    pub presence: Option<Presence>,
    pub reading: Option<SensorReading>,
    pub decision: Decision,
}

impl Evaluation {
    /// How old the reading was at `decided_at`, in seconds; `None` without one. A wall clock
    /// stepped back since the reading was received counts as 0 rather than a negative age.
    pub fn reading_age_s(&self) -> Option<i64> {
        self.reading
            .as_ref()
            .map(|reading| reading.age(self.decided_at).num_seconds().max(0))
    }
}

/// How switching a heater to the tick's command ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SwitchOutcome {
    Switched,
    Failed,
    /// The service was stopping, so the command was not sent.
    ShuttingDown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeaterSwitch {
    pub name: String,
    pub outcome: SwitchOutcome,
}

/// A person counts as present when any detection has `Person` among its events and ended at or
/// after `window_start`.
pub fn person_present(detections: &[DetectionResult], window_start: DateTime<Utc>) -> bool {
    detections.iter().any(|detection| {
        detection.end_time >= window_start && detection.events.contains(&RecordingType::Person)
    })
}

/// Decides what to do with the heaters. The rules are evaluated in order; the first match wins.
///
/// The rule a tick runs under is the active period containing `now`: it holds the target and
/// says whether presence is consulted. Outside every period the heaters are turned off.
///
/// `presence` is `None` when the camera was not consulted. A period that requires presence
/// treats that as unknown, which decides off, so that the decision function rather than the
/// actor encodes when presence matters. A period that does not require presence ignores it.
///
/// Every decision is a command, so that each tick asserts the heaters anew and a plug switched
/// by hand is corrected within one interval. Within the deadband the last command is repeated,
/// so that the heaters do not cycle around the setpoint; without one, right after a start, they
/// are turned off.
///
/// A missing reading turns the heaters off as well: the readings are not retained, so none is
/// known right after a start, and the plugs may have been left on by a run that crashed.
pub fn decide(
    now: DateTime<Utc>,
    presence: Option<Presence>,
    reading: Option<&SensorReading>,
    last_command: Option<HeaterCommand>,
    config: &Thermostat,
) -> Decision {
    let Some(period) = config.active_period(now) else {
        return Decision::new(HeaterCommand::Off, Reason::OutsideActivePeriod);
    };

    if period.require_presence {
        match presence {
            None | Some(Err(PresenceError)) => {
                return Decision::new(HeaterCommand::Off, Reason::CameraError);
            }
            Some(Ok(false)) => return Decision::new(HeaterCommand::Off, Reason::NobodyPresent),
            Some(Ok(true)) => {}
        }
    }

    let max_age = Duration::seconds(config.reading_max_age_s as i64);

    let Some(reading) = reading else {
        return Decision::new(HeaterCommand::Off, Reason::NoReading);
    };

    if reading.age(now) > max_age {
        return Decision::new(HeaterCommand::Off, Reason::StaleReading);
    }

    if reading.temperature > period.target_temperature + config.deadband {
        Decision::new(HeaterCommand::Off, Reason::AboveTarget)
    } else if reading.temperature < period.target_temperature - config.deadband {
        Decision::new(HeaterCommand::On, Reason::BelowTarget)
    } else {
        Decision::new(
            last_command.unwrap_or(HeaterCommand::Off),
            Reason::WithinDeadband,
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::settings::ActivePeriod;

    fn period(start: &str, end: &str, target_temperature: f64) -> ActivePeriod {
        ActivePeriod::new(start, end, target_temperature, true)
    }

    /// A single period that contains `now` and requires presence.
    fn config() -> Thermostat {
        Thermostat {
            name: "Living Room".to_string(),
            enabled: true,
            time_zone: chrono_tz::Europe::London,
            active_periods: vec![period("08:00", "22:00", 22.0)],
            camera_ip_address: "192.168.1.154".to_string(),
            sensor_location: "Living Room".to_string(),
            heaters: vec![],
            deadband: 0.25,
            presence_window_s: 900,
            reading_max_age_s: 900,
            interval_s: 300,
            min_interval_s: 60,
        }
    }

    /// Noon in London, inside the period of `config`.
    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 25, 12, 0, 0).unwrap()
    }

    fn reading(temperature: f64, age: Duration) -> SensorReading {
        SensorReading {
            location: "Living Room".to_string(),
            temperature,
            received_at: now() - age,
        }
    }

    /// `DetectionResult` is not constructible outside `tapo`, so it is built from its serialized form.
    fn detection(end_time: DateTime<Utc>, events: Vec<RecordingType>) -> DetectionResult {
        serde_json::from_value(serde_json::json!({
            "start_time": (end_time - Duration::seconds(10)).to_rfc3339(),
            "end_time": end_time.to_rfc3339(),
            "trigger": events.last().unwrap(),
            "events": events,
        }))
        .unwrap()
    }

    #[test]
    fn outside_every_period_turns_off() {
        // 03:00 London, with fresh inputs that would otherwise decide On.
        let now = Utc.with_ymd_and_hms(2026, 10, 25, 3, 0, 0).unwrap();
        let reading = SensorReading {
            location: "Living Room".to_string(),
            temperature: 20.0,
            received_at: now,
        };

        let decision = decide(
            now,
            Some(Ok(true)),
            Some(&reading),
            Some(HeaterCommand::On),
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::OutsideActivePeriod)
        );
    }

    #[test]
    fn period_not_requiring_presence_ignores_it() {
        let mut config = config();
        config.active_periods[0].require_presence = false;

        for presence in [None, Some(Err(PresenceError)), Some(Ok(false))] {
            let decision = decide(
                now(),
                presence,
                Some(&reading(20.0, Duration::zero())),
                None,
                &config,
            );

            assert_eq!(
                decision,
                Decision::new(HeaterCommand::On, Reason::BelowTarget),
                "presence {presence:?}"
            );
        }
    }

    #[test]
    fn period_requiring_presence_treats_no_presence_as_a_camera_error() {
        let decision = decide(
            now(),
            None,
            Some(&reading(20.0, Duration::zero())),
            None,
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::CameraError)
        );
    }

    #[test]
    fn target_comes_from_the_period() {
        // 06:00 London is in the first period, noon in the second; 21.5 °C is below the band
        // around 22 °C and above the one around 21 °C.
        let mut config = config();
        config.active_periods = vec![
            period("05:45", "07:00", 21.0),
            period("07:00", "22:00", 22.0),
        ];

        let decide_at = |now: DateTime<Utc>| {
            let reading = SensorReading {
                location: "Living Room".to_string(),
                temperature: 21.5,
                received_at: now,
            };

            decide(now, Some(Ok(true)), Some(&reading), None, &config)
        };

        assert_eq!(
            decide_at(Utc.with_ymd_and_hms(2026, 10, 25, 6, 0, 0).unwrap()),
            Decision::new(HeaterCommand::Off, Reason::AboveTarget)
        );
        assert_eq!(
            decide_at(now()),
            Decision::new(HeaterCommand::On, Reason::BelowTarget)
        );
    }

    #[test]
    fn camera_error_turns_off() {
        let decision = decide(
            now(),
            Some(Err(PresenceError)),
            Some(&reading(20.0, Duration::zero())),
            None,
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::CameraError)
        );
    }

    #[test]
    fn nobody_present_turns_off() {
        let decision = decide(
            now(),
            Some(Ok(false)),
            Some(&reading(20.0, Duration::zero())),
            None,
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::NobodyPresent)
        );
    }

    #[test]
    fn no_reading_turns_off() {
        let decision = decide(now(), Some(Ok(true)), None, None, &config());

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::NoReading)
        );
    }

    #[test]
    fn stale_reading_turns_off() {
        let decision = decide(
            now(),
            Some(Ok(true)),
            Some(&reading(20.0, Duration::seconds(901))),
            None,
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::StaleReading)
        );
    }

    #[test]
    fn reading_exactly_at_max_age_is_fresh() {
        let decision = decide(
            now(),
            Some(Ok(true)),
            Some(&reading(20.0, Duration::seconds(900))),
            None,
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::On, Reason::BelowTarget)
        );
    }

    #[test]
    fn above_target_turns_off() {
        let decision = decide(
            now(),
            Some(Ok(true)),
            Some(&reading(22.3, Duration::zero())),
            Some(HeaterCommand::On),
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::AboveTarget)
        );
    }

    #[test]
    fn below_target_turns_on() {
        let decision = decide(
            now(),
            Some(Ok(true)),
            Some(&reading(21.7, Duration::zero())),
            Some(HeaterCommand::Off),
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::On, Reason::BelowTarget)
        );
    }

    #[test]
    fn within_deadband_repeats_the_last_command() {
        for last_command in [HeaterCommand::On, HeaterCommand::Off] {
            let decision = decide(
                now(),
                Some(Ok(true)),
                Some(&reading(22.0, Duration::zero())),
                Some(last_command),
                &config(),
            );

            assert_eq!(
                decision,
                Decision::new(last_command, Reason::WithinDeadband),
                "last command {last_command}"
            );
        }
    }

    #[test]
    fn within_deadband_without_a_last_command_turns_off() {
        let decision = decide(
            now(),
            Some(Ok(true)),
            Some(&reading(22.0, Duration::zero())),
            None,
            &config(),
        );

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::WithinDeadband)
        );
    }

    #[test]
    fn deadband_boundaries_repeat_the_last_command() {
        for temperature in [21.75, 22.25] {
            let decision = decide(
                now(),
                Some(Ok(true)),
                Some(&reading(temperature, Duration::zero())),
                Some(HeaterCommand::On),
                &config(),
            );

            assert_eq!(
                decision,
                Decision::new(HeaterCommand::On, Reason::WithinDeadband),
                "temperature {temperature}"
            );
        }
    }

    #[test]
    fn person_present_requires_a_person_event_in_the_window() {
        let window_start = now() - Duration::seconds(900);

        assert!(person_present(
            &[detection(
                now(),
                vec![RecordingType::Motion, RecordingType::Person]
            )],
            window_start
        ));
        assert!(
            !person_present(
                &[detection(now(), vec![RecordingType::Motion])],
                window_start
            ),
            "motion alone is not a person"
        );
        assert!(
            !person_present(
                &[detection(
                    window_start - Duration::seconds(1),
                    vec![RecordingType::Person]
                )],
                window_start
            ),
            "a detection that ended before the window does not count"
        );
        assert!(!person_present(&[], window_start));
    }

    #[test]
    fn detection_ending_exactly_at_window_start_counts() {
        let window_start = now() - Duration::seconds(900);

        assert!(person_present(
            &[detection(window_start, vec![RecordingType::Person])],
            window_start
        ));
    }

    #[test]
    fn reading_age_is_floored_at_zero() {
        let evaluation = |age: Duration| Evaluation {
            decided_at: now(),
            active_period: None,
            presence: None,
            reading: Some(reading(21.0, age)),
            decision: Decision::new(HeaterCommand::Off, Reason::OutsideActivePeriod),
        };

        assert_eq!(evaluation(Duration::seconds(42)).reading_age_s(), Some(42));
        // The wall clock was stepped back since the reading was received.
        assert_eq!(evaluation(Duration::seconds(-1)).reading_age_s(), Some(0));
    }
}
