use chrono::{DateTime, Duration, Utc};
use derive_more::Display;
use tapo::responses::{DetectionResult, RecordingType};

use crate::settings::Thermostat;
use crate::system::messages::SensorReading;

/// The camera could not be queried, so presence is unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresenceError;

/// Whether a person is present, or `Err` when the camera could not be queried.
pub type Presence = Result<bool, PresenceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum HeaterCommand {
    On,
    Off,
}

/// Why the heaters were commanded. The first one is recorded by the actor rather than by
/// `decide`, which is only called inside the active period, so that the span's vocabulary stays
/// in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum Reason {
    /// The tick fell outside the active period.
    OutsideActivePeriod,
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

/// A person counts as present when any detection has `Person` among its events and ended at or
/// after `window_start`.
pub fn person_present(detections: &[DetectionResult], window_start: DateTime<Utc>) -> bool {
    detections.iter().any(|detection| {
        detection.end_time >= window_start && detection.events.contains(&RecordingType::Person)
    })
}

/// Decides what to do with the heaters. The rules are evaluated in order; the first match wins.
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
    presence: Presence,
    reading: Option<&SensorReading>,
    last_command: Option<HeaterCommand>,
    config: &Thermostat,
) -> Decision {
    match presence {
        Err(PresenceError) => return Decision::new(HeaterCommand::Off, Reason::CameraError),
        Ok(false) => return Decision::new(HeaterCommand::Off, Reason::NobodyPresent),
        Ok(true) => {}
    }

    let max_age = Duration::seconds(config.reading_max_age_s as i64);

    let Some(reading) = reading else {
        return Decision::new(HeaterCommand::Off, Reason::NoReading);
    };

    if now - reading.received_at > max_age {
        return Decision::new(HeaterCommand::Off, Reason::StaleReading);
    }

    if reading.temperature > config.target_temperature + config.deadband {
        Decision::new(HeaterCommand::Off, Reason::AboveTarget)
    } else if reading.temperature < config.target_temperature - config.deadband {
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

    fn config() -> Thermostat {
        Thermostat {
            name: "Living Room".to_string(),
            enabled: true,
            active_period: None,
            camera_ip_address: "192.168.1.154".to_string(),
            sensor_location: "Living Room".to_string(),
            heaters: vec![],
            target_temperature: 22.0,
            deadband: 0.25,
            presence_window_s: 900,
            reading_max_age_s: 900,
            interval_s: 300,
        }
    }

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
    fn camera_error_turns_off() {
        let decision = decide(
            now(),
            Err(PresenceError),
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
            Ok(false),
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
        let decision = decide(now(), Ok(true), None, None, &config());

        assert_eq!(
            decision,
            Decision::new(HeaterCommand::Off, Reason::NoReading)
        );
    }

    #[test]
    fn stale_reading_turns_off() {
        let decision = decide(
            now(),
            Ok(true),
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
            Ok(true),
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
            Ok(true),
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
            Ok(true),
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
                Ok(true),
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
            Ok(true),
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
                Ok(true),
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
}
