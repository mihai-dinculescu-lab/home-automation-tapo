use actix::{Addr, Message};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tapo::responses::DeviceUsageEnergyMonitoringResult;

use crate::settings::{ActivePeriod, Device};
use crate::system::devices::device_session_actor::DeviceSessionActor;
use crate::system::devices::{DeviceType, Error as DeviceError};
use crate::system::thermostat::decision::{Evaluation, HeaterCommand, HeaterSwitch, Reason};

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct StopMessage;

/// Sent by the API's health check. The reply carries nothing: the round trip itself is the
/// answer, since it proves that the coordinator is running and draining its mailbox.
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct ProbeMessage;

/// A reading received from a sensor over MQTT.
#[derive(Debug, Clone, PartialEq)]
pub struct SensorReading {
    pub location: String,
    pub temperature: f64,
    /// When the reading was received, not when the sensor took it; the payload carries no timestamp.
    pub received_at: DateTime<Utc>,
}

impl SensorReading {
    /// How old the reading is at `now`. Negative for one received after `now`.
    pub fn age(&self, now: DateTime<Utc>) -> chrono::Duration {
        now - self.received_at
    }
}

/// Sent by the sensors actor for every reading it receives, and forwarded by the coordinator to
/// the thermostats whose `sensor_location` is the reading's, each of which keeps it and ticks on
/// it.
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct SensorReadingMessage {
    pub span_context: opentelemetry::Context,
    pub reading: SensorReading,
}

/// Asks the coordinator for the session actor of the device at `ip_address`, which it creates
/// on first use. `None` once the service is shutting down. Not traced: the lookup does nothing
/// slow, and the request it precedes carries the caller's span.
#[derive(Debug, Message)]
#[rtype(result = "Option<Addr<DeviceSessionActor>>")]
pub struct GetDeviceSessionMessage {
    pub ip_address: String,
}

/// The requests run on a device's session, sent through `DeviceSessions`. The session is only
/// reused for the `device_type` it was logged in as.
#[derive(Debug, Message)]
#[rtype(result = "Result<(), DeviceError>")]
pub struct TurnOnMessage {
    pub span_context: opentelemetry::Context,
    pub device_type: DeviceType,
}

#[derive(Debug, Message)]
#[rtype(result = "Result<(), DeviceError>")]
pub struct TurnOffMessage {
    pub span_context: opentelemetry::Context,
    pub device_type: DeviceType,
}

#[derive(Debug, Message)]
#[rtype(result = "Result<bool, DeviceError>")]
pub struct GetDeviceOnMessage {
    pub span_context: opentelemetry::Context,
    pub device_type: DeviceType,
}

#[derive(Debug, Message)]
#[rtype(result = "Result<DeviceUsageEnergyMonitoringResult, DeviceError>")]
pub struct GetDeviceUsageMessage {
    pub span_context: opentelemetry::Context,
    pub device_type: DeviceType,
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct DeviceUsageMessage {
    pub span_context: opentelemetry::Context,
    /// The topic the usage is published on.
    pub topic_name: String,
    pub device: Device,
    pub device_usage: DeviceUsageEnergyMonitoringResult,
}

#[derive(Serialize)]
pub struct MqttMessagePayload {
    device_name: String,
    // Today's time usage in minutes
    time_usage_today: Option<u64>,
    // Past 7 days time usage in minutes
    time_usage_past7: Option<u64>,
    // Past 30 days time usage in minutes
    time_usage_past30: Option<u64>,
    // Today's power usage in watt-hour (Wh)
    power_usage_today: Option<u64>,
    // Past 7 days power usage in watt-hour (Wh)
    power_usage_past7: Option<u64>,
    // Today's power usage in watt-hour (Wh)
    power_usage_past30: Option<u64>,
}

impl From<(Device, DeviceUsageEnergyMonitoringResult)> for MqttMessagePayload {
    fn from(data: (Device, DeviceUsageEnergyMonitoringResult)) -> Self {
        let (device, dur) = data;

        MqttMessagePayload {
            device_name: device.name,
            time_usage_today: match device.record_time_usage {
                true => dur.time_usage.today,
                false => None,
            },
            time_usage_past7: match device.record_time_usage {
                true => dur.time_usage.past7,
                false => None,
            },
            time_usage_past30: match device.record_time_usage {
                true => dur.time_usage.past30,
                false => None,
            },
            power_usage_today: dur.power_usage.today,
            power_usage_past7: dur.power_usage.past7,
            power_usage_past30: dur.power_usage.past30,
        }
    }
}

/// A thermostat's decision, sent once its tick has switched the heaters. A tick cancelled by the
/// next one sends none.
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct ThermostatDecisionMessage {
    pub span_context: opentelemetry::Context,
    /// The topic the decision is published on.
    pub topic_name: String,
    pub thermostat_name: String,
    pub evaluation: Evaluation,
    /// How switching each heater ended, in the order of the settings.
    pub heaters: Vec<HeaterSwitch>,
}

#[derive(Debug, Serialize)]
pub struct ThermostatDecisionPayload {
    thermostat_name: String,
    decided_at: DateTime<Utc>,
    command: HeaterCommand,
    reason: Reason,
    // The period the tick ran under, null outside every period
    active_period: Option<ActivePeriod>,
    // Null when the camera was not queried or could not be
    person_detected: Option<bool>,
    // The latest reading in °C, null when there is none
    temperature: Option<f64>,
    // The age of the latest reading in seconds, null when there is none
    reading_age_s: Option<i64>,
    heaters: Vec<HeaterSwitch>,
}

impl ThermostatDecisionPayload {
    pub fn new(
        thermostat_name: String,
        evaluation: Evaluation,
        heaters: Vec<HeaterSwitch>,
    ) -> Self {
        ThermostatDecisionPayload {
            thermostat_name,
            decided_at: evaluation.decided_at,
            command: evaluation.decision.command,
            reason: evaluation.decision.reason,
            person_detected: evaluation.presence.and_then(Result::ok),
            temperature: evaluation
                .reading
                .as_ref()
                .map(|reading| reading.temperature),
            reading_age_s: evaluation.reading_age_s(),
            active_period: evaluation.active_period,
            heaters,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::system::thermostat::decision::{Decision, PresenceError, SwitchOutcome};

    fn decided_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 28, 14, 0, 0).unwrap()
    }

    fn payload(evaluation: Evaluation, heaters: Vec<HeaterSwitch>) -> serde_json::Value {
        json!(ThermostatDecisionPayload::new(
            "Living Room".to_string(),
            evaluation,
            heaters
        ))
    }

    #[test]
    fn decision_in_a_period_carries_its_inputs() {
        let evaluation = Evaluation {
            decided_at: decided_at(),
            active_period: Some(ActivePeriod::new("07:00", "22:00", 21.0, true)),
            presence: Some(Ok(true)),
            reading: Some(SensorReading {
                location: "Living Room".to_string(),
                temperature: 20.1,
                received_at: decided_at() - chrono::Duration::seconds(42),
            }),
            decision: Decision::new(HeaterCommand::On, Reason::BelowTarget),
        };
        let heaters = vec![
            HeaterSwitch {
                name: "Radiator".to_string(),
                outcome: SwitchOutcome::Switched,
            },
            HeaterSwitch {
                name: "Fan Heater".to_string(),
                outcome: SwitchOutcome::Failed,
            },
        ];

        assert_eq!(
            payload(evaluation, heaters),
            json!({
                "thermostat_name": "Living Room",
                "decided_at": "2026-10-28T14:00:00Z",
                "command": "on",
                "reason": "below_target",
                "active_period": {
                    "start": "07:00",
                    "end": "22:00",
                    "target_temperature": 21.0,
                    "require_presence": true,
                },
                "person_detected": true,
                "temperature": 20.1,
                "reading_age_s": 42,
                "heaters": [
                    { "name": "Radiator", "outcome": "switched" },
                    { "name": "Fan Heater", "outcome": "failed" },
                ],
            })
        );
    }

    #[test]
    fn camera_error_is_published_as_unknown_presence() {
        let evaluation = Evaluation {
            decided_at: decided_at(),
            active_period: Some(ActivePeriod::new("07:00", "22:00", 21.0, true)),
            presence: Some(Err(PresenceError)),
            reading: None,
            decision: Decision::new(HeaterCommand::Off, Reason::CameraError),
        };

        let payload = payload(evaluation, vec![]);

        assert_eq!(payload["person_detected"], json!(null));
    }

    // The spellings consumers match on, as listed in the README. The matches have no wildcard, so
    // a new variant does not compile until it has a spelling here; it is only tested once it is
    // also added to the list in `every_published_value_keeps_its_spelling`.

    fn command_spelling(command: HeaterCommand) -> &'static str {
        match command {
            HeaterCommand::On => "on",
            HeaterCommand::Off => "off",
        }
    }

    fn reason_spelling(reason: Reason) -> &'static str {
        match reason {
            Reason::OutsideActivePeriod => "outside_active_period",
            Reason::CameraError => "camera_error",
            Reason::NobodyPresent => "nobody_present",
            Reason::NoReading => "no_reading",
            Reason::StaleReading => "stale_reading",
            Reason::AboveTarget => "above_target",
            Reason::BelowTarget => "below_target",
            Reason::WithinDeadband => "within_deadband",
        }
    }

    fn outcome_spelling(outcome: SwitchOutcome) -> &'static str {
        match outcome {
            SwitchOutcome::Switched => "switched",
            SwitchOutcome::Failed => "failed",
            SwitchOutcome::ShuttingDown => "shutting_down",
        }
    }

    #[test]
    fn every_published_value_keeps_its_spelling() {
        for command in [HeaterCommand::On, HeaterCommand::Off] {
            assert_eq!(json!(command), json!(command_spelling(command)));
        }

        for reason in [
            Reason::OutsideActivePeriod,
            Reason::CameraError,
            Reason::NobodyPresent,
            Reason::NoReading,
            Reason::StaleReading,
            Reason::AboveTarget,
            Reason::BelowTarget,
            Reason::WithinDeadband,
        ] {
            assert_eq!(json!(reason), json!(reason_spelling(reason)));
        }

        for outcome in [
            SwitchOutcome::Switched,
            SwitchOutcome::Failed,
            SwitchOutcome::ShuttingDown,
        ] {
            assert_eq!(json!(outcome), json!(outcome_spelling(outcome)));
        }
    }
}
