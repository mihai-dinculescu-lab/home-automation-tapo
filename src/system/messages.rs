use actix::Message;
use chrono::{DateTime, Utc};
use serde::Serialize;
use tapo::responses::DeviceUsageEnergyMonitoringResult;

use crate::settings::Device;

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct StopMessage;

/// Sent by the API's health check. The reply carries nothing: the round trip itself is the
/// answer, since it proves that the coordinator is running and draining its mailbox.
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct ProbeMessage;

/// The latest reading received from a sensor over MQTT.
#[derive(Debug, Clone, PartialEq)]
pub struct SensorReading {
    pub location: String,
    pub temperature: f64,
    /// When the reading was received, not when the sensor took it; the payload carries no timestamp.
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Message)]
#[rtype(result = "Option<SensorReading>")]
pub struct GetSensorReadingMessage {
    pub span_context: opentelemetry::Context,
    pub location: String,
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct DeviceUsageMessage {
    pub span_context: opentelemetry::Context,
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
