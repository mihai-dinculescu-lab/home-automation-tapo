use actix::{Addr, Message};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tapo::responses::DeviceUsageEnergyMonitoringResult;

use crate::settings::Device;
use crate::system::devices::device_session_actor::DeviceSessionActor;
use crate::system::devices::{DeviceType, Error as DeviceError};

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
