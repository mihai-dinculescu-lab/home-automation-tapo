use chrono::{DateTime, Utc};
use derive_more::Display;
use tapo::responses::{
    ChildDeviceHubIrResult, ChildDeviceHubResult, RecordingType, Status, T100Event,
};
use tracing::debug;

use crate::settings::{HubType, PresenceSource, Tapo};
use crate::system::thermostat::decision::{Presence, PresenceError};
use crate::telemetry::record_error;

/// Why a presence source could not answer. Each is unknown presence, never nobody present.
#[derive(Debug, Display)]
enum QueryError {
    /// The device could not be reached, or refused the request.
    #[display("{_0}")]
    Tapo(tapo::Error),
    /// The camera at the address carries another name than the one in the settings.
    #[display("The camera at this address has another name")]
    NameMismatch,
    /// The hub lists no T100 with the nickname in the settings.
    #[display("The hub has no T100 with this name")]
    SensorNotFound,
    /// The hub lists more than one T100 with the nickname in the settings, so which room the
    /// motion is from is unknown.
    #[display("The hub has more than one T100 with this name")]
    SensorAmbiguous,
    /// The hub lists the T100 as offline, so its log says nothing about now.
    #[display("The T100 is offline")]
    SensorOffline,
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Tapo(e) => e.source(),
            Self::NameMismatch
            | Self::SensorNotFound
            | Self::SensorAmbiguous
            | Self::SensorOffline => None,
        }
    }
}

impl From<tapo::Error> for QueryError {
    fn from(e: tapo::Error) -> Self {
        Self::Tapo(e)
    }
}

/// The handler and the status of the T100 whose nickname is `$name` on `$hub`, whose child list
/// holds `$child`s. A hub that lists no such sensor, or more than one, is an error: with two of
/// them, which room the motion is from is unknown.
///
/// The list is read once and does three jobs: it finds the sensor, gives the id its logs are
/// asked by and carries its status. The checked `t100` builder would read the same list and
/// throw the rest away.
///
/// A macro rather than a function: every hub has a handler type of its own, and they share the
/// methods used here by name only, not through a trait.
macro_rules! t100_on_hub {
    ($hub:expr, $child:ident, $name:expr) => {{
        let hub = $hub;

        let mut sensors = hub
            .get_child_device_list()
            .await?
            .into_iter()
            .filter_map(|child| match child {
                $child::T100(sensor) if sensor.nickname == *$name => Some(sensor),
                _ => None,
            });

        match (sensors.next(), sensors.next()) {
            (Some(sensor), None) => Ok((hub.t100_unchecked(sensor.device_id), sensor.status)),
            (None, _) => Err(QueryError::SensorNotFound),
            (Some(_), Some(_)) => Err(QueryError::SensorAmbiguous),
        }
    }};
}

/// Asks `source` whether anyone was present in its window, which ends at `now`.
///
/// The device is only ever queried here, and a tick cancels the previous one, so it is logged
/// into on every query rather than through a session.
///
/// A failure is recorded on the caller's span and returned as unknown presence, never as nobody
/// present.
pub async fn query(tapo: &Tapo, source: &PresenceSource, now: DateTime<Utc>) -> Presence {
    let window_start = now - chrono::Duration::seconds(source.window_s() as i64);

    let result = async {
        let detections = match source {
            PresenceSource::Camera {
                ip_address, name, ..
            } => {
                let handler = tapo.client().c120(ip_address.clone()).await?;

                if handler.get_device_info().await?.nickname != *name {
                    return Err(QueryError::NameMismatch);
                }

                handler.get_detections(window_start, now).await?
            }
            PresenceSource::CameraPtz {
                ip_address, name, ..
            } => {
                let handler = tapo.client().c220(ip_address.clone()).await?;

                if handler.get_device_info().await?.nickname != *name {
                    return Err(QueryError::NameMismatch);
                }

                handler.get_detections(window_start, now).await?
            }
            PresenceSource::T100 {
                hub_type,
                ip_address,
                name,
                ..
            } => {
                let client = tapo.client();
                let ip_address = ip_address.clone();

                let (sensor, status) = match hub_type {
                    HubType::Hub => {
                        t100_on_hub!(client.h100(ip_address).await?, ChildDeviceHubResult, name)
                    }
                    HubType::HubIr => {
                        t100_on_hub!(client.h110(ip_address).await?, ChildDeviceHubIrResult, name)
                    }
                    // `h200` and `h500` log in the same way and build the same handler.
                    HubType::CameraHub => {
                        t100_on_hub!(client.h200(ip_address).await?, ChildDeviceHubResult, name)
                    }
                }?;

                if matches!(status, Status::Offline) {
                    return Err(QueryError::SensorOffline);
                }

                // The sensor's `detected` flag is not read: it has no time of its own, so one
                // left set by a sensor that died would keep the heaters on past the window.
                let last_triggered_at =
                    sensor
                        .get_trigger_logs(1, 0)
                        .await?
                        .logs
                        .first()
                        .map(|log| match log.event {
                            // Exhaustive, so that an event tapo adds later, such as a low battery,
                            // has to be judged here rather than being read as motion.
                            T100Event::Motion => log.triggered_at,
                        });

                debug!("Motion last triggered at {last_triggered_at:?}");

                // Motion counts as presence when the newest trigger log was triggered at or
                // after `window_start`. An empty log, as on a sensor just paired, is nobody
                // present. The log's time is the hub's and `window_start` this service's, so a
                // hub whose clock runs ahead stretches the window by the difference.
                return Ok(
                    last_triggered_at.is_some_and(|triggered_at| triggered_at >= window_start)
                );
            }
        };

        debug!("{} detections in the presence window", detections.len());

        // A person counts as present when any detection has `Person` among its events and
        // ended at or after `window_start`.
        Ok(detections.iter().any(|detection| {
            detection.end_time >= window_start && detection.events.contains(&RecordingType::Person)
        }))
    }
    .await;

    result.map_err(|e: QueryError| {
        record_error!(&e);
        PresenceError
    })
}
