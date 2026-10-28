use std::error::Error;
use std::time::Duration;

use actix::Recipient;
use actix_web::http::StatusCode;
use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};
use tracing::{instrument, warn};

use crate::system::api::errors::ApiError;
use crate::system::devices::{DeviceSessions, DeviceType, Error as DeviceError};
use crate::system::messages::ProbeMessage;
use crate::telemetry::record_error;

/// How long the coordinator gets to answer a probe. The round trip is a message to a handler
/// that does nothing, so the budget only has to absorb a busy moment on the actors' thread.
/// The Kubernetes probes allow a little more than this, so that a timeout here is what fails
/// them rather than the kubelet giving up first.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiStatusResponse {
    pub code: u16,
    pub message: String,
}

impl ApiStatusResponse {
    pub fn new(status_code: StatusCode, message: &str) -> Self {
        Self {
            code: status_code.as_u16(),
            message: message.to_string(),
        }
    }
}

/// The state of a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceState {
    pub on: Option<bool>,
}

#[derive(Deserialize)]
pub struct SetDevicePayload {
    ip_address: String,
    device_type: DeviceType,
    state: DeviceState,
}

#[derive(Deserialize)]
pub struct GetDevicePayload {
    ip_address: String,
    device_type: DeviceType,
}

#[derive(Serialize)]
pub struct DeviceResponse {
    ip_address: String,
    state: DeviceState,
}

/// `200 OK` when the coordinator answers a probe in time, `503 Service Unavailable` otherwise.
///
/// The web server answers from its own threads, so on its own it would keep reporting a healthy
/// service after the coordinator has stopped (a closed mailbox) or the thread that every actor
/// runs on has got stuck (a timeout). The round trip is what lets Kubernetes restart the pod in
/// either case.
#[instrument(name = "health_check", skip_all)]
pub async fn health_check(probe_recipient: web::Data<Recipient<ProbeMessage>>) -> HttpResponse {
    // The timeout wraps the whole send rather than using actix's own, which only starts once
    // the message has been queued: a full mailbox would otherwise hang the probe for good.
    match tokio::time::timeout(PROBE_TIMEOUT, probe_recipient.send(ProbeMessage)).await {
        Ok(Ok(())) => {
            let body = ApiStatusResponse::new(StatusCode::OK, "OK");

            HttpResponse::Ok().json(body)
        }
        Ok(Err(e)) => coordinator_unavailable(&e),
        Err(e) => coordinator_unavailable(&e),
    }
}

fn coordinator_unavailable<E: Error>(e: &E) -> HttpResponse {
    record_error!(e);

    let body = ApiStatusResponse::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "The coordinator did not answer",
    );

    HttpResponse::ServiceUnavailable().json(body)
}

#[instrument(name = "get_device", skip_all, fields(
    device.ip_address = %device.ip_address,
    device.device_type = ?device.device_type,
))]
pub async fn get_device(
    device_sessions: web::Data<DeviceSessions>,
    device: web::Json<GetDevicePayload>,
) -> Result<HttpResponse, ApiError> {
    let on = device_sessions
        .device_on(device.device_type, &device.ip_address)
        .await
        .map_err(api_error)?;

    let result = DeviceResponse {
        ip_address: device.ip_address.clone(),
        state: DeviceState { on: Some(on) },
    };

    Ok(HttpResponse::Ok().json(result))
}

#[instrument(name = "set_device", skip_all, fields(
    device.ip_address = %device.ip_address,
    device.device_type = ?device.device_type,
    device.state.on = ?device.state.on,
))]
pub async fn set_device(
    device_sessions: web::Data<DeviceSessions>,
    device: web::Json<SetDevicePayload>,
) -> Result<HttpResponse, ApiError> {
    let (device_type, ip_address) = (device.device_type, &device.ip_address);

    let on = match device.state.on {
        Some(true) => device_sessions
            .turn_on(device_type, ip_address)
            .await
            .map(|()| true),
        Some(false) => device_sessions
            .turn_off(device_type, ip_address)
            .await
            .map(|()| false),
        // Without a state to set, the device is asked for its state instead, so that an
        // unreachable one is still reported.
        None => device_sessions.device_on(device_type, ip_address).await,
    }
    .map_err(api_error)?;

    let result = DeviceResponse {
        ip_address: device.ip_address.clone(),
        state: DeviceState { on: Some(on) },
    };

    Ok(HttpResponse::Ok().json(result))
}

/// A failed login is the client's error, e.g. a wrong address, so the request span isn't marked
/// as failed. So is a device that cannot be reached on a session already open, since it fails
/// the same way whether or not a session is cached; anything else a device answered is ours.
///
/// A shutdown is expected, so it isn't marked as failed either; a session actor that stopped
/// without being asked to is. Both are a `503`, since the request may succeed once the service
/// is back.
fn api_error(e: DeviceError) -> ApiError {
    match &e {
        DeviceError::Login(cause) | DeviceError::Request(cause @ tapo::Error::Http(_)) => {
            warn!(error = %cause, "Failed to connect to the device");
            ApiError::BadRequest("Failed to connect to the device".to_string())
        }
        DeviceError::ShuttingDown => {
            warn!(error = %e, "Refused a device request");
            ApiError::ServiceUnavailable
        }
        DeviceError::Unavailable(_) => {
            record_error!(&e);
            ApiError::ServiceUnavailable
        }
        DeviceError::Request(_) => {
            record_error!(&e);
            ApiError::InternalServerError
        }
    }
}
