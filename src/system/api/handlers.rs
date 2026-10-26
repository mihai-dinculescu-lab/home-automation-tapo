use actix_web::http::StatusCode;
use actix_web::{HttpResponse, web};
use futures_util::FutureExt as _;
use serde::{Deserialize, Serialize};
use tracing::{instrument, warn};

use crate::system::api::errors::ApiError;
use crate::system::devices::{DeviceType, Devices, Error as DeviceError};
use crate::telemetry::record_error;

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

#[instrument(name = "health_check", skip_all)]
pub async fn health_check() -> HttpResponse {
    let body = ApiStatusResponse::new(StatusCode::OK, "OK");

    HttpResponse::Ok().json(body)
}

#[instrument(name = "get_device", skip_all, fields(
    device.ip_address = %device.ip_address,
    device.device_type = ?device.device_type,
))]
pub async fn get_device(
    devices: web::Data<Devices>,
    device: web::Json<GetDevicePayload>,
) -> Result<HttpResponse, ApiError> {
    let on = devices
        .run(device.device_type, &device.ip_address, |handler| {
            handler.device_on().boxed_local()
        })
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
    devices: web::Data<Devices>,
    device: web::Json<SetDevicePayload>,
) -> Result<HttpResponse, ApiError> {
    let on = device.state.on;

    let on = devices
        .run(device.device_type, &device.ip_address, move |handler| {
            async move {
                match on {
                    Some(true) => handler.on().await.map(|_| true),
                    Some(false) => handler.off().await.map(|_| false),
                    // Without a state to set, the device is asked for its state instead, so that
                    // an unreachable one is still reported.
                    None => handler.device_on().await,
                }
            }
            .boxed_local()
        })
        .await
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
fn api_error(e: DeviceError) -> ApiError {
    match &e {
        DeviceError::Login(cause) | DeviceError::Request(cause @ tapo::Error::Http(_)) => {
            warn!(error = %cause, "Failed to connect to the device");
            ApiError::BadRequest("Failed to connect to the device".to_string())
        }
        _ => {
            record_error!(&e);
            ApiError::InternalServerError
        }
    }
}
