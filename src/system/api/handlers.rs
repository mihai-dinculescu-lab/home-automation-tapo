use actix_web::http::StatusCode;
use actix_web::{HttpResponse, web};
use serde::{Deserialize, Serialize};
use tapo::ApiClient;
use tracing::instrument;

use crate::settings::Tapo;
use crate::system::api::device_type::{DeviceHandler, DeviceState, DeviceType};
use crate::system::api::errors::ApiError;

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
    config: web::Data<Tapo>,
    device: web::Json<GetDevicePayload>,
) -> Result<HttpResponse, ApiError> {
    let client = ApiClient::new(config.username.clone(), config.password.clone());
    let handler = DeviceHandler::new(client, device.device_type, device.ip_address.clone())
        .await
        .map_err(|_| ApiError::BadRequest("failed to connect to the device".to_string()))?;

    let state = handler
        .state()
        .await
        .map_err(|_| ApiError::InternalServerError)?;

    let result = DeviceResponse {
        ip_address: device.ip_address.clone(),
        state,
    };

    Ok(HttpResponse::Ok().json(result))
}

#[instrument(name = "set_device", skip_all, fields(
    device.ip_address = %device.ip_address,
    device.device_type = ?device.device_type,
    device.state.on = ?device.state.on,
))]
pub async fn set_device(
    config: web::Data<Tapo>,
    device: web::Json<SetDevicePayload>,
) -> Result<HttpResponse, ApiError> {
    let client = ApiClient::new(config.username.clone(), config.password.clone());
    let handler = DeviceHandler::new(client, device.device_type, device.ip_address.clone())
        .await
        .map_err(|_| ApiError::BadRequest("failed to connect to the device".to_string()))?;

    match device.state.on {
        Some(true) => handler.on().await,
        Some(false) => handler.off().await,
        None => Ok(()),
    }
    .map_err(|_| ApiError::InternalServerError)?;

    let result = DeviceResponse {
        ip_address: device.ip_address.clone(),
        state: device.state,
    };

    Ok(HttpResponse::Ok().json(result))
}
