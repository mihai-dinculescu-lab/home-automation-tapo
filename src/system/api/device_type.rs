use serde::{Deserialize, Serialize};
use tapo::{
    ApiClient, ColorLightHandler, LightHandler, PlugEnergyMonitoringHandler, PlugHandler,
    RgbLightStripHandler, RgbicLightStripHandler,
};

/// The type of a device. Each variant maps to a Tapo handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    /// L510, L520, L610
    Light,
    /// L530, L535, L630
    ColorLight,
    /// L900
    RgbLightStrip,
    /// L920, L930
    RgbicLightStrip,
    /// P100, P105
    Plug,
    /// P110, P115
    PlugEnergyMonitoring,
}

/// The state of a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceState {
    pub on: Option<bool>,
}

pub enum DeviceHandler {
    Light(LightHandler),
    ColorLight(ColorLightHandler),
    RgbLightStrip(RgbLightStripHandler),
    RgbicLightStrip(RgbicLightStripHandler),
    Plug(PlugHandler),
    PlugEnergyMonitoring(PlugEnergyMonitoringHandler),
}

impl DeviceHandler {
    pub async fn new(
        client: ApiClient,
        device_type: DeviceType,
        ip_address: String,
    ) -> Result<Self, tapo::Error> {
        let handler = match device_type {
            DeviceType::Light => Self::Light(client.l510(ip_address).await?),
            DeviceType::ColorLight => Self::ColorLight(client.l530(ip_address).await?),
            DeviceType::RgbLightStrip => Self::RgbLightStrip(client.l900(ip_address).await?),
            DeviceType::RgbicLightStrip => Self::RgbicLightStrip(client.l920(ip_address).await?),
            DeviceType::Plug => Self::Plug(client.p100(ip_address).await?),
            DeviceType::PlugEnergyMonitoring => {
                Self::PlugEnergyMonitoring(client.p110(ip_address).await?)
            }
        };

        Ok(handler)
    }

    pub async fn on(&self) -> Result<(), tapo::Error> {
        match self {
            Self::Light(handler) => handler.on().await,
            Self::ColorLight(handler) => handler.on().await,
            Self::RgbLightStrip(handler) => handler.on().await,
            Self::RgbicLightStrip(handler) => handler.on().await,
            Self::Plug(handler) => handler.on().await,
            Self::PlugEnergyMonitoring(handler) => handler.on().await,
        }
    }

    pub async fn off(&self) -> Result<(), tapo::Error> {
        match self {
            Self::Light(handler) => handler.off().await,
            Self::ColorLight(handler) => handler.off().await,
            Self::RgbLightStrip(handler) => handler.off().await,
            Self::RgbicLightStrip(handler) => handler.off().await,
            Self::Plug(handler) => handler.off().await,
            Self::PlugEnergyMonitoring(handler) => handler.off().await,
        }
    }

    pub async fn state(&self) -> Result<DeviceState, tapo::Error> {
        let on = match self {
            Self::Light(handler) => handler.get_device_info().await?.device_on,
            Self::ColorLight(handler) => handler.get_device_info().await?.device_on,
            Self::RgbLightStrip(handler) => handler.get_device_info().await?.device_on,
            Self::RgbicLightStrip(handler) => handler.get_device_info().await?.device_on,
            Self::Plug(handler) => handler.get_device_info().await?.device_on,
            Self::PlugEnergyMonitoring(handler) => handler.get_device_info().await?.device_on,
        };

        Ok(DeviceState { on: Some(on) })
    }
}
