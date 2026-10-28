use serde::Deserialize;
use tapo::responses::DeviceUsageEnergyMonitoringResult;
use tapo::{
    ApiClient, ColorLightHandler, LightHandler, PlugEnergyMonitoringHandler, PlugHandler,
    RgbLightStripHandler, RgbicLightStripHandler,
};
use tracing::instrument;

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

/// A logged-in session with a device.
pub enum DeviceHandler {
    Light(LightHandler),
    ColorLight(ColorLightHandler),
    RgbLightStrip(RgbLightStripHandler),
    RgbicLightStrip(RgbicLightStripHandler),
    Plug(PlugHandler),
    PlugEnergyMonitoring(PlugEnergyMonitoringHandler),
}

impl DeviceHandler {
    #[instrument(name = "DeviceHandler::new", skip_all, fields(
        device.ip_address = %ip_address,
        device.device_type = ?device_type,
    ))]
    pub async fn new(
        client: ApiClient,
        device_type: DeviceType,
        ip_address: String,
    ) -> Result<Self, tapo::Error> {
        match device_type {
            DeviceType::Light => client.l510(ip_address).await.map(Self::Light),
            DeviceType::ColorLight => client.l530(ip_address).await.map(Self::ColorLight),
            DeviceType::RgbLightStrip => client.l900(ip_address).await.map(Self::RgbLightStrip),
            DeviceType::RgbicLightStrip => client.l920(ip_address).await.map(Self::RgbicLightStrip),
            DeviceType::Plug => client.p100(ip_address).await.map(Self::Plug),
            DeviceType::PlugEnergyMonitoring => client
                .p110(ip_address)
                .await
                .map(Self::PlugEnergyMonitoring),
        }
    }

    pub fn device_type(&self) -> DeviceType {
        match self {
            Self::Light(_) => DeviceType::Light,
            Self::ColorLight(_) => DeviceType::ColorLight,
            Self::RgbLightStrip(_) => DeviceType::RgbLightStrip,
            Self::RgbicLightStrip(_) => DeviceType::RgbicLightStrip,
            Self::Plug(_) => DeviceType::Plug,
            Self::PlugEnergyMonitoring(_) => DeviceType::PlugEnergyMonitoring,
        }
    }

    #[instrument(name = "DeviceHandler::on", skip_all)]
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

    #[instrument(name = "DeviceHandler::off", skip_all)]
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

    /// Whether the device is on.
    #[instrument(name = "DeviceHandler::device_on", skip_all)]
    pub async fn device_on(&self) -> Result<bool, tapo::Error> {
        match self {
            Self::Light(handler) => handler.get_device_info().await.map(|i| i.device_on),
            Self::ColorLight(handler) => handler.get_device_info().await.map(|i| i.device_on),
            Self::RgbLightStrip(handler) => handler.get_device_info().await.map(|i| i.device_on),
            Self::RgbicLightStrip(handler) => handler.get_device_info().await.map(|i| i.device_on),
            Self::Plug(handler) => handler.get_device_info().await.map(|i| i.device_on),
            Self::PlugEnergyMonitoring(handler) => {
                handler.get_device_info().await.map(|i| i.device_on)
            }
        }
    }

    /// The device's usage, which only a `PlugEnergyMonitoring` reports.
    #[instrument(name = "DeviceHandler::device_usage", skip_all)]
    pub async fn device_usage(&self) -> Result<DeviceUsageEnergyMonitoringResult, tapo::Error> {
        match self {
            Self::PlugEnergyMonitoring(handler) => handler.get_device_usage().await,
            _ => Err(tapo::Error::Other(anyhow::anyhow!(
                "Only a plug_energy_monitoring reports its usage"
            ))),
        }
    }
}
