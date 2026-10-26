use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use derive_more::Display;
use futures_util::future::LocalBoxFuture;
use serde::Deserialize;
use tapo::responses::DeviceUsageEnergyMonitoringResult;
use tapo::{
    ApiClient, ColorLightHandler, LightHandler, PlugEnergyMonitoringHandler, PlugHandler,
    RgbLightStripHandler, RgbicLightStripHandler, TapoResponseError,
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::instrument;

use crate::settings::Tapo;

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

/// Why an operation on a device failed.
#[derive(Debug, Display)]
pub enum Error {
    /// The login failed, so there is no session and the device was never asked anything.
    #[display("Failed to log in to the device")]
    Login(tapo::Error),
    /// The request failed on an open session, which has been dropped.
    #[display("The request to the device failed")]
    Request(tapo::Error),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Login(e) | Self::Request(e) => Some(e),
        }
    }
}

type Session = Arc<AsyncMutex<Option<DeviceHandler>>>;

/// The devices, with one session each that one caller at a time runs on.
///
/// A Tapo device keeps a single session, so a login while another caller is mid-request fails
/// both. Every operation on a device takes its lock first, behind whoever holds it, and runs on
/// the session already open; a login only happens when there is none. A session the device no
/// longer accepts is reopened and the operation retried once; any other failure drops the
/// session, so that the next operation logs in afresh rather than reuse one that may be broken.
#[derive(Clone)]
pub struct Devices {
    config: Tapo,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
}

impl Devices {
    pub fn new(config: Tapo) -> Self {
        Self {
            config,
            sessions: Arc::default(),
        }
    }

    pub fn config(&self) -> &Tapo {
        &self.config
    }

    /// The session slot of the device at `ip_address`, created on first use.
    fn session(&self, ip_address: &str) -> Session {
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(ip_address.to_string())
            .or_default()
            .clone()
    }

    async fn login(
        &self,
        device_type: DeviceType,
        ip_address: &str,
    ) -> Result<DeviceHandler, Error> {
        DeviceHandler::new(self.config.client(), device_type, ip_address.to_string())
            .await
            .map_err(Error::Login)
    }

    /// Runs `op` on the device at `ip_address`, once any operation already running on it is
    /// done. A session is only reused for the `device_type` it was logged in as.
    ///
    /// The session stays in its slot while `op` runs, so that a caller cancelled mid-request
    /// leaves it for the next one rather than force a login while the device is still busy.
    pub async fn run<T>(
        &self,
        device_type: DeviceType,
        ip_address: &str,
        op: impl for<'a> Fn(&'a DeviceHandler) -> LocalBoxFuture<'a, Result<T, tapo::Error>>,
    ) -> Result<T, Error> {
        let session = self.session(ip_address);
        let mut session = session.lock().await;

        let mut reused = matches!(&*session, Some(handler) if handler.device_type() == device_type);

        loop {
            let handler = match &*session {
                Some(handler) if reused => handler,
                _ => {
                    // Cleared first, so that a failed login does not leave a session that is
                    // known to be dead for the next caller to try.
                    *session = None;
                    session.insert(self.login(device_type, ip_address).await?)
                }
            };

            match op(handler).await {
                Ok(result) => return Ok(result),
                // A reused session that the device turns down is expected now and then: it has
                // expired, or a cancelled request left it out of step. A fresh one gets one retry.
                Err(e) if reused && session_rejected(&e) => reused = false,
                Err(e) => {
                    *session = None;
                    return Err(Error::Request(e));
                }
            }
        }
    }
}

/// Whether the device turned the session down, as opposed to being unreachable or having
/// answered the request. A 401, a 403 and the session-timeout codes all come back as
/// `Unauthorized`.
fn session_rejected(e: &tapo::Error) -> bool {
    matches!(e, tapo::Error::Tapo(TapoResponseError::Unauthorized { .. }))
}

impl fmt::Debug for Devices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Devices")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
