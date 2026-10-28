use actix::{Handler, MailboxError, Recipient};
use derive_more::Display;
use tapo::responses::DeviceUsageEnergyMonitoringResult;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::system::devices::device_handler::DeviceType;
use crate::system::devices::device_session_actor::{DeviceSessionActor, Request};
use crate::system::messages::{
    GetDeviceOnMessage, GetDeviceSessionMessage, GetDeviceUsageMessage, TurnOffMessage,
    TurnOnMessage,
};

/// Why an operation on a device failed.
#[derive(Debug, Display)]
pub enum Error {
    /// The login failed, so there is no session and the device was never asked anything.
    #[display("Failed to log in to the device")]
    Login(tapo::Error),
    /// The request failed on an open session, which has been dropped.
    #[display("The request to the device failed")]
    Request(tapo::Error),
    /// The device's session actor, or the coordinator that hands it out, has stopped without
    /// being asked to.
    #[display("The device's session is unavailable")]
    Unavailable(MailboxError),
    /// The service is shutting down, so the coordinator no longer hands out sessions.
    #[display("The service is shutting down")]
    ShuttingDown,
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Login(e) | Self::Request(e) => Some(e),
            Self::Unavailable(e) => Some(e),
            Self::ShuttingDown => None,
        }
    }
}

/// Reaches each device through its session actor, which the coordinator hands out.
#[derive(Debug, Clone)]
pub struct DeviceSessions {
    session_recipient: Recipient<GetDeviceSessionMessage>,
}

impl DeviceSessions {
    pub fn new(session_recipient: Recipient<GetDeviceSessionMessage>) -> Self {
        Self { session_recipient }
    }

    pub async fn turn_on(&self, device_type: DeviceType, ip_address: &str) -> Result<(), Error> {
        let request = TurnOnMessage {
            span_context: Span::current().context(),
            device_type,
        };

        self.send(ip_address, request).await
    }

    pub async fn turn_off(&self, device_type: DeviceType, ip_address: &str) -> Result<(), Error> {
        let request = TurnOffMessage {
            span_context: Span::current().context(),
            device_type,
        };

        self.send(ip_address, request).await
    }

    /// Whether the device is on.
    pub async fn device_on(
        &self,
        device_type: DeviceType,
        ip_address: &str,
    ) -> Result<bool, Error> {
        let request = GetDeviceOnMessage {
            span_context: Span::current().context(),
            device_type,
        };

        self.send(ip_address, request).await
    }

    /// The device's usage, which only a `PlugEnergyMonitoring` reports.
    pub async fn device_usage(
        &self,
        device_type: DeviceType,
        ip_address: &str,
    ) -> Result<DeviceUsageEnergyMonitoringResult, Error> {
        let request = GetDeviceUsageMessage {
            span_context: Span::current().context(),
            device_type,
        };

        self.send(ip_address, request).await
    }

    async fn send<R>(&self, ip_address: &str, request: R) -> Result<R::Output, Error>
    where
        R: Request + Send,
        R::Output: Send,
        DeviceSessionActor: Handler<R>,
    {
        let device_session_actor = self
            .session_recipient
            .send(GetDeviceSessionMessage {
                ip_address: ip_address.to_string(),
            })
            .await
            .map_err(Error::Unavailable)?
            .ok_or(Error::ShuttingDown)?;

        device_session_actor
            .send(request)
            .await
            .map_err(Error::Unavailable)?
    }
}
