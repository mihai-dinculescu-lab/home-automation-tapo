use actix::{
    Actor, ActorContext, ActorFutureExt, AtomicResponse, Context, Handler, Message, WrapFuture,
};
use tapo::TapoResponseError;
use tapo::responses::DeviceUsageEnergyMonitoringResult;
use tracing::{Instrument, Span, instrument};

use crate::settings::Tapo;
use crate::system::devices::device_handler::{DeviceHandler, DeviceType};
use crate::system::devices::sessions::Error;
use crate::system::messages::{
    GetDeviceOnMessage, GetDeviceUsageMessage, StopMessage, TurnOffMessage, TurnOnMessage,
};
use crate::telemetry::consumer_span;

/// A request to a device, run on its session.
pub(super) trait Request: Message<Result = Result<Self::Output, Error>> + 'static {
    type Output: 'static;

    /// The type to log in as. A session is only reused for the type it was logged in as.
    fn device_type(&self) -> DeviceType;

    async fn run(&self, handler: &DeviceHandler) -> Result<Self::Output, tapo::Error>;
}

impl Request for TurnOnMessage {
    type Output = ();

    fn device_type(&self) -> DeviceType {
        self.device_type
    }

    async fn run(&self, handler: &DeviceHandler) -> Result<(), tapo::Error> {
        handler.on().await
    }
}

impl Request for TurnOffMessage {
    type Output = ();

    fn device_type(&self) -> DeviceType {
        self.device_type
    }

    async fn run(&self, handler: &DeviceHandler) -> Result<(), tapo::Error> {
        handler.off().await
    }
}

impl Request for GetDeviceOnMessage {
    type Output = bool;

    fn device_type(&self) -> DeviceType {
        self.device_type
    }

    async fn run(&self, handler: &DeviceHandler) -> Result<bool, tapo::Error> {
        handler.device_on().await
    }
}

impl Request for GetDeviceUsageMessage {
    type Output = DeviceUsageEnergyMonitoringResult;

    fn device_type(&self) -> DeviceType {
        self.device_type
    }

    async fn run(
        &self,
        handler: &DeviceHandler,
    ) -> Result<DeviceUsageEnergyMonitoringResult, tapo::Error> {
        handler.device_usage().await
    }
}

/// The session with one device, which runs one request at a time.
///
/// A Tapo device keeps a single session, so a login while another request is in progress fails
/// both. Each request is handled with `ctx.wait`, so the next one waits in the mailbox until it
/// is done, and runs on the session already open; a login only happens when there is none. A
/// session the device no longer accepts is reopened and the request retried once; any other
/// failure drops the session, so that the next request logs in afresh rather than reuse one that
/// may be broken.
///
/// A request whose caller gives up once it has started is still finished, so the device is never
/// left mid-request; one whose caller gave up while it was queued is skipped.
pub struct DeviceSessionActor {
    config: Tapo,
    ip_address: String,
    /// Taken out while a request runs on it and put back once it is done. No other request can
    /// find it missing meanwhile, since the mailbox waits.
    session: Option<DeviceHandler>,
}

impl DeviceSessionActor {
    pub fn new(config: Tapo, ip_address: String) -> Self {
        Self {
            config,
            ip_address,
            session: None,
        }
    }

    /// Runs `request` under `span`, holding the mailbox until it is done.
    fn handle_request<R>(&mut self, request: R, span: Span) -> AtomicResponse<Self, R::Result>
    where
        R: Request,
    {
        let fut = Self::run(
            request,
            self.session.take(),
            self.config.clone(),
            self.ip_address.clone(),
        )
        .instrument(span)
        .into_actor(self)
        .map(|(session, result), act, _| {
            act.session = session;
            result
        });

        AtomicResponse::new(Box::pin(fut))
    }

    /// Runs `request` on `session`, logging in first when there is none or it was logged in as
    /// another type. Returns the session to keep alongside the result.
    async fn run<R: Request>(
        request: R,
        mut session: Option<DeviceHandler>,
        config: Tapo,
        ip_address: String,
    ) -> (Option<DeviceHandler>, Result<R::Output, Error>) {
        let device_type = request.device_type();

        let mut reused = matches!(&session, Some(handler) if handler.device_type() == device_type);

        loop {
            let handler = match &session {
                Some(handler) if reused => handler,
                _ => {
                    // Cleared first, so that a failed login does not keep a session that is
                    // known to be dead for the next request to try.
                    session = None;

                    match DeviceHandler::new(config.client(), device_type, ip_address.clone()).await
                    {
                        Ok(handler) => session.insert(handler),
                        Err(e) => return (None, Err(Error::Login(e))),
                    }
                }
            };

            match request.run(handler).await {
                Ok(output) => return (session, Ok(output)),
                // A reused session that the device turns down is expected now and then, once it
                // has expired. A fresh one gets one retry.
                Err(e) if reused && session_rejected(&e) => reused = false,
                Err(e) => return (None, Err(Error::Request(e))),
            }
        }
    }
}

impl Actor for DeviceSessionActor {
    type Context = Context<Self>;

    #[instrument(name = "DeviceSessionActor::stopped", level = "error", skip_all, fields(
        device.ip_address = %self.ip_address,
    ))]
    fn stopped(&mut self, _: &mut Self::Context) {}
}

impl Handler<TurnOnMessage> for DeviceSessionActor {
    type Result = AtomicResponse<Self, Result<(), Error>>;

    fn handle(&mut self, message: TurnOnMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "DeviceSessionActor::Handler<TurnOnMessage>",
            message = TurnOnMessage,
            parent = message.span_context.clone(),
            destination = "DeviceSessionActor",
            device.ip_address = %self.ip_address,
            device.device_type = ?message.device_type,
        );

        self.handle_request(message, span)
    }
}

impl Handler<TurnOffMessage> for DeviceSessionActor {
    type Result = AtomicResponse<Self, Result<(), Error>>;

    fn handle(&mut self, message: TurnOffMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "DeviceSessionActor::Handler<TurnOffMessage>",
            message = TurnOffMessage,
            parent = message.span_context.clone(),
            destination = "DeviceSessionActor",
            device.ip_address = %self.ip_address,
            device.device_type = ?message.device_type,
        );

        self.handle_request(message, span)
    }
}

impl Handler<GetDeviceOnMessage> for DeviceSessionActor {
    type Result = AtomicResponse<Self, Result<bool, Error>>;

    fn handle(&mut self, message: GetDeviceOnMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "DeviceSessionActor::Handler<GetDeviceOnMessage>",
            message = GetDeviceOnMessage,
            parent = message.span_context.clone(),
            destination = "DeviceSessionActor",
            device.ip_address = %self.ip_address,
            device.device_type = ?message.device_type,
        );

        self.handle_request(message, span)
    }
}

impl Handler<GetDeviceUsageMessage> for DeviceSessionActor {
    type Result = AtomicResponse<Self, Result<DeviceUsageEnergyMonitoringResult, Error>>;

    fn handle(&mut self, message: GetDeviceUsageMessage, _: &mut Context<Self>) -> Self::Result {
        let span = consumer_span!(
            name = "DeviceSessionActor::Handler<GetDeviceUsageMessage>",
            message = GetDeviceUsageMessage,
            parent = message.span_context.clone(),
            destination = "DeviceSessionActor",
            device.ip_address = %self.ip_address,
            device.device_type = ?message.device_type,
        );

        self.handle_request(message, span)
    }
}

impl Handler<StopMessage> for DeviceSessionActor {
    type Result = ();

    /// Handled once the request in progress, if any, is done, since that holds the mailbox.
    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}

/// Whether the device turned the session down, as opposed to being unreachable or having
/// answered the request. A 401, a 403 and the session-timeout codes all come back as
/// `Unauthorized`.
fn session_rejected(e: &tapo::Error) -> bool {
    matches!(e, tapo::Error::Tapo(TapoResponseError::Unauthorized { .. }))
}
