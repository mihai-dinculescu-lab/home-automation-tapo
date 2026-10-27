use std::io;

use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, Recipient,
    ResponseActFuture, WrapFuture,
};
use actix_web::dev::ServerHandle;
use tracing::{Instrument, debug, instrument};

use crate::settings::Api;
use crate::system::api::web_server::WebServer;
use crate::system::devices::Devices;
use crate::system::messages::{ProbeMessage, StopMessage};
use crate::telemetry::record_error;

#[derive(Debug)]
pub struct ApiActor {
    config_api: Api,
    devices: Devices,
    /// Probed by the health check.
    coordinator: Recipient<ProbeMessage>,
    server_handle: Option<ServerHandle>,
}

impl ApiActor {
    #[instrument(name = "ApiActor::new", skip_all)]
    pub fn new(config_api: Api, devices: Devices, coordinator: Recipient<ProbeMessage>) -> Self {
        Self {
            config_api,
            devices,
            coordinator,
            server_handle: None,
        }
    }

    /// A stop that `StopMessage` asked for, and that went well, is the handler's to finish, so
    /// it isn't traced.
    fn server_exited(&mut self, result: io::Result<()>, ctx: &mut Context<Self>) {
        if self.server_handle.is_none() && result.is_ok() {
            return;
        }

        self.server_stopped(result, ctx);
    }

    /// The server has stopped on its own or failed. Unless a `StopMessage` asked for the stop,
    /// the actor stops with it, so that the coordinator restarts an API that is no longer
    /// serving; otherwise the handler stops it once it has replied.
    #[instrument(name = "ApiActor::server_stopped", skip_all)]
    fn server_stopped(&mut self, result: io::Result<()>, ctx: &mut Context<Self>) {
        if let Err(e) = result {
            record_error!(&e);
        }

        if self.server_handle.is_some() {
            ctx.stop();
        }
    }
}

impl Actor for ApiActor {
    type Context = Context<Self>;

    #[instrument(name = "ApiActor::started", skip_all, fields(
        server.address = %self.config_api.host,
        server.port = self.config_api.port,
    ))]
    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("Api Actor started...");

        let host = self.config_api.host.clone();
        let port = self.config_api.port;

        let devices = self.devices.clone();
        let coordinator = self.coordinator.clone();

        // Only block the mailbox while the server is being created; it then runs in the
        // background so that StopMessage can still be handled. A server that can't be created
        // stops the actor, and the coordinator retries on its next health check.
        let fut = async move {
            WebServer::new(&host, port, devices, coordinator)
                .await
                .inspect_err(|e| record_error!(e))
                .ok()
        }
        .instrument(tracing::Span::current())
        .into_actor(self)
        .map(|web_server, act, ctx| {
            let Some(web_server) = web_server else {
                ctx.stop();
                return;
            };

            act.server_handle = Some(web_server.handle());

            ctx.spawn(
                web_server
                    .run_until_stopped()
                    .into_actor(act)
                    .map(|result, act, ctx| act.server_exited(result, ctx)),
            );
        });

        ctx.wait(fut);
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        debug!("Api Actor stopped.");
    }
}

impl Handler<StopMessage> for ApiActor {
    type Result = ResponseActFuture<Self, ()>;

    /// Stops the actor once the server has stopped gracefully. Taking the handle tells the
    /// server's own completion to leave the stop to this handler, so that the reply is sent.
    fn handle(&mut self, _: StopMessage, _: &mut Context<Self>) -> Self::Result {
        let server_handle = self.server_handle.take();

        Box::pin(
            async move {
                if let Some(server_handle) = server_handle {
                    server_handle.stop(true).await;
                }
            }
            .into_actor(self)
            .map(|_, _, ctx| ctx.stop()),
        )
    }
}
