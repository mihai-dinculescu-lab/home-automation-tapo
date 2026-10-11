use actix::{
    Actor, ActorContext, ActorFutureExt, AsyncContext, Context, Handler, ResponseActFuture,
    WrapFuture,
};
use actix_web::dev::ServerHandle;
use tracing::debug;

use crate::settings::{Api, Tapo};
use crate::system::api::web_server::WebServer;
use crate::system::messages::StopMessage;

#[derive(Debug)]
pub struct ApiActor {
    config_api: Api,
    config_tapo: Tapo,
    server_handle: Option<ServerHandle>,
}

impl ApiActor {
    pub fn new(config_api: Api, config_tapo: Tapo) -> Self {
        Self {
            config_api,
            config_tapo,
            server_handle: None,
        }
    }
}

impl Actor for ApiActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("Api Actor started...");

        let host = self.config_api.host.clone();
        let port = self.config_api.port;

        let tapo = self.config_tapo.clone();

        // Only block the mailbox while the server is being created; it then runs in the
        // background so that StopMessage can still be handled.
        let fut = async move {
            WebServer::new(&host, port, tapo)
                .await
                .expect("failed to create the API")
        }
        .into_actor(self)
        .map(|web_server, act, ctx| {
            act.server_handle = Some(web_server.handle());

            ctx.spawn(
                async move {
                    web_server
                        .run_until_stopped()
                        .await
                        .expect("failed to start the API");
                }
                .into_actor(act),
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
