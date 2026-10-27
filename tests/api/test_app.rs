use actix::{Actor, ActorContext, Addr, Context, Handler, ResponseFuture};
use home_automation_tapo::{
    settings::Tapo,
    system::{ProbeMessage, StopMessage, api::web_server::WebServer, devices::Devices},
};

/// Stands in for the coordinator behind `/health-check`: it answers every probe, or none.
pub struct CoordinatorStub {
    pub answers: bool,
}

impl Actor for CoordinatorStub {
    type Context = Context<Self>;
}

impl Handler<ProbeMessage> for CoordinatorStub {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _: ProbeMessage, _: &mut Context<Self>) -> Self::Result {
        if self.answers {
            Box::pin(async {})
        } else {
            Box::pin(std::future::pending())
        }
    }
}

impl Handler<StopMessage> for CoordinatorStub {
    type Result = ();

    fn handle(&mut self, _: StopMessage, ctx: &mut Context<Self>) -> Self::Result {
        ctx.stop();
    }
}

/// Devices that are never logged in to; no test talks to a device.
pub fn devices() -> Devices {
    Devices::new(Tapo {
        username: "".to_string(),
        password: "".to_string(),
        refresh_rate_s: 60,
    })
}

pub struct TestApp {
    pub address: String,
    pub coordinator: Addr<CoordinatorStub>,
}

impl TestApp {
    /// An app whose coordinator answers every probe.
    pub async fn new() -> Self {
        Self::with_coordinator(CoordinatorStub { answers: true }).await
    }

    pub async fn with_coordinator(coordinator: CoordinatorStub) -> Self {
        let coordinator = coordinator.start();

        let web_server = WebServer::new("localhost", 0, devices(), coordinator.clone().recipient())
            .await
            .expect("Failed to build API");

        let port = web_server.port();
        tokio::spawn(web_server.run_until_stopped());

        Self {
            address: format!("http://localhost:{port}"),
            coordinator,
        }
    }
}
