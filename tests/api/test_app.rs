use actix::{Actor, ActorContext, Addr, Context, Handler, ResponseFuture};
use home_automation_tapo::system::{
    GetDeviceSessionMessage, ProbeMessage, StopMessage,
    api::web_server::WebServer,
    devices::{DeviceSessions, device_session_actor::DeviceSessionActor},
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

impl Handler<GetDeviceSessionMessage> for CoordinatorStub {
    type Result = Option<Addr<DeviceSessionActor>>;

    /// No test talks to a device.
    fn handle(&mut self, _: GetDeviceSessionMessage, _: &mut Context<Self>) -> Self::Result {
        None
    }
}

/// Device sessions that `coordinator_addr` hands out.
pub fn device_sessions(coordinator_addr: &Addr<CoordinatorStub>) -> DeviceSessions {
    DeviceSessions::new(coordinator_addr.clone().recipient())
}

pub struct TestApp {
    pub address: String,
    pub coordinator_addr: Addr<CoordinatorStub>,
}

impl TestApp {
    /// An app whose coordinator answers every probe.
    pub async fn new() -> Self {
        Self::with_coordinator(CoordinatorStub { answers: true }).await
    }

    pub async fn with_coordinator(coordinator: CoordinatorStub) -> Self {
        let coordinator_addr = coordinator.start();

        let web_server = WebServer::new(
            "localhost",
            0,
            device_sessions(&coordinator_addr),
            coordinator_addr.clone().recipient(),
        )
        .await
        .expect("Failed to build API");

        let port = web_server.port();
        tokio::spawn(web_server.run_until_stopped());

        Self {
            address: format!("http://localhost:{port}"),
            coordinator_addr,
        }
    }
}
