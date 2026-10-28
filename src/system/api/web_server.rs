use std::io;
use std::net::TcpListener;

use actix::Recipient;
use actix_web::{
    App, HttpServer,
    dev::{Server, ServerHandle},
    web,
};
use tracing_actix_web::TracingLogger;

use crate::system::{api::handlers, devices::DeviceSessions, messages::ProbeMessage};

pub struct WebServer {
    port: u16,
    server: Server,
}

impl WebServer {
    /// `probe_recipient`, the coordinator in production, answers the health check's probes.
    pub async fn new(
        host: &str,
        port: u16,
        device_sessions: DeviceSessions,
        probe_recipient: Recipient<ProbeMessage>,
    ) -> io::Result<Self> {
        let address = format!("{host}:{port}",);

        let listener = TcpListener::bind(address)?;
        let port = listener.local_addr()?.port();

        let device_sessions = web::Data::new(device_sessions);
        let probe_recipient = web::Data::new(probe_recipient);

        let server = HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::default())
                .app_data(device_sessions.clone())
                .app_data(probe_recipient.clone())
                .route("/health-check", web::get().to(handlers::health_check))
                .service(
                    web::resource("/device")
                        .route(web::get().to(handlers::get_device))
                        .route(web::post().to(handlers::set_device)),
                )
        })
        // The process-level shutdown (main.rs) stops the server via its handle.
        .disable_signals()
        .shutdown_timeout(5)
        .listen(listener)?
        .run();

        Ok(Self { port, server })
    }

    pub async fn run_until_stopped(self) -> io::Result<()> {
        self.server.await
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn handle(&self) -> ServerHandle {
        self.server.handle()
    }
}
