use std::net::TcpListener;

use actix_web::{
    App, HttpServer,
    dev::{Server, ServerHandle},
    web,
};
use anyhow::Context;
use tracing_actix_web::TracingLogger;

use crate::{settings::Tapo, system::api::handlers};

pub struct WebServer {
    port: u16,
    server: Server,
}

impl WebServer {
    pub async fn new(host: &str, port: u16, tapo: Tapo) -> Result<Self, anyhow::Error> {
        let address = format!("{host}:{port}",);

        let listener = TcpListener::bind(address)?;
        let port = listener.local_addr()?.port();

        let data = web::Data::new(tapo);

        let server = HttpServer::new(move || {
            App::new()
                .wrap(TracingLogger::default())
                .app_data(data.clone())
                .route("/health-check", web::get().to(handlers::health_check))
                .route("/device", web::get().to(handlers::get_device))
                .route("/device", web::post().to(handlers::set_device))
        })
        // The process-level shutdown (main.rs) stops the server via its handle.
        .disable_signals()
        .shutdown_timeout(5)
        .listen(listener)
        .context("failed to listen to the API socket")?
        .run();

        Ok(Self { port, server })
    }

    pub async fn run_until_stopped(self) -> Result<(), anyhow::Error> {
        Ok(self.server.await?)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn handle(&self) -> ServerHandle {
        self.server.handle()
    }
}
