use std::net::TcpListener;
use std::time::Duration;

use actix::{Actor, Addr};
use home_automation_tapo::settings::Api;
use home_automation_tapo::system::StopMessage;
use home_automation_tapo::system::api::api_actor::ApiActor;
use home_automation_tapo::system::coordinator_actor::wait_until_stopped;

use crate::api::test_app::{CoordinatorStub, device_sessions};

#[actix_rt::test]
async fn api_actor_stops_when_the_server_cannot_be_created() {
    // Arrange
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind a port");
    let port = local_port(&listener);

    // Act
    let api_actor_addr = start_api_actor(port);

    // Assert
    assert_stops(&api_actor_addr).await;
}

#[actix_rt::test]
async fn api_actor_serves_until_asked_to_stop() {
    // Arrange
    let port = local_port(&TcpListener::bind("127.0.0.1:0").expect("Failed to bind a port"));
    let api_actor_addr = start_api_actor(port);

    assert_serves(port).await;

    // Act
    api_actor_addr
        .send(StopMessage)
        .await
        .expect("The API actor stopped without replying");

    // Assert
    assert_stops(&api_actor_addr).await;
}

fn local_port(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .expect("Failed to read the bound port")
        .port()
}

fn start_api_actor(port: u16) -> Addr<ApiActor> {
    let coordinator_addr = CoordinatorStub { answers: true }.start();

    let config = Api {
        host: "127.0.0.1".to_string(),
        port,
    };

    ApiActor::new(
        config,
        device_sessions(&coordinator_addr),
        coordinator_addr.recipient(),
    )
    .start()
}

/// Asserts that the health check answers within a second.
async fn assert_serves(port: u16) {
    let client = reqwest::Client::new();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let response = client
                .get(format!("http://127.0.0.1:{port}/health-check"))
                .send()
                .await;

            if response.is_ok_and(|response| response.status().is_success()) {
                return;
            }

            actix::clock::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The API did not serve");
}

/// Asserts that the actor stops within a second.
async fn assert_stops(api_actor_addr: &Addr<ApiActor>) {
    tokio::time::timeout(Duration::from_secs(1), wait_until_stopped(api_actor_addr))
        .await
        .expect("The API actor did not stop");
}
