use std::time::Duration;

use home_automation_tapo::system::StopMessage;
use home_automation_tapo::system::api::handlers::ApiStatusResponse;

use crate::api::test_app::{CoordinatorStub, TestApp};

#[actix_rt::test]
async fn health_check_works() {
    // Arrange
    let app = TestApp::new().await;
    let client = reqwest::Client::new();

    // Act
    let response = client
        .get(format!("{}/health-check", app.address))
        .send()
        .await
        .expect("Failed to execute request.");

    // Assert
    assert!(response.status().is_success());

    let json: ApiStatusResponse = response.json().await.expect("Failed to parse the response");
    assert_eq!(
        json,
        ApiStatusResponse {
            code: 200,
            message: "OK".to_string(),
        }
    );
}

#[actix_rt::test]
async fn health_check_fails_when_the_coordinator_has_stopped() {
    // Arrange
    let app = TestApp::new().await;

    app.coordinator
        .send(StopMessage)
        .await
        .expect("Failed to stop the coordinator stub");
    tokio::time::timeout(Duration::from_secs(1), async {
        while app.coordinator.connected() {
            actix::clock::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("The coordinator stub did not stop");

    // Act & Assert
    assert_unavailable(&app).await;
}

#[actix_rt::test]
async fn health_check_fails_when_the_coordinator_does_not_answer() {
    // Arrange
    let app = TestApp::with_coordinator(CoordinatorStub { answers: false }).await;

    // Act & Assert
    assert_unavailable(&app).await;
}

/// Asserts that the health check reports the coordinator as unavailable.
async fn assert_unavailable(app: &TestApp) {
    let response = reqwest::Client::new()
        .get(format!("{}/health-check", app.address))
        .send()
        .await
        .expect("Failed to execute request.");

    assert_eq!(response.status(), 503);

    let json: ApiStatusResponse = response.json().await.expect("Failed to parse the response");
    assert_eq!(
        json,
        ApiStatusResponse {
            code: 503,
            message: "The coordinator did not answer".to_string(),
        }
    );
}
