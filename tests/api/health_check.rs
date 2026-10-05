use home_automation_tapo::system::api::handlers::ApiStatusResponse;

use crate::api::test_app::TestApp;

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
