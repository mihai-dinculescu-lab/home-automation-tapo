use std::time::Duration;

use tracing::{info, warn};

use home_automation_tapo::settings::Settings;
use home_automation_tapo::system::coordinator_actor::CoordinatorActor;
use home_automation_tapo::telemetry::{init_telemetry, shutdown_telemetry};

// Together these stay well within Kubernetes' default 30s termination grace period. The actors
// only have to drain what is in flight: an MQTT publish or the API's graceful stop, a few seconds
// each.
const ACTORS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TELEMETRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[actix_rt::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Registered before anything else so that a signal received during startup isn't lost.
    let shutdown_signal = shutdown_signal();

    let settings = Settings::new().expect("Failed to read the settings");

    let tracer_provider = init_telemetry()?;

    info!("Starting home automation tapo system with Actix-RT on Tokio runtime");

    // Start coordinator actor
    let coordinator =
        CoordinatorActor::spawn(settings).expect("Failed to start the CoordinatorActor");

    info!("System started, waiting for shutdown signal...");

    shutdown_signal.await;

    info!("Received shutdown signal, shutting down application...");

    // Stop the actors first so that no spans are created while telemetry is being flushed.
    if tokio::time::timeout(ACTORS_SHUTDOWN_TIMEOUT, CoordinatorActor::stop(coordinator))
        .await
        .is_err()
    {
        warn!("Timed out waiting for the actors to stop");
    }

    // Shutdown telemetry. Failing to flush only loses spans; the application itself shut down
    // cleanly, so don't report it with a non-zero exit code.
    if let Some(tracer_provider) = tracer_provider {
        match tokio::task::spawn_blocking(move || {
            shutdown_telemetry(tracer_provider, TELEMETRY_SHUTDOWN_TIMEOUT)
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to shut down telemetry: {e}"),
            Err(e) => warn!("Telemetry shutdown task failed: {e}"),
        }
    }

    Ok(())
}

/// Kubernetes stops pods with SIGTERM, not SIGINT.
///
/// The listeners are registered when this is called, not when the returned future is first polled.
fn shutdown_signal() -> impl Future<Output = ()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to listen for SIGINT");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");

        async move {
            tokio::select! {
                _ = sigint.recv() => {}
                _ = sigterm.recv() => {}
            }
        }
    }

    #[cfg(not(unix))]
    {
        let mut ctrl_c = tokio::signal::windows::ctrl_c().expect("Failed to listen for ctrl+c");

        async move {
            ctrl_c.recv().await;
        }
    }
}
