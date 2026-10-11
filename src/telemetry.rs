use std::error::Error;
use std::time::Duration;

use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use opentelemetry_semantic_conventions as semconv;
use tracing::{Level, Span};
use tracing_subscriber::Registry;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _};

pub fn record_error(span: &Span, e: &impl Error) {
    span.record(semconv::attribute::OTEL_STATUS_CODE, "ERROR");
    span.record(
        semconv::attribute::EXCEPTION_TYPE,
        "SendError<DeviceUsageMessage>",
    );
    span.record(semconv::attribute::EXCEPTION_MESSAGE, e.to_string());
    span.record(semconv::attribute::EXCEPTION_STACKTRACE, format!("{e:?}"));
}

pub fn init_telemetry() -> Result<Option<SdkTracerProvider>, Box<dyn Error>> {
    let filter_layer = EnvFilter::try_from_default_env()
        .unwrap_or(EnvFilter::new(Level::INFO.to_string()))
        // .add_directive("reqwest=off".parse()?)
        ;

    let format_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_line_number(true)
        // .with_span_events(FmtSpan::ACTIVE)
        ;

    let otlp_endpoint_configured = [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    .iter()
    .any(|var| std::env::var(var).is_ok_and(|value| !value.is_empty()));

    let (tracing_layer, tracer_provider) = if otlp_endpoint_configured {
        global::set_text_map_propagator(TraceContextPropagator::new());

        // HTTP rather than gRPC: the batch processor exports from its own thread, and the
        // blocking HTTP client doesn't depend on the actix runtime, which `shutdown` blocks.
        let exporter = SpanExporter::builder().with_http().build()?;

        let provider = SdkTracerProvider::builder()
            .with_resource(get_resource())
            .with_batch_exporter(exporter)
            .build();

        let tracer = provider.tracer(env!("CARGO_PKG_NAME"));

        (
            Some(tracing_opentelemetry::layer().with_tracer(tracer)),
            Some(provider),
        )
    } else {
        (None, None)
    };

    let subscriber = Registry::default()
        .with(filter_layer)
        .with(tracing_layer)
        .with(format_layer);

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(tracer_provider)
}

pub fn shutdown_telemetry(
    tracer_provider: SdkTracerProvider,
    timeout: Duration,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Collect all shutdown errors
    let mut shutdown_errors = Vec::new();

    if let Err(e) = tracer_provider.shutdown_with_timeout(timeout) {
        shutdown_errors.push(format!("tracer provider: {e}"));
    }

    // Return an error if any shutdown failed
    if !shutdown_errors.is_empty() {
        return Err(format!(
            "Failed to shutdown providers:{}",
            shutdown_errors.join("\n")
        )
        .into());
    }

    Ok(())
}

fn get_resource() -> Resource {
    let builder = Resource::builder().with_attribute(KeyValue::new(
        semconv::attribute::SERVICE_VERSION,
        env!("CARGO_PKG_VERSION"),
    ));

    if service_name_from_env() {
        builder.build()
    } else {
        builder.with_service_name(env!("CARGO_PKG_NAME")).build()
    }
}

/// Mirrors the SDK's env detectors, which ignore empty values.
fn service_name_from_env() -> bool {
    let non_empty = |v: &str| !v.trim().is_empty();

    std::env::var("OTEL_SERVICE_NAME").is_ok_and(|v| non_empty(&v))
        || std::env::var("OTEL_RESOURCE_ATTRIBUTES").is_ok_and(|attrs| {
            attrs.split(',').any(|pair| {
                pair.split_once('=').is_some_and(|(k, v)| {
                    k.trim() == semconv::resource::SERVICE_NAME && non_empty(v)
                })
            })
        })
}
