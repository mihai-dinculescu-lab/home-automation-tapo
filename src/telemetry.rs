use std::error::Error;
use std::time::Duration;

use opentelemetry::trace::TracerProvider;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use opentelemetry_semantic_conventions as semconv;
use tracing::{Level, Span};
use tracing_opentelemetry::{OpenTelemetrySpanExt as _, SetParentError};
use tracing_subscriber::Registry;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _};

/// Parents `span` to the OpenTelemetry context carried by a message.
///
/// Must be called before the span is entered: entering builds the OpenTelemetry span, after
/// which its parent can no longer be changed and `set_parent` fails with `AlreadyStarted`.
pub fn set_parent_context(span: &Span, cx: opentelemetry::Context) {
    match span.set_parent(cx) {
        // Without an OTLP endpoint the OpenTelemetry layer isn't installed, and a span filtered
        // out by the log level has nothing to link; neither is an error.
        Ok(()) | Err(SetParentError::LayerNotFound | SetParentError::SpanDisabled) => {}
        Err(e) => record_error!(parent: span, &e),
    }
}

/// Reports the error behind `$e` (a `&E` where `E: Error`) once, as an `error!` event in the
/// current span, or in `parent` if given.
///
/// The fmt layer prints the event to stdout. The OpenTelemetry layer sets the span status to
/// ERROR with the error text as description, and turns the event into an `exception` span event
/// carrying `exception.message` (the error text), `exception.type` (the Rust type of the error)
/// and `exception.stacktrace` (its `Debug` representation, which includes the causes).
// Two arms rather than an optional `parent:` prefix: `parent` can also start an expression, so a
// single arm is ambiguous to `macro_rules!`.
macro_rules! record_error {
    (parent: $span:expr, $e:expr) => {{
        let e = $e;
        ::tracing::error!(
            parent: $span,
            error = %e,
            exception.type = $crate::telemetry::error_type(e),
            exception.stacktrace = ?e,
        );
    }};
    ($e:expr) => {{
        let e = $e;
        ::tracing::error!(
            error = %e,
            exception.type = $crate::telemetry::error_type(e),
            exception.stacktrace = ?e,
        );
    }};
}
pub(crate) use record_error;

#[doc(hidden)]
pub(crate) fn error_type<E: Error>(_: &E) -> &'static str {
    std::any::type_name::<E>()
}

/// Span for a message exchanged between actors. Internal to the `producer_span!` and
/// `consumer_span!` helpers.
///
/// Any trailing `$fields` are passed through to `info_span!` as extra span fields.
/// The span name has to be a literal, which is why these helpers are macros.
#[doc(hidden)]
macro_rules! message_span {
    (
        name = $name:literal,
        kind = $kind:literal,
        operation = ($operation_name:literal, $operation_type:literal),
        message = $message:ident,
        destination = $destination:literal
        $(, $($fields:tt)*)?
    ) => {
        ::tracing::info_span!(
            $name,
            otel.kind = $kind,
            messaging.system = "actix",
            actor.message.type = stringify!($message),
            messaging.operation.name = $operation_name,
            messaging.operation.type = $operation_type,
            messaging.destination.name = $destination,
            $($($fields)*)?
        )
    };
}
pub(crate) use message_span;

/// Span for sending `message` to the `destination` actor. Enter it, then put its `context()` in
/// the message so that the receiving handler can parent its span to it.
///
/// Any trailing `$fields` are passed through to `info_span!` as extra span fields.
macro_rules! producer_span {
    (
        name = $name:literal,
        message = $message:ident,
        destination = $destination:literal
        $(, $($fields:tt)*)?
    ) => {
        $crate::telemetry::message_span!(
            name = $name,
            kind = "producer",
            operation = ("send", "send"),
            message = $message,
            destination = $destination
            $(, $($fields)*)?
        )
    };
}
pub(crate) use producer_span;

/// Span for handling `message` in the `destination` actor, parented to the context carried by
/// the message. Must be created before it is entered (see `set_parent_context`).
///
/// Any trailing `$fields` are passed through to `info_span!` as extra span fields.
macro_rules! consumer_span {
    (
        name = $name:literal,
        message = $message:ident,
        parent = $parent:expr,
        destination = $destination:literal
        $(, $($fields:tt)*)?
    ) => {{
        let span = $crate::telemetry::message_span!(
            name = $name,
            kind = "consumer",
            operation = ("handle", "process"),
            message = $message,
            destination = $destination
            $(, $($fields)*)?
        );
        $crate::telemetry::set_parent_context(&span, $parent);
        span
    }};
}
pub(crate) use consumer_span;

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
