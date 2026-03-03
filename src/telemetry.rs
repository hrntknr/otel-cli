use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider,
};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::store::SharedStore;

pub struct TelemetryGuard {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
}

pub fn init(otlp_endpoint: Option<&str>) -> Option<TelemetryGuard> {
    let endpoint = otlp_endpoint?;
    let resource = resource();

    // Traces
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("failed to create OTLP span exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    let tracer = tracer_provider.tracer("otel-cli");
    let trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    // Logs
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("failed to create OTLP log exporter");

    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource.clone())
        .build();

    let log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

    // Metrics
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .expect("failed to create OTLP metric exporter");

    let metric_reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
        .with_interval(std::time::Duration::from_secs(15))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(metric_reader)
        .with_resource(resource)
        .build();

    let metrics_layer = tracing_opentelemetry::MetricsLayer::new(meter_provider.clone());

    let filter = Targets::new().with_target("otel_cli", tracing::Level::TRACE);

    tracing_subscriber::registry()
        .with(filter)
        .with(trace_layer)
        .with(metrics_layer)
        .with(log_layer)
        .init();

    Some(TelemetryGuard {
        tracer_provider,
        logger_provider,
        meter_provider,
    })
}

fn resource() -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder_empty()
        .with_service_name("otel-cli")
        .build()
}

/// Register observable gauges for store metrics.
/// Returns gauge handles that must be kept alive for callbacks to fire.
pub fn register_store_metrics(
    guard: &TelemetryGuard,
    store: SharedStore,
) -> Vec<opentelemetry::metrics::ObservableGauge<u64>> {
    use opentelemetry::metrics::MeterProvider;

    let meter = guard.meter_provider.meter("otel-cli");

    let store_traces = store.clone();
    let trace_gauge = meter
        .u64_observable_gauge("otel_cli.store.trace_count")
        .with_description("Number of trace groups in the store")
        .with_callback(
            move |observer: &dyn opentelemetry::metrics::AsyncInstrument<u64>| {
                if let Ok(s) = store_traces.try_read() {
                    observer.observe(s.trace_count() as u64, &[]);
                }
            },
        )
        .build();

    let store_logs = store.clone();
    let log_gauge = meter
        .u64_observable_gauge("otel_cli.store.log_count")
        .with_description("Number of log entries in the store")
        .with_callback(
            move |observer: &dyn opentelemetry::metrics::AsyncInstrument<u64>| {
                if let Ok(s) = store_logs.try_read() {
                    observer.observe(s.log_count() as u64, &[]);
                }
            },
        )
        .build();

    let store_metrics = store;
    let metric_gauge = meter
        .u64_observable_gauge("otel_cli.store.metric_count")
        .with_description("Number of metric entries in the store")
        .with_callback(
            move |observer: &dyn opentelemetry::metrics::AsyncInstrument<u64>| {
                if let Ok(s) = store_metrics.try_read() {
                    observer.observe(s.metric_count() as u64, &[]);
                }
            },
        )
        .build();

    vec![trace_gauge, log_gauge, metric_gauge]
}

pub fn shutdown(guard: Option<TelemetryGuard>) {
    if let Some(guard) = guard {
        if let Err(e) = guard.tracer_provider.shutdown() {
            eprintln!("OpenTelemetry tracer shutdown error: {}", e);
        }
        if let Err(e) = guard.logger_provider.shutdown() {
            eprintln!("OpenTelemetry logger shutdown error: {}", e);
        }
        if let Err(e) = guard.meter_provider.shutdown() {
            eprintln!("OpenTelemetry meter shutdown error: {}", e);
        }
    }
}
