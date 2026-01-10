use std::time::Duration;

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};

use crate::error::{Result, ServerError};

/// Prometheus metrics registry.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    query_count: IntCounter,
    query_latency: Histogram,
    active_connections: IntGauge,
    stream_backpressure: IntCounter,
    error_count: IntCounterVec,
}

impl Metrics {
    /// Create a new metrics registry.
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let query_count = IntCounter::with_opts(Opts::new("query_count", "Total queries"))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        let query_latency = Histogram::with_opts(HistogramOpts::new(
            "query_latency_seconds",
            "Query latency in seconds",
        ))
        .map_err(|err| ServerError::Internal(err.to_string()))?;
        let active_connections =
            IntGauge::with_opts(Opts::new("active_connections", "Active connections"))
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        let stream_backpressure =
            IntCounter::with_opts(Opts::new("stream_backpressure", "Backpressure events"))
                .map_err(|err| ServerError::Internal(err.to_string()))?;
        let error_count = IntCounterVec::new(Opts::new("error_count", "Error count"), &["kind"])
            .map_err(|err| ServerError::Internal(err.to_string()))?;

        registry
            .register(Box::new(query_count.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(query_latency.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(active_connections.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(stream_backpressure.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        registry
            .register(Box::new(error_count.clone()))
            .map_err(|err| ServerError::Internal(err.to_string()))?;

        Ok(Self {
            registry,
            query_count,
            query_latency,
            active_connections,
            stream_backpressure,
            error_count,
        })
    }

    /// Record query completion.
    pub fn record_query(&self, duration: Duration, success: bool) {
        self.query_count.inc();
        self.query_latency.observe(duration.as_secs_f64());
        if !success {
            self.error_count.with_label_values(&["query"]).inc();
        }
    }

    /// Track connection count delta.
    pub fn record_connection(&self, delta: i64) {
        if delta >= 0 {
            self.active_connections.add(delta);
        } else {
            self.active_connections.sub(-delta);
        }
    }

    /// Track a backpressure event.
    pub fn record_backpressure(&self) {
        self.stream_backpressure.inc();
    }

    /// Record a generic error.
    pub fn record_error(&self, kind: &str) {
        self.error_count.with_label_values(&[kind]).inc();
    }

    /// Render metrics in Prometheus text format.
    pub fn expose_prometheus(&self) -> Result<String> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        String::from_utf8(buffer)
            .map_err(|err| ServerError::Internal(format!("invalid metrics utf8: {err}")))
    }
}
