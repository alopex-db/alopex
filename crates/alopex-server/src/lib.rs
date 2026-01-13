//! Alopex server implementation (HTTP/gRPC).

pub mod audit;
pub mod auth;
pub mod config;
pub mod error;
pub mod grpc;
pub mod http;
pub mod metrics;
pub mod server;
pub mod session;
pub mod tls;

pub use config::ServerConfig;
pub use error::{Result, ServerError};
pub use server::Server;
