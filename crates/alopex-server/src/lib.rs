//! Alopex server implementation (HTTP/gRPC).

pub mod audit;
pub mod auth;
mod catalog_rollback;
pub mod config;
pub mod error;
pub mod grpc;
pub mod http;
pub mod metrics;
pub mod ops;
pub mod server;
pub mod session;
pub mod tls;

pub use config::ServerConfig;
pub use error::{Result, ServerError};
pub use server::Server;
