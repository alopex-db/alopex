use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataMap;

/// Authentication mode for the server.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthMode {
    /// No authentication.
    #[default]
    None,
    /// Dev API key authentication.
    Dev { api_key: String },
}

/// Authentication error for HTTP/gRPC.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing credentials")]
    Missing,
    #[error("invalid credentials")]
    Invalid,
}

/// Middleware helper for authentication.
#[derive(Clone)]
pub struct AuthMiddleware {
    mode: AuthMode,
}

impl AuthMiddleware {
    /// Create a new auth middleware.
    pub fn new(mode: AuthMode) -> Self {
        Self { mode }
    }

    /// Validate HTTP headers and return actor identity (if any).
    pub fn validate_http(&self, headers: &HeaderMap) -> Result<Option<String>, AuthError> {
        match &self.mode {
            AuthMode::None => Ok(None),
            AuthMode::Dev { api_key } => {
                let provided = extract_api_key(headers);
                if provided.as_deref() == Some(api_key.as_str()) {
                    Ok(Some("dev".to_string()))
                } else if provided.is_none() {
                    Err(AuthError::Missing)
                } else {
                    Err(AuthError::Invalid)
                }
            }
        }
    }

    /// Validate gRPC metadata and return actor identity (if any).
    pub fn validate_grpc(&self, metadata: &MetadataMap) -> Result<Option<String>, AuthError> {
        match &self.mode {
            AuthMode::None => Ok(None),
            AuthMode::Dev { api_key } => {
                let provided = extract_api_key_from_metadata(metadata);
                if provided.as_deref() == Some(api_key.as_str()) {
                    Ok(Some("dev".to_string()))
                } else if provided.is_none() {
                    Err(AuthError::Missing)
                } else {
                    Err(AuthError::Invalid)
                }
            }
        }
    }
}

fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(value.to_string());
    }
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.strip_prefix("Bearer "))
        .map(|v| v.to_string())
}

fn extract_api_key_from_metadata(metadata: &MetadataMap) -> Option<String> {
    if let Some(value) = metadata.get("x-api-key").and_then(|v| v.to_str().ok()) {
        return Some(value.to_string());
    }
    metadata
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.strip_prefix("Bearer "))
        .map(|v| v.to_string())
}
