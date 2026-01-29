use axum::http::StatusCode;

/// Server-wide result type.
pub type Result<T> = std::result::Result<T, ServerError>;

/// Top-level error type for alopex-server.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("payload too large: {0}")]
    PayloadTooLarge(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("session expired: {0}")]
    SessionExpired(String),
    #[error("sql error: {0}")]
    Sql(#[from] alopex_sql::SqlError),
    #[error("core error: {0}")]
    Core(#[from] alopex_core::Error),
    #[error("catalog error: {0}")]
    Catalog(#[from] alopex_sql::catalog::CatalogError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("internal error: {0}")]
    Internal(String),
}

impl ServerError {
    /// Map error to HTTP status code.
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::InvalidConfig(_) | Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Sql(err) if err.code() == "ALOPEX-S001" => StatusCode::CONFLICT,
            Self::Sql(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Timeout(_) => StatusCode::REQUEST_TIMEOUT,
            Self::SessionExpired(_) => StatusCode::GONE,
            Self::Core(_) | Self::Catalog(_) | Self::Io(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    /// Map error to a stable error code for clients.
    pub fn error_code(&self) -> String {
        match self {
            Self::Sql(err) => err.code().to_string(),
            Self::InvalidConfig(_) => "INVALID_CONFIG".to_string(),
            Self::BadRequest(_) => "INVALID_REQUEST".to_string(),
            Self::Unauthorized(_) => "UNAUTHORIZED".to_string(),
            Self::NotFound(_) => "NOT_FOUND".to_string(),
            Self::Conflict(_) => "CONFLICT".to_string(),
            Self::PayloadTooLarge(_) => "PAYLOAD_TOO_LARGE".to_string(),
            Self::Timeout(_) => "QUERY_TIMEOUT".to_string(),
            Self::SessionExpired(_) => "SESSION_EXPIRED".to_string(),
            Self::Core(_) | Self::Catalog(_) | Self::Io(_) | Self::Internal(_) => {
                "INTERNAL".to_string()
            }
        }
    }
}
