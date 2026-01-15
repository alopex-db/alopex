use std::time::Duration;

use reqwest::{Client, Method, RequestBuilder, Response, Url};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::time::sleep;

use crate::client::auth::{AuthConfig, AuthError};
use crate::profile::config::ServerConfig;

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("invalid server url: {0}")]
    InvalidUrl(String),
    #[error("failed to build HTTP client: {0}")]
    Build(String),
    #[error("authentication error: {0}")]
    Auth(#[from] AuthError),
    #[error("request failed after {retries} retries: {source}")]
    Request {
        retries: usize,
        #[source]
        source: reqwest::Error,
    },
    #[error("unexpected response status {status}: {body}")]
    HttpStatus {
        status: reqwest::StatusCode,
        body: String,
    },
}

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Debug, Clone)]
struct RetryPolicy {
    delays: Vec<Duration>,
}

impl RetryPolicy {
    fn default() -> Self {
        Self {
            delays: vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
            ],
        }
    }

    fn attempts(&self) -> usize {
        self.delays.len() + 1
    }
}

pub struct HttpClient {
    base_url: Url,
    auth: AuthConfig,
    client: Client,
    retry_policy: RetryPolicy,
}

impl HttpClient {
    pub fn new(config: &ServerConfig) -> ClientResult<Self> {
        let base_url =
            Url::parse(&config.url).map_err(|err| ClientError::InvalidUrl(err.to_string()))?;
        if base_url.scheme() != "https" {
            return Err(ClientError::InvalidUrl(
                "server url must use https scheme".to_string(),
            ));
        }
        let auth = AuthConfig::from_server_config(config)?;
        let builder = Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(8);
        let builder = auth.apply_to_builder(builder)?;
        let client = builder
            .build()
            .map_err(|err| ClientError::Build(err.to_string()))?;

        Ok(Self {
            base_url,
            auth,
            client,
            retry_policy: RetryPolicy::default(),
        })
    }

    fn request(&self, method: Method, path: &str) -> ClientResult<RequestBuilder> {
        let url = self
            .base_url
            .join(path)
            .map_err(|err| ClientError::InvalidUrl(err.to_string()))?;
        let request = self.client.request(method, url);
        Ok(self.auth.apply_to_request(request)?)
    }

    async fn send_with_retry<F>(&self, mut build: F) -> ClientResult<Response>
    where
        F: FnMut() -> ClientResult<RequestBuilder>,
    {
        let mut last_err: Option<reqwest::Error> = None;
        for (attempt, delay) in self.retry_policy.delays.iter().enumerate() {
            match build()?.send().await {
                Ok(response) => return Ok(response),
                Err(err) => {
                    last_err = Some(err);
                    sleep(*delay).await;
                    tracing::warn!(
                        attempt = attempt + 1,
                        "HTTP request failed, retrying after {:?}",
                        delay
                    );
                }
            }
        }
        match build()?.send().await {
            Ok(response) => Ok(response),
            Err(err) => Err(ClientError::Request {
                retries: self.retry_policy.attempts(),
                source: last_err.unwrap_or(err),
            }),
        }
    }

    async fn send_and_check<F>(&self, build: F) -> ClientResult<Response>
    where
        F: FnMut() -> ClientResult<RequestBuilder>,
    {
        let response = self.send_with_retry(build).await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(ClientError::HttpStatus { status, body })
        }
    }

    #[allow(dead_code)]
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> ClientResult<T> {
        let response = self
            .send_and_check(|| self.request(Method::GET, path))
            .await?;
        response
            .json::<T>()
            .await
            .map_err(|err| ClientError::Request {
                retries: 0,
                source: err,
            })
    }

    #[allow(dead_code)]
    pub async fn get_text(&self, path: &str) -> ClientResult<String> {
        let response = self
            .send_and_check(|| self.request(Method::GET, path))
            .await?;
        response.text().await.map_err(|err| ClientError::Request {
            retries: 0,
            source: err,
        })
    }

    pub async fn post_json<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> ClientResult<T> {
        let response = self
            .send_and_check(|| self.request(Method::POST, path).map(|req| req.json(body)))
            .await?;
        response
            .json::<T>()
            .await
            .map_err(|err| ClientError::Request {
                retries: 0,
                source: err,
            })
    }

    pub async fn post_json_stream<B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> ClientResult<Response> {
        self.send_and_check(|| self.request(Method::POST, path).map(|req| req.json(body)))
            .await
    }
}
