use std::collections::HashMap;
use std::env;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::error;

fn scheme_from_location(storage_location: &str) -> Option<&str> {
    storage_location
        .find("://")
        .map(|pos| &storage_location[..pos])
}

fn merge_if_missing(target: &mut HashMap<String, String>, key: &str, value: String) {
    if !target.contains_key(key) {
        target.insert(key.to_string(), value);
    }
}

fn auto_credentials(
    storage_location: &str,
    mut options: HashMap<String, String>,
) -> PyResult<HashMap<String, String>> {
    let scheme = scheme_from_location(storage_location);
    let Some(scheme) = scheme else {
        return Ok(options);
    };

    match scheme {
        "file" => Ok(options),
        "s3" => {
            let access_key = env::var("AWS_ACCESS_KEY_ID").ok();
            let secret_key = env::var("AWS_SECRET_ACCESS_KEY").ok();
            match (access_key, secret_key) {
                (Some(access_key), Some(secret_key)) => {
                    merge_if_missing(&mut options, "aws_access_key_id", access_key);
                    merge_if_missing(&mut options, "aws_secret_access_key", secret_key);
                    if let Ok(token) = env::var("AWS_SESSION_TOKEN") {
                        merge_if_missing(&mut options, "aws_session_token", token);
                    }
                    if let Ok(region) = env::var("AWS_REGION") {
                        merge_if_missing(&mut options, "aws_region", region);
                    }
                    Ok(options)
                }
                _ => Err(error::to_py_err("Credentials not found for s3://...")),
            }
        }
        "az" | "abfs" | "abfss" => {
            let account_name = env::var("AZURE_STORAGE_ACCOUNT_NAME").ok();
            let account_key = env::var("AZURE_STORAGE_ACCOUNT_KEY").ok();
            let sas_token = env::var("AZURE_STORAGE_SAS_TOKEN").ok();
            match (account_name, account_key, sas_token) {
                (Some(account_name), Some(account_key), _) => {
                    merge_if_missing(&mut options, "azure_storage_account_name", account_name);
                    merge_if_missing(&mut options, "azure_storage_account_key", account_key);
                    Ok(options)
                }
                (account_name, _, Some(sas_token)) => {
                    if let Some(account_name) = account_name {
                        merge_if_missing(
                            &mut options,
                            "azure_storage_account_name",
                            account_name,
                        );
                    }
                    merge_if_missing(&mut options, "azure_storage_sas_token", sas_token);
                    Ok(options)
                }
                _ => Err(error::to_py_err("Credentials not found for az://...")),
            }
        }
        "gs" => match env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            Ok(path) => {
                merge_if_missing(&mut options, "google_service_account_path", path);
                Ok(options)
            }
            Err(_) => Err(error::to_py_err("Credentials not found for gs://...")),
        },
        other => Err(error::to_py_err(format!(
            "Unsupported storage scheme: {}. Supported: file, s3, az, gs, abfs, abfss, or local paths",
            other
        ))),
    }
}

#[allow(dead_code)]
pub fn resolve_credentials(
    py: Python<'_>,
    credential_provider: &Bound<'_, PyAny>,
    storage_options: Option<HashMap<String, String>>,
    storage_location: &str,
) -> PyResult<HashMap<String, String>> {
    if credential_provider.is_none() {
        return auto_credentials(storage_location, storage_options.unwrap_or_default());
    }

    if credential_provider.is_callable() {
        let result = credential_provider
            .call0()
            .map_err(|err| error::to_py_err(format!("Credential provider failed: {}", err)))?;
        let options = result
            .extract::<HashMap<String, String>>()
            .map_err(|_| error::to_py_err("Credential provider must return dict[str, str]"))?;
        return Ok(options);
    }

    if let Ok(provider) = credential_provider.extract::<String>() {
        if provider == "auto" {
            return auto_credentials(storage_location, storage_options.unwrap_or_default());
        }
        return Err(error::to_py_err(format!(
            "Unsupported credential_provider: {}",
            provider
        )));
    }

    let _ = py;
    Err(error::to_py_err(
        "credential_provider must be \"auto\" or Callable",
    ))
}

#[pyfunction]
#[pyo3(signature = (storage_location, credential_provider = None, storage_options = None))]
pub fn _resolve_credentials(
    py: Python<'_>,
    storage_location: &str,
    credential_provider: Option<PyObject>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<HashMap<String, String>> {
    let credential_provider = credential_provider.unwrap_or_else(|| py.None());
    let credential_provider = credential_provider.bind(py);
    resolve_credentials(py, credential_provider, storage_options, storage_location)
}
