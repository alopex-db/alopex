pub mod batch;
pub mod cli;
pub mod client;
pub mod config;
pub mod error;
pub mod models;
pub mod output;
pub mod profile;
pub mod progress;
pub mod version;

#[cfg(test)]
pub mod commands {
    pub mod columnar;
    pub mod sql;
    pub mod vector;
}
#[cfg(test)]
pub mod streaming;
#[cfg(test)]
mod streaming_outputs_test;
