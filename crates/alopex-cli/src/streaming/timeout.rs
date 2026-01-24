//! Deadline parsing and enforcement for streaming queries.

use std::time::{Duration, Instant};

use crate::error::{CliError, Result};

pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    start: Instant,
    duration: Duration,
}

impl Deadline {
    pub fn new(duration: Duration) -> Self {
        Self {
            start: Instant::now(),
            duration,
        }
    }

    pub fn remaining(&self) -> Duration {
        let elapsed = self.start.elapsed();
        if elapsed >= self.duration {
            Duration::from_secs(0)
        } else {
            self.duration - elapsed
        }
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }

    pub fn check(&self) -> Result<()> {
        if self.remaining().is_zero() {
            return Err(CliError::Timeout(format!(
                "deadline exceeded after {}",
                humantime::format_duration(self.duration)
            )));
        }
        Ok(())
    }
}

pub fn parse_deadline(value: Option<&str>) -> Result<Duration> {
    let duration = match value {
        Some(raw) => humantime::parse_duration(raw)
            .map_err(|err| CliError::InvalidArgument(format!("Invalid deadline: {err}")))?,
        None => DEFAULT_DEADLINE,
    };
    Ok(duration)
}
