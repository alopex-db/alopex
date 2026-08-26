//! Bounded wildcard and regular-expression search over opaque KV key bytes.

use regex::bytes::Regex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::types::{Key, Value};

/// Maximum accepted pattern size.
pub const MAX_KEY_PATTERN_BYTES: usize = 4 * 1024;
/// Maximum number of entries returned by one page.
pub const MAX_KEY_SEARCH_LIMIT: usize = 10_000;
/// Maximum number of candidate keys inspected by one page.
pub const MAX_KEY_SEARCH_SCAN_BUDGET: usize = 1_000_000;
/// Default response payload budget before transport serialization overhead.
pub const DEFAULT_KEY_SEARCH_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum configurable response payload budget.
pub const MAX_KEY_SEARCH_RESPONSE_BYTES: usize = 100 * 1024 * 1024;

/// Cooperative cancellation shared with a running bounded search.
#[derive(Debug, Clone, Default)]
pub struct KeySearchCancellation(Arc<AtomicBool>);

impl KeySearchCancellation {
    /// Request cancellation. Repeated calls are harmless.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Return whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Explicit matching mode for raw KV keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum KeyPattern {
    /// Byte glob where `*` matches zero or more bytes, `?` matches one byte,
    /// and `\` escapes the following byte.
    Glob {
        /// Raw pattern bytes.
        pattern: Vec<u8>,
    },
    /// Rust byte-regex syntax. The expression is applied directly to key bytes.
    Regex {
        /// Rust byte-regex source text.
        pattern: String,
    },
}

impl KeyPattern {
    /// Construct an explicit byte-glob pattern.
    pub fn glob(pattern: impl AsRef<[u8]>) -> Self {
        Self::Glob {
            pattern: pattern.as_ref().to_vec(),
        }
    }

    /// Construct an explicit byte-regex pattern.
    pub fn regex(pattern: impl Into<String>) -> Self {
        Self::Regex {
            pattern: pattern.into(),
        }
    }
}

/// One bounded, deterministic search-page request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySearchRequest {
    /// Explicit glob or regex pattern.
    pub pattern: KeyPattern,
    /// Exclusive bytewise cursor from a previous page.
    pub cursor: Option<Key>,
    /// Maximum returned entries.
    pub limit: usize,
    /// Maximum candidate keys inspected after `cursor`.
    pub scan_budget: usize,
    /// Maximum combined key and value bytes inspected or returned by one page.
    #[serde(default = "default_response_bytes")]
    pub max_bytes: usize,
}

impl KeySearchRequest {
    /// Construct a first-page request.
    pub fn new(pattern: KeyPattern, limit: usize, scan_budget: usize) -> Self {
        Self {
            pattern,
            cursor: None,
            limit,
            scan_budget,
            max_bytes: DEFAULT_KEY_SEARCH_RESPONSE_BYTES,
        }
    }

    /// Continue strictly after a prior response cursor.
    pub fn after(mut self, cursor: Key) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Override the response payload budget.
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

/// One matching key/value entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySearchEntry {
    /// Matching raw key bytes.
    pub key: Key,
    /// Value stored under the key.
    pub value: Value,
}

/// A bounded search page in ascending raw-byte key order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeySearchPage {
    /// Matching entries in ascending raw-byte key order.
    pub entries: Vec<KeySearchEntry>,
    /// The last returned key when the page filled its limit.
    pub next_cursor: Option<Key>,
    /// Candidate keys inspected after the cursor.
    pub scanned: usize,
}

pub(crate) struct PreparedKeySearch {
    matcher: Regex,
    prefix: Key,
}

impl PreparedKeySearch {
    pub(crate) fn new(request: &KeySearchRequest) -> Result<Self> {
        validate_request(request)?;
        let (source, prefix) = match &request.pattern {
            KeyPattern::Glob { pattern } => (glob_regex(pattern)?, glob_prefix(pattern)?),
            // Regex syntax is rich enough that a hand-written literal-prefix
            // extractor can introduce false negatives. Keep regex searches
            // on the bounded full-keyspace path.
            KeyPattern::Regex { pattern } => (pattern.clone(), Vec::new()),
        };
        let matcher = Regex::new(&source).map_err(|error| Error::InvalidParameter {
            param: "pattern".into(),
            reason: error.to_string(),
        })?;
        Ok(Self { matcher, prefix })
    }

    pub(crate) fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    pub(crate) fn collect(
        &self,
        mut next: impl FnMut() -> Result<Option<(Key, Option<Value>)>>,
        request: &KeySearchRequest,
        cancellation: &KeySearchCancellation,
    ) -> Result<KeySearchPage> {
        let mut entries = Vec::with_capacity(request.limit);
        let mut scanned = 0usize;
        let mut scanned_bytes = 0usize;
        let mut response_bytes = 0usize;
        loop {
            if cancellation.is_cancelled() {
                return Err(Error::SearchCancelled);
            }
            if scanned == request.scan_budget {
                return Err(Error::SearchBudgetExceeded {
                    limit: request.scan_budget,
                });
            }
            let Some((key, value)) = next()? else {
                break;
            };
            if !self.prefix.is_empty() && !key.starts_with(&self.prefix) {
                break;
            }
            scanned += 1;
            scanned_bytes = scanned_bytes
                .saturating_add(key.len())
                .saturating_add(value.as_ref().map_or(0, Vec::len));
            if scanned_bytes > request.max_bytes {
                return Err(Error::SearchResponseTooLarge {
                    limit: request.max_bytes,
                    requested: scanned_bytes,
                });
            }
            if let Some(value) = value.filter(|_| self.matcher.is_match(&key)) {
                let cursor_bytes = if entries.len() + 1 == request.limit {
                    key.len()
                } else {
                    0
                };
                let requested = response_bytes
                    .saturating_add(key.len())
                    .saturating_add(value.len())
                    .saturating_add(cursor_bytes);
                if requested > request.max_bytes {
                    return Err(Error::SearchResponseTooLarge {
                        limit: request.max_bytes,
                        requested,
                    });
                }
                response_bytes = requested;
                entries.push(KeySearchEntry { key, value });
                if entries.len() == request.limit {
                    break;
                }
            }
        }
        let next_cursor = (entries.len() == request.limit)
            .then(|| entries.last().expect("non-empty full page").key.clone());
        Ok(KeySearchPage {
            entries,
            next_cursor,
            scanned,
        })
    }
}

fn validate_request(request: &KeySearchRequest) -> Result<()> {
    let pattern_len = match &request.pattern {
        KeyPattern::Glob { pattern } => pattern.len(),
        KeyPattern::Regex { pattern } => pattern.len(),
    };
    if pattern_len > MAX_KEY_PATTERN_BYTES {
        return invalid(
            "pattern",
            format!("must be at most {MAX_KEY_PATTERN_BYTES} bytes"),
        );
    }
    if !(1..=MAX_KEY_SEARCH_LIMIT).contains(&request.limit) {
        return invalid(
            "limit",
            format!("must be between 1 and {MAX_KEY_SEARCH_LIMIT}"),
        );
    }
    if !(1..=MAX_KEY_SEARCH_SCAN_BUDGET).contains(&request.scan_budget) {
        return invalid(
            "scan_budget",
            format!("must be between 1 and {MAX_KEY_SEARCH_SCAN_BUDGET}"),
        );
    }
    if !(1..=MAX_KEY_SEARCH_RESPONSE_BYTES).contains(&request.max_bytes) {
        return invalid(
            "max_bytes",
            format!("must be between 1 and {MAX_KEY_SEARCH_RESPONSE_BYTES}"),
        );
    }
    Ok(())
}

fn default_response_bytes() -> usize {
    DEFAULT_KEY_SEARCH_RESPONSE_BYTES
}

fn invalid<T>(param: &str, reason: String) -> Result<T> {
    Err(Error::InvalidParameter {
        param: param.into(),
        reason,
    })
}

fn glob_regex(pattern: &[u8]) -> Result<String> {
    let mut source = String::from("^(?-u:");
    let mut escaped = false;
    for &byte in pattern {
        if escaped {
            push_byte(&mut source, byte);
            escaped = false;
        } else {
            match byte {
                b'\\' => escaped = true,
                b'*' => source.push_str("(?s:.*)"),
                b'?' => source.push_str("(?s:.)"),
                literal => push_byte(&mut source, literal),
            }
        }
    }
    if escaped {
        return invalid("pattern", "glob ends with an escape byte".into());
    }
    source.push_str(")$");
    Ok(source)
}

fn glob_prefix(pattern: &[u8]) -> Result<Key> {
    let mut prefix = Vec::new();
    let mut escaped = false;
    for &byte in pattern {
        if escaped {
            prefix.push(byte);
            escaped = false;
        } else {
            match byte {
                b'\\' => escaped = true,
                b'*' | b'?' => break,
                literal => prefix.push(literal),
            }
        }
    }
    if escaped {
        return invalid("pattern", "glob ends with an escape byte".into());
    }
    Ok(prefix)
}

fn push_byte(output: &mut String, byte: u8) {
    use std::fmt::Write;
    write!(output, "\\x{byte:02X}").expect("writing to String cannot fail");
}
