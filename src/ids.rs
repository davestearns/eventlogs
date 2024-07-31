use base64::prelude::*;
use chrono::{DateTime, SubsecRound, Utc};
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr};
use thiserror::Error;

/// The common prefix for all log IDs so that
/// applications can tell LogIds apart from other
/// IDs in their system.
pub const LOG_ID_PREFIX: &str = "log_";

/// The number of random bytes used when creating a LogId
/// via [LogId::new]. These random bytes are paired with
/// a microsecond resolution timestamp, so by default you
/// should be able to create about 2^64 IDs _per-microsecond_
/// without collision. If you need more, use the
/// [LogId::with_entropy_len] method to specify the number
/// of random bytes.
pub const DEFAULT_ENTROPY_LEN: usize = 8;

/// Uniquely identifies an event log.
///
/// LogIds can be encoded into strings using `to_string()`, and parsed
/// from strings using `parse()`:
/// ```
/// use eventlogs::ids::LogId;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let log_id = LogId::new();
/// let s = log_id.to_string();
/// let parsed_log_id: LogId = s.parse()?;
/// assert_eq!(parsed_log_id, log_id);
///
/// // or use parse::<LogId>() in an expression
/// assert_eq!(s.parse::<LogId>().unwrap(), log_id);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct LogId {
    created_at: DateTime<Utc>,
    random: Vec<u8>,
}

impl LogId {
    /// Creates a new LogId with [DEFAULT_ENTROPY_LEN] random bytes.
    /// The length of this ID encoded as a string will be 26 characters.
    pub fn new() -> Self {
        Self::with_entropy_len(DEFAULT_ENTROPY_LEN)
    }

    /// Creates a new LogId with the specified number of random bytes
    /// paired with a microsecond-resolution timestamp.
    /// The more entropy bytes, the longer the encoded ID, so ensure
    /// your database column is sized appropriately.
    pub fn with_entropy_len(entropy_len: usize) -> Self {
        // Truncate the DateTime to microseconds
        // so that when we encode this into a string
        // using timestamp_micros() and parse it back
        // into a DateTime using from_timestamp_micros(),
        // the DateTimes will actually be equal.
        let created_at = Utc::now().trunc_subsecs(6);
        // alloc an array of bytes for the random portion
        // and fill it using rand::ThreadRng, which is
        // secure-enough for our purposes.
        let mut random = vec![0; entropy_len];
        thread_rng().fill_bytes(&mut random);

        Self {
            created_at,
            random: random.to_vec(),
        }
    }

    /// Returns the timestamp at which this LogId was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}

/// Same as calling [LogId::new].
impl Default for LogId {
    fn default() -> Self {
        Self::new()
    }
}

/// Encodes this [LogId] to a [String], suitable for storing in a database.
/// The String is base64-encoded using the URL-safe alphabet, so it is also
/// suitable for transmitting over HTTP.
impl Display for LogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut buf: Vec<u8> = Vec::with_capacity(8 + self.random.len());
        buf.extend(self.created_at.timestamp_micros().to_le_bytes());
        buf.extend(&self.random);

        write!(f, "{LOG_ID_PREFIX}{}", BASE64_URL_SAFE_NO_PAD.encode(buf))
    }
}

impl From<LogId> for String {
    fn from(value: LogId) -> Self {
        value.to_string()
    }
}

/// Errors that can be returned when parsing a [LogId] from a string.
#[derive(Debug, Error, Clone)]
pub enum LogIdParsingError {
    /// The string is too short to be an encoded [LogId].
    #[error("string is too short")]
    TooShort,
    /// An error occurred while base64-decoding the string.
    #[error("error decoding string")]
    Decode(#[from] base64::DecodeError),
    /// The timestamp in the decoded string is out of acceptable range.
    #[error("timestamp {0} is out of acceptable range")]
    TimestampOutOfRange(i64),
}

impl FromStr for LogId {
    type Err = LogIdParsingError;

    /// Parses a [LogId] from a string. Normally you'd use this via the
    /// `&str.parse()` method:
    /// ```
    /// # use eventlogs::ids::{LogId, LogIdParsingError};
    /// # fn main() -> Result<(), LogIdParsingError> {
    /// # let saved_log_id = LogId::new().to_string();
    /// let log_id: LogId = saved_log_id.parse()?;
    /// # Ok(())
    /// # }
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < LOG_ID_PREFIX.len() {
            return Err(LogIdParsingError::TooShort);
        }

        let decoded = BASE64_URL_SAFE_NO_PAD.decode(&s[LOG_ID_PREFIX.len()..])?;

        // decoded portion must have at least 8 bytes for the timestamp
        if decoded.len() < 8 {
            return Err(LogIdParsingError::TooShort);
        }

        let timestamp = i64::from_le_bytes(decoded[0..8].try_into().expect("checked length above"));

        let created_at = DateTime::<Utc>::from_timestamp_micros(timestamp)
            .ok_or(LogIdParsingError::TimestampOutOfRange(timestamp))?;

        Ok(LogId {
            created_at,
            random: decoded[8..].to_vec(),
        })
    }
}

impl TryFrom<String> for LogId {
    type Error = LogIdParsingError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        FromStr::from_str(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let log_id = LogId::new();
        let s = log_id.to_string();
        assert!(s.starts_with(LOG_ID_PREFIX));
        assert_eq!(s.len(), 26);

        let parsed: LogId = s.parse().unwrap();
        assert_eq!(parsed, log_id);
    }

    #[test]
    fn with_entropy_len() {
        let log_id = LogId::with_entropy_len(16);
        let s = log_id.to_string();
        assert!(s.starts_with(LOG_ID_PREFIX));
        assert_eq!(s.len(), 36);

        let parsed: LogId = s.parse().unwrap();
        assert_eq!(parsed, log_id);
    }

    #[test]
    fn empty_too_short() {
        assert!(matches!(
            "".parse::<LogId>(),
            Err(LogIdParsingError::TooShort)
        ));
    }

    #[test]
    fn prefix_only_too_short() {
        assert!(matches!(
            LOG_ID_PREFIX.parse::<LogId>(),
            Err(LogIdParsingError::TooShort)
        ));
    }

    #[test]
    fn invalid_base64_decode_error() {
        let s = format!("{LOG_ID_PREFIX}🤓");
        let result: Result<LogId, LogIdParsingError> = s.parse::<LogId>();
        assert!(matches!(result, Err(LogIdParsingError::Decode(_))));
    }

    #[test]
    fn serializes_as_string() {
        let log_id = LogId::new();
        let serialized = serde_json::to_string(&log_id).unwrap();
        // serialized JSON strings are quoted
        let expected = format!("\"{}\"", log_id.to_string());
        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserializes_from_string() {
        let log_id = LogId::new();
        let serialized = format!("\"{}\"", log_id.to_string());
        let deserialized: LogId = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, log_id);
    }
}
