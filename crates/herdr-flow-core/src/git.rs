use alloc::string::{String, ToString};
use core::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitObjectFormat {
    Sha1,
    Sha256,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GitObjectId(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitObjectIdError {
    InvalidPrefix,
    InvalidLength,
    InvalidHex,
}

impl GitObjectId {
    pub fn parse(value: impl Into<String>) -> Result<Self, GitObjectIdError> {
        let value = value.into();
        let (format, encoded) = if let Some(encoded) = value.strip_prefix("sha1:") {
            (GitObjectFormat::Sha1, encoded)
        } else if let Some(encoded) = value.strip_prefix("sha256:") {
            (GitObjectFormat::Sha256, encoded)
        } else {
            return Err(GitObjectIdError::InvalidPrefix);
        };
        let expected_length = match format {
            GitObjectFormat::Sha1 => 40,
            GitObjectFormat::Sha256 => 64,
        };
        if encoded.len() != expected_length {
            return Err(GitObjectIdError::InvalidLength);
        }
        if !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(GitObjectIdError::InvalidHex);
        }
        Ok(Self(value))
    }

    pub fn from_hex(format: GitObjectFormat, encoded: &str) -> Result<Self, GitObjectIdError> {
        let prefix = match format {
            GitObjectFormat::Sha1 => "sha1:",
            GitObjectFormat::Sha256 => "sha256:",
        };
        Self::parse(prefix.to_string() + encoded)
    }

    pub fn format(&self) -> GitObjectFormat {
        if self.0.starts_with("sha1:") {
            GitObjectFormat::Sha1
        } else {
            GitObjectFormat::Sha256
        }
    }

    pub fn hex(&self) -> &str {
        match self.format() {
            GitObjectFormat::Sha1 => &self.0["sha1:".len()..],
            GitObjectFormat::Sha256 => &self.0["sha256:".len()..],
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GitObjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for GitObjectIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrefix => formatter.write_str("Git object ID has an unsupported format"),
            Self::InvalidLength => formatter.write_str("Git object ID has the wrong length"),
            Self::InvalidHex => formatter.write_str("Git object ID must use lowercase hexadecimal"),
        }
    }
}

impl FromStr for GitObjectId {
    type Err = GitObjectIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl Serialize for GitObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for GitObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_full_lowercase_object_ids() {
        let sha1 = GitObjectId::from_hex(GitObjectFormat::Sha1, &"a".repeat(40)).unwrap();
        let sha256 = GitObjectId::from_hex(GitObjectFormat::Sha256, &"b".repeat(64)).unwrap();
        assert_eq!(sha1.format(), GitObjectFormat::Sha1);
        assert_eq!(sha256.format(), GitObjectFormat::Sha256);
        assert_eq!(sha1.hex().len(), 40);
    }

    #[test]
    fn rejects_abbreviated_uppercase_and_unprefixed_ids() {
        assert_eq!(
            GitObjectId::parse("sha1:abc"),
            Err(GitObjectIdError::InvalidLength)
        );
        assert_eq!(
            GitObjectId::from_hex(GitObjectFormat::Sha1, &"A".repeat(40)),
            Err(GitObjectIdError::InvalidHex)
        );
        assert_eq!(
            GitObjectId::parse("a".repeat(40)),
            Err(GitObjectIdError::InvalidPrefix)
        );
    }
}
