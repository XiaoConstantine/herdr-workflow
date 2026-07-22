use alloc::string::String;
use core::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierError {
    InvalidPrefix,
    InvalidLength,
    InvalidUlid,
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrefix => formatter.write_str("identifier has the wrong type prefix"),
            Self::InvalidLength => {
                formatter.write_str("identifier must contain one 26-character ULID")
            }
            Self::InvalidUlid => formatter.write_str("identifier contains a non-canonical ULID"),
        }
    }
}

fn validate(value: &str, prefix: &str) -> Result<(), IdentifierError> {
    let suffix = value
        .strip_prefix(prefix)
        .ok_or(IdentifierError::InvalidPrefix)?;
    if suffix.len() != 26 {
        return Err(IdentifierError::InvalidLength);
    }
    let bytes = suffix.as_bytes();
    if bytes[0] > b'7' || !bytes.iter().copied().all(is_crockford_base32) {
        return Err(IdentifierError::InvalidUlid);
    }
    Ok(())
}

fn is_crockford_base32(value: u8) -> bool {
    matches!(
        value,
        b'0'..=b'9' | b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z'
    )
}

macro_rules! typed_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                validate(&value, $prefix)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(de::Error::custom)
            }
        }
    };
}

typed_id!(RunId, "flow_");
typed_id!(StageInstanceId, "stage_");
typed_id!(RoleBindingId, "role_");
typed_id!(MessageId, "msg_");
typed_id!(EventId, "evt_");
typed_id!(ArtifactId, "art_");
typed_id!(FindingId, "finding_");
typed_id!(BatchId, "batch_");
typed_id!(ParticipantPrincipalId, "principal_");

#[cfg(test)]
mod tests {
    use alloc::{format, string::ToString};

    use super::*;

    const ULID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

    #[test]
    fn accepts_prefixed_canonical_ulids() {
        let run = RunId::parse(format!("flow_{ULID}")).unwrap();

        assert_eq!(run.as_str(), format!("flow_{ULID}"));
    }

    #[test]
    fn rejects_ambiguous_lowercase_and_overflow_ulids() {
        assert_eq!(
            RunId::parse("flow_01arz3ndektsv4rrffq69g5fav"),
            Err(IdentifierError::InvalidUlid)
        );
        assert_eq!(
            RunId::parse("flow_81ARZ3NDEKTSV4RRFFQ69G5FAV"),
            Err(IdentifierError::InvalidUlid)
        );
        assert_eq!(
            RunId::parse("stage_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            Err(IdentifierError::InvalidPrefix)
        );
    }

    #[test]
    fn deserialization_cannot_bypass_validation() {
        let error = serde_json::from_str::<MessageId>(r#""msg_not-a-ulid""#).unwrap_err();

        assert!(error.to_string().contains("26-character ULID"));
    }
}
