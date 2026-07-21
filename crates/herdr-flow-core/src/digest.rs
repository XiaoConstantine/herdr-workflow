use alloc::string::String;
use core::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest as _, Sha256};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestParseError {
    InvalidPrefix,
    InvalidLength,
    InvalidHex,
}

impl Sha256Digest {
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_prefixed_string(self) -> String {
        let mut output = String::with_capacity(71);
        output.push_str("sha256:");
        for byte in self.0 {
            output.push(HEX[(byte >> 4) as usize] as char);
            output.push(HEX[(byte & 0x0f) as usize] as char);
        }
        output
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

impl fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_prefixed_string())
    }
}

impl fmt::Display for DigestParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrefix => formatter.write_str("digest must start with sha256:"),
            Self::InvalidLength => formatter.write_str("SHA-256 digest must contain 64 hex digits"),
            Self::InvalidHex => {
                formatter.write_str("SHA-256 digest must use lowercase hexadecimal")
            }
        }
    }
}

impl FromStr for Sha256Digest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let encoded = value
            .strip_prefix("sha256:")
            .ok_or(DigestParseError::InvalidPrefix)?;
        if encoded.len() != 64 {
            return Err(DigestParseError::InvalidLength);
        }

        let mut bytes = [0_u8; 32];
        for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (decode_hex(pair[0])? << 4) | decode_hex(pair[1])?;
        }
        Ok(Self(bytes))
    }
}

fn decode_hex(value: u8) -> Result<u8, DigestParseError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(DigestParseError::InvalidHex),
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_prefixed_string())
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use super::*;

    #[test]
    fn hashes_and_round_trips_bytes() {
        let digest = Sha256Digest::of_bytes(b"abc");

        assert_eq!(
            digest.to_string(),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(digest.to_string().parse(), Ok(digest));
    }

    #[test]
    fn rejects_noncanonical_digest_text() {
        assert_eq!(
            "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"
                .parse::<Sha256Digest>(),
            Err(DigestParseError::InvalidPrefix)
        );
        assert_eq!(
            "sha256:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"
                .parse::<Sha256Digest>(),
            Err(DigestParseError::InvalidHex)
        );
    }
}
