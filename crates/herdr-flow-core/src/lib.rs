#![no_std]
#![forbid(unsafe_code)]

//! Pure domain types and deterministic workflow logic for Herdr Flow.
//!
//! This crate is `no_std` and CI compiles it for a bare-metal target with no
//! standard library, preventing reducer code from accessing standard filesystem,
//! network, terminal, Git, clock, process, and random-number APIs. CI also
//! allowlists its direct dependencies.

/// The base protocol implemented by this runtime.
pub const BASE_PROTOCOL: &str = "herdr.flow/v1";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_the_versioned_base_protocol() {
        assert_eq!(BASE_PROTOCOL, "herdr.flow/v1");
    }
}
