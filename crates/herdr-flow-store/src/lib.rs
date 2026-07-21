#![forbid(unsafe_code)]

//! Persistence adapters for Herdr Flow.
//!
//! The initial implementation will provide an atomic SQLite event journal and a
//! content-addressed artifact store. Domain decisions remain in
//! `herdr-flow-core`.

/// Returns the protocol understood by this store adapter.
pub fn base_protocol() -> &'static str {
    herdr_flow_core::BASE_PROTOCOL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_the_core_protocol() {
        assert_eq!(base_protocol(), "herdr.flow/v1");
    }
}
