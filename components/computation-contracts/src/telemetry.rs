// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

/// Portable trace identity. This is not a live span or durable event metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceContextV1 {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_flags: u8,
    pub trace_state: Option<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid trace context: {0}")]
pub struct TraceContextError(pub &'static str);

impl TraceContextV1 {
    /// Validate nonzero IDs and bounded W3C trace state before adopting context.
    pub fn validate(&self) -> Result<(), TraceContextError> {
        if self.trace_id == [0; 16] || self.span_id == [0; 8] {
            return Err(TraceContextError("trace and span IDs must be nonzero"));
        }
        if let Some(state) = &self.trace_state {
            if state.is_empty() || state.len() > 512 || !state.is_ascii() {
                return Err(TraceContextError(
                    "trace state must contain 1..=512 ASCII bytes",
                ));
            }
            let mut keys = std::collections::BTreeSet::new();
            for entry in state.split(',') {
                let Some((key, value)) = entry.trim_start_matches([' ', '\t']).split_once('=')
                else {
                    return Err(TraceContextError(
                        "trace state entry requires a key and value",
                    ));
                };
                let key = key.trim_end_matches([' ', '\t']);
                let key_character =
                    |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit() || b"_-*/".contains(&c);
                let valid_key = if let Some((tenant, vendor)) = key.split_once('@') {
                    (1..=241).contains(&tenant.len())
                        && tenant.as_bytes()[0].is_ascii_alphanumeric()
                        && tenant.bytes().all(key_character)
                        && (1..=14).contains(&vendor.len())
                        && vendor.as_bytes()[0].is_ascii_lowercase()
                        && vendor.bytes().all(key_character)
                } else {
                    !key.is_empty()
                        && key.as_bytes()[0].is_ascii_lowercase()
                        && key.bytes().all(key_character)
                };
                if !valid_key || key.len() > 256 || !keys.insert(key) || keys.len() > 32 {
                    return Err(TraceContextError("invalid or duplicate trace state key"));
                }
                if value.is_empty()
                    || value.len() > 256
                    || value.ends_with(' ')
                    || !value
                        .bytes()
                        .all(|c| (0x20..=0x7e).contains(&c) && c != b'=')
                {
                    return Err(TraceContextError("invalid trace state value"));
                }
            }
        }
        Ok(())
    }
}

/// Transient delivery metadata, deliberately separate from envelope codecs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecutionContext {
    pub trace: Option<TraceContextV1>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_ids_and_bounded_trace_state() {
        let mut context = TraceContextV1 {
            trace_id: [1; 16],
            span_id: [2; 8],
            trace_flags: 0,
            trace_state: Some("vendor=value, 123@tenant=another".into()),
        };
        assert!(context.validate().is_ok());
        for state in [
            "",
            "vendor=a,vendor=b",
            "Vendor=a",
            "vendor=a=b",
            "vendor=last ",
        ] {
            context.trace_state = Some(state.into());
            assert!(context.validate().is_err(), "{state}");
        }
        context.trace_state = None;
        context.span_id = [0; 8];
        assert!(context.validate().is_err());
    }
}
