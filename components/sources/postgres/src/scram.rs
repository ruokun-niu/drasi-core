// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SCRAM-SHA-256 client wrapper.
//!
//! Delegates to [`postgres_protocol::authentication::sasl::ScramSha256`] so
//! that the replication connection uses the exact same SCRAM implementation
//! as tokio-postgres.
//!
//! # Channel Binding
//!
//! The caller must provide the correct [`ChannelBinding`] value based on the
//! TLS state and the SASL mechanisms offered by the server.  See
//! [`ReplicationConnection::startup_replication`] for the full negotiation
//! logic that mirrors tokio-postgres.

use anyhow::Result;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};

pub struct ScramClient {
    inner: ScramSha256,
}

impl ScramClient {
    /// Create a new SCRAM-SHA-256 client.
    ///
    /// The `channel_binding` argument must be chosen according to the same
    /// rules as tokio-postgres (see `connect_raw.rs`):
    ///
    /// | Server offers PLUS? | Have TLS CB data? | Use                              |
    /// |---------------------|-------------------|----------------------------------|
    /// | yes                 | yes               | `tls_server_end_point(data)` + SCRAM-SHA-256-PLUS |
    /// | yes                 | no                | `unsupported()` + SCRAM-SHA-256  |
    /// | no                  | yes               | `unrequested()` + SCRAM-SHA-256  |
    /// | no                  | no                | `unsupported()` + SCRAM-SHA-256  |
    pub fn new(_username: &str, password: &str, channel_binding: ChannelBinding) -> Self {
        Self {
            inner: ScramSha256::new(password.as_bytes(), channel_binding),
        }
    }

    /// Returns the client-first-message to send in `SASLInitialResponse`.
    pub fn client_first_message(&self) -> Vec<u8> {
        self.inner.message().to_vec()
    }

    /// Processes the server-first-message from `AuthenticationSASLContinue`.
    pub fn process_server_first_message(&mut self, message: &[u8]) -> Result<()> {
        self.inner
            .update(message)
            .map_err(|e| anyhow::anyhow!("SCRAM server-first processing failed: {e}"))
    }

    /// Returns the client-final-message to send in `SASLResponse`.
    pub fn client_final_message(&self) -> Vec<u8> {
        self.inner.message().to_vec()
    }

    /// Verifies the server-final-message from `AuthenticationSASLFinal`.
    pub fn verify_server_final(&mut self, message: &[u8]) -> Result<()> {
        self.inner
            .finish(message)
            .map_err(|e| anyhow::anyhow!("SCRAM server verification failed: {e}"))
    }
}
