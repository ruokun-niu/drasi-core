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

//! Plugin contract for Write-Ahead Log implementations.

use async_trait::async_trait;
use drasi_core::models::SourceChange;

use super::{WalError, WriteAheadLogConfig};

/// Write-Ahead Log plugin contract.
///
/// A `WalProvider` is user-configured once when building `DrasiLib` and shared
/// across all transient sources that require durable crash recovery. Each
/// source partitions its events by a unique `source_id`.
///
/// The trait mirrors [`StateStoreProvider`](crate::StateStoreProvider): a single
/// trait where every method takes the partition key as its first parameter.
/// Implementations manage per-source state internally (DB handles, counters,
/// file paths, etc.).
///
/// # Lifecycle
///
/// A source must call [`register`](WalProvider::register) before any other method
/// for that `source_id`. All other methods return [`WalError::SourceNotRegistered`]
/// if called for an unregistered source.
///
/// # Thread safety
///
/// Implementations must be `Send + Sync`. A single provider instance is shared
/// across many concurrent tasks; per-source state management must handle
/// concurrent `append`/`read_from`/`prune_up_to` calls safely.
#[async_trait]
pub trait WalProvider: Send + Sync {
    /// Register a source with a WAL instance. Must be called before any other
    /// method for this `source_id`.
    ///
    /// Idempotent when called with the same config (returns `Ok(())` without
    /// reopening). Returns [`WalError::SourceAlreadyRegistered`] if the source
    /// is already registered with a different config.
    ///
    /// Implementations validate the config via
    /// [`WriteAheadLogConfig::validate`] before opening any storage.
    async fn register(&self, source_id: &str, config: WriteAheadLogConfig) -> Result<(), WalError>;

    /// Append an event to the source's WAL; returns the assigned
    /// monotonic-increasing sequence number.
    ///
    /// Rejects [`SourceChange::Future`](drasi_core::models::SourceChange::Future)
    /// as invalid — future events are internal to the query engine and must
    /// never enter a transient source's WAL.
    ///
    /// On capacity exhaustion, behavior depends on the registered
    /// [`CapacityPolicy`](super::CapacityPolicy).
    async fn append(&self, source_id: &str, event: &SourceChange) -> Result<u64, WalError>;

    /// Append a batch of events to the source's WAL as a single durable unit,
    /// returning the assigned monotonic sequence numbers in the same order as
    /// `events`.
    ///
    /// Implementations that can group the writes (e.g. one storage transaction
    /// with a single fsync) should do so to amortize per-event durability cost.
    /// The batch is all-or-nothing: on error, no events are persisted and no
    /// sequence numbers are consumed.
    ///
    /// Rejects the batch if any event is a
    /// [`SourceChange::Future`](drasi_core::models::SourceChange::Future).
    /// On capacity exhaustion, behavior depends on the registered
    /// [`CapacityPolicy`](super::CapacityPolicy); an empty batch is a no-op that
    /// returns an empty vector.
    ///
    /// The default implementation appends events one at a time via
    /// [`append`](WalProvider::append) and therefore provides no grouping
    /// benefit; implementations should override it where batching is possible.
    async fn append_batch(
        &self,
        source_id: &str,
        events: &[SourceChange],
    ) -> Result<Vec<u64>, WalError> {
        let mut sequences = Vec::with_capacity(events.len());
        for event in events {
            sequences.push(self.append(source_id, event).await?);
        }
        Ok(sequences)
    }

    /// Read events from `sequence` (inclusive) to the head, in sequence order.
    ///
    /// Returns [`WalError::PositionUnavailable`] if `sequence` is older than
    /// the oldest retained event (i.e., has been pruned).
    async fn read_from(
        &self,
        source_id: &str,
        sequence: u64,
    ) -> Result<Vec<(u64, SourceChange)>, WalError>;

    /// Remove all events with `seq <= sequence`. Returns the number of
    /// events pruned.
    async fn prune_up_to(&self, source_id: &str, sequence: u64) -> Result<u64, WalError>;

    /// Returns the highest allocated sequence number (in-memory counter).
    ///
    /// May be higher than the highest persisted sequence if a prior write
    /// transaction failed after the counter was incremented. Callers needing
    /// authoritative data should use [`read_from`](WalProvider::read_from).
    async fn head_sequence(&self, source_id: &str) -> Result<u64, WalError>;

    /// Returns the oldest retained sequence number, or `None` if the WAL is empty.
    async fn oldest_sequence(&self, source_id: &str) -> Result<Option<u64>, WalError>;

    /// Returns the number of events currently retained in the WAL.
    async fn event_count(&self, source_id: &str) -> Result<u64, WalError>;

    /// Delete all WAL data for a source and unregister it from the provider.
    ///
    /// Called on source removal with cleanup enabled. Best-effort: does not
    /// error if the source was never registered.
    async fn delete_wal(&self, source_id: &str) -> Result<(), WalError>;
}
