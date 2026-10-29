// Copyright 2026 The Drasi Authors.
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

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    OnceLock,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Non-transactional diagnostic counters. Pending delivery counts may briefly
/// include a receiver's completing receive, separately from channel capacity.
pub struct PipeMetricsSnapshot {
    pub accepted: u64,
    pub delivered: u64,
    pub discarded: u64,
    pub blocked_sends: u64,
    pub queued: usize,
    pub max_queued: usize,
}

#[derive(Default)]
pub(super) struct PipeMetrics {
    telemetry: OnceLock<crate::observability::QueueTelemetry>,
    accepted: AtomicU64,
    delivered: AtomicU64,
    discarded: AtomicU64,
    blocked_sends: AtomicU64,
    queued: AtomicUsize,
    max_queued: AtomicUsize,
}

impl PipeMetrics {
    pub(super) fn bind_telemetry(
        &self,
        telemetry: crate::observability::QueueTelemetry,
    ) -> Result<(), super::PipeError> {
        self.telemetry.set(telemetry).map_err(|_| {
            super::PipeError::Backend(anyhow::anyhow!("queue telemetry is already bound"))
        })
    }

    pub(super) fn accepted(&self) {
        let telemetry = self.telemetry.get();
        let _update = telemetry.map(|telemetry| telemetry.lock());
        self.accepted.fetch_add(1, Ordering::Relaxed);
        let depth = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_queued.fetch_max(depth, Ordering::Relaxed);
        if let Some(telemetry) = telemetry {
            telemetry.occupancy(depth, self.max_queued.load(Ordering::Relaxed));
        }
    }
    pub(super) fn delivered(&self) {
        let telemetry = self.telemetry.get();
        let _update = telemetry.map(|telemetry| telemetry.lock());
        self.delivered.fetch_add(1, Ordering::Relaxed);
        let depth = self.queued.fetch_sub(1, Ordering::Relaxed) - 1;
        if let Some(telemetry) = telemetry {
            telemetry.occupancy(depth, self.max_queued.load(Ordering::Relaxed));
        }
    }
    pub(super) fn discarded(&self, count: usize) {
        let telemetry = self.telemetry.get();
        let _update = telemetry.map(|telemetry| telemetry.lock());
        self.discarded.fetch_add(count as u64, Ordering::Relaxed);
        let depth = self.queued.fetch_sub(count, Ordering::Relaxed) - count;
        if let Some(telemetry) = telemetry {
            telemetry.discarded(count);
            telemetry.occupancy(depth, self.max_queued.load(Ordering::Relaxed));
        }
    }
    pub(super) fn blocked(&self) {
        self.blocked_sends.fetch_add(1, Ordering::Relaxed);
        if let Some(telemetry) = self.telemetry.get() {
            telemetry.blocked();
        }
    }
    pub(super) fn snapshot(&self) -> PipeMetricsSnapshot {
        PipeMetricsSnapshot {
            accepted: self.accepted.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            discarded: self.discarded.load(Ordering::Relaxed),
            blocked_sends: self.blocked_sends.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            max_queued: self.max_queued.load(Ordering::Relaxed),
        }
    }
}
