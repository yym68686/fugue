package main

import (
	"fmt"
	"io"
	"sync/atomic"
)

// imageCacheMetrics is process-local. The control plane remains authoritative
// for replica state; these counters describe data-plane work by one cache.
type imageCacheMetrics struct {
	replicationTotal      atomic.Uint64
	replicationSuccess    atomic.Uint64
	replicationFailure    atomic.Uint64
	copyInvocations       atomic.Uint64
	hydrateWaitTotal      atomic.Uint64
	peerHitTotal          atomic.Uint64
	upstreamHitTotal      atomic.Uint64
	bytesSkippedTotal     atomic.Uint64
	bytesTransferredTotal atomic.Uint64
	diskPressureTotal     atomic.Uint64
}

func (m *imageCacheMetrics) writePrometheus(w io.Writer) {
	if m == nil {
		return
	}
	write := func(name, help string, value uint64) {
		_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
	}
	write("fugue_image_cache_replication_total", "Image replication requests received.", m.replicationTotal.Load())
	write("fugue_image_cache_replication_success_total", "Image replication requests that completed and were verified.", m.replicationSuccess.Load())
	write("fugue_image_cache_replication_failure_total", "Image replication requests that failed.", m.replicationFailure.Load())
	write("fugue_image_cache_copy_invocations_total", "Underlying image copy operations started.", m.copyInvocations.Load())
	write("fugue_image_cache_hydrate_wait_total", "Requests that joined an already running hydrate operation.", m.hydrateWaitTotal.Load())
	write("fugue_image_cache_peer_hit_total", "Hydrates completed from a peer image-cache.", m.peerHitTotal.Load())
	write("fugue_image_cache_upstream_hit_total", "Hydrates completed from the configured upstream registry.", m.upstreamHitTotal.Load())
	write("fugue_image_cache_bytes_skipped_total", "Blob bytes skipped because the destination already had the required digest.", m.bytesSkippedTotal.Load())
	write("fugue_image_cache_bytes_transferred_total", "Blob bytes received during digest-aware replication.", m.bytesTransferredTotal.Load())
	write("fugue_image_cache_disk_pressure_total", "Inventory observations where the configured disk pressure guard was active.", m.diskPressureTotal.Load())
}
