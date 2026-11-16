package observability

import "time"

// The reason comes from the actual rejecting branch, not a later reconstruction.
// ComparedValue/Limit are the exact atomic reservation and threshold for slot or
// byte rejection. Other gauges are independent concurrent samples after refund,
// not a transactionally consistent queue snapshot. No payload is retained.
type logAdmissionObservation struct {
	ObservedAt    time.Time `json:"observed_at"`
	Reason        string    `json:"reason"`
	EventBytes    int64     `json:"event_bytes"`
	ComparedValue int64     `json:"compared_value"`
	ComparedLimit int64     `json:"compared_limit"`
	QueueDepth    int64     `json:"queue_depth"`
	QueuedBytes   int64     `json:"queued_bytes"`
	OrdinarySlots int64     `json:"ordinary_slots"`
	QueueCapacity int       `json:"queue_capacity"`
	MemoryLimit   int64     `json:"memory_limit_bytes"`
}

func (p *Pipeline) observeLogAdmission(dst *logAdmissionObservation, reason string, size, value, limit int64) {
	if dst == nil {
		return
	}
	*dst = logAdmissionObservation{ObservedAt: time.Now().UTC(), Reason: reason,
		EventBytes: size, ComparedValue: value, ComparedLimit: limit,
		QueueDepth: p.queueDepth.Load(), QueuedBytes: p.queuedBytes.Load(),
		OrdinarySlots: p.ordinaryQueuedSlots.Load(), QueueCapacity: cap(p.queue), MemoryLimit: p.cfg.MemoryLimitBytes}
}
