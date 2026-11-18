package observability

import (
	"strings"
	"time"
)

const invalidRecordRuntimeUnavailableMessage = "runtime_log_unavailable_message"

// A fixed-size observation of the exact rejected record. Do not retain its
// text, timestamp token, runtime ID or any other payload. Classification is
// diagnostic only: it does not establish whether retained logs were lost.
type logInvalidRecordObservation struct {
	ObservedAt  time.Time                   `json:"observed_at"`
	Class       string                      `json:"class"`
	RecordBytes int                         `json:"record_bytes"`
	Read        logReadTransportObservation `json:"read"`
}

func observeInvalidLogRecord(line string, trace *logReadTransportTrace) *logInvalidRecordObservation {
	class := "timestamp_invalid"
	switch {
	case strings.HasPrefix(line, "unable to retrieve container logs for "):
		class = invalidRecordRuntimeUnavailableMessage
	case !strings.Contains(line, " "):
		class = "timestamp_missing"
	default:
		stamp, _, _ := strings.Cut(line, " ")
		if parsed, err := time.Parse(time.RFC3339Nano, stamp); err == nil && parsed.IsZero() {
			class = "timestamp_zero"
		}
	}
	v := &logInvalidRecordObservation{ObservedAt: time.Now().UTC(), Class: class, RecordBytes: len(line)}
	if fact := trace.snapshot(); fact != nil {
		v.Read = *fact
	}
	return v
}
