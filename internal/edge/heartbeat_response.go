package edge

import (
	"context"
	"io"
	"net/http"
	"time"
)

const heartbeatResponseDrainLimit = 64 << 10
const heartbeatResponseDrainTimeout = time.Second

// Heartbeat success is acknowledged by the status code. Consuming its small
// response permits HTTP/1.1 connection reuse; an oversized, stalled or broken
// optional body must not turn an acknowledged POST into a retry.
func drainSuccessfulHeartbeatResponse(response *http.Response, cancel context.CancelFunc) {
	if response.ContentLength > heartbeatResponseDrainLimit {
		return
	}
	timer := time.AfterFunc(heartbeatResponseDrainTimeout, cancel)
	defer timer.Stop()
	// One extra byte distinguishes EOF from hitting the bound. Copy to Discard
	// without retaining the response or trusting a missing Content-Length.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, heartbeatResponseDrainLimit+1))
}
