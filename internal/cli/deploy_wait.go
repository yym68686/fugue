package cli

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

const deployWaitTransientErrorWindow = 2 * time.Minute

var deployWaitPollInterval = 2 * time.Second

type deployWaitTransientErrorTracker struct {
	firstSeenAt time.Time
	warned      bool
}

func (t *deployWaitTransientErrorTracker) reset() {
	t.firstSeenAt = time.Time{}
	t.warned = false
}

func (t *deployWaitTransientErrorTracker) shouldRetry(c *CLI, err error) (bool, error) {
	if err == nil {
		t.reset()
		return false, nil
	}
	if !isTransientDeployWaitError(err) {
		return false, err
	}

	now := time.Now()
	if t.firstSeenAt.IsZero() {
		t.firstSeenAt = now
	}
	if now.Sub(t.firstSeenAt) > deployWaitTransientErrorWindow {
		return false, fmt.Errorf("deploy wait API remained temporarily unavailable for %s: %w", deployWaitTransientErrorWindow, err)
	}
	if !t.warned {
		c.progressf("warning=deploy wait API temporarily unavailable; continuing to poll: %v", err)
		t.warned = true
	}
	return true, nil
}

func isTransientDeployWaitError(err error) bool {
	if err == nil {
		return false
	}
	if isRetryableHTTPClientError(err) {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return false
	}
	for _, statusCode := range []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		if strings.Contains(message, fmt.Sprintf("status=%d", statusCode)) ||
			strings.Contains(message, fmt.Sprintf("status %d", statusCode)) ||
			strings.Contains(message, fmt.Sprintf("error code: %d", statusCode)) {
			return true
		}
	}
	return strings.Contains(message, "bad gateway") ||
		strings.Contains(message, "service unavailable") ||
		strings.Contains(message, "gateway timeout")
}

func sleepDeployWaitPoll() {
	time.Sleep(deployWaitPollInterval)
}
