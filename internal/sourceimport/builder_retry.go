package sourceimport

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	defaultBuilderJobMaxAttempts = 2
)

var retriableBuilderFailureSignals = []string{
	"unexpected eof",
	"tls handshake timeout",
	"net/http: tls handshake timeout",
	"i/o timeout",
	"context deadline exceeded",
	"connection reset by peer",
	"connection refused",
	"no route to host",
	"temporary failure in name resolution",
	"server closed idle connection",
	"http2: client connection lost",
	"transport is closing",
	"timeout awaiting response headers",
	"too many requests",
	"service unavailable",
	"gateway timeout",
	"signal: killed",
	"resource temporarily unavailable",
	"no space left on device",
	"ephemeral-storage",
	"disk-pressure",
	"diskpressure",
	"evicted",
}

func runBuilderJobWithRetry(ctx context.Context, kind, jobName, imageRef string, logger *log.Logger, run func(context.Context) error) error {
	logger = effectiveBuilderLogger(logger)
	if run == nil {
		return fmt.Errorf("builder job runner is nil")
	}

	var lastErr error
	for attempt := 1; attempt <= defaultBuilderJobMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			break
		}
		err := run(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		retriable, signal := shouldRetryBuilderJobFailure(err)
		logger.Printf(
			"builder job attempt failed kind=%s name=%s image=%s attempt=%d/%d retriable=%t signal=%s err=%v",
			strings.TrimSpace(kind),
			strings.TrimSpace(jobName),
			strings.TrimSpace(imageRef),
			attempt,
			defaultBuilderJobMaxAttempts,
			retriable,
			strings.TrimSpace(signal),
			err,
		)
		if !retriable || attempt >= defaultBuilderJobMaxAttempts {
			break
		}

		delay := builderRetryBackoff(attempt)
		logger.Printf(
			"builder job retry scheduled kind=%s name=%s image=%s next_attempt=%d/%d after=%s",
			strings.TrimSpace(kind),
			strings.TrimSpace(jobName),
			strings.TrimSpace(imageRef),
			attempt+1,
			defaultBuilderJobMaxAttempts,
			delay,
		)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastErr
		case <-timer.C:
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return context.Cause(ctx)
}

func shouldRetryBuilderJobFailure(err error) (bool, string) {
	if err == nil {
		return false, ""
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if isEmptyKubectlGetJobExitStatusOne(message) {
		return true, "kubectl get job exit status 1"
	}
	for _, signal := range retriableBuilderFailureSignals {
		if strings.Contains(message, signal) {
			return true, signal
		}
	}
	return false, ""
}

func builderRetryBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return 5 * time.Second
	}
	return time.Duration(attempt*attempt) * 5 * time.Second
}
