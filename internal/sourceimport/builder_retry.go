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
	oomBuilderJobMaxAttempts     = 4
)

type builderJobAttempt struct {
	Number              int
	OOMRetryCount       int
	EphemeralRetryCount int
}

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
	"oomkilled",
	"out of memory",
}

func runBuilderJobWithRetry(ctx context.Context, kind, jobName, imageRef string, logger *log.Logger, run func(context.Context, builderJobAttempt) error) error {
	logger = effectiveBuilderLogger(logger)
	if run == nil {
		return fmt.Errorf("builder job runner is nil")
	}

	var lastErr error
	oomRetryCount := 0
	ephemeralRetryCount := 0
	for attempt := 1; attempt <= oomBuilderJobMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			break
		}
		err := run(ctx, builderJobAttempt{
			Number:              attempt,
			OOMRetryCount:       oomRetryCount,
			EphemeralRetryCount: ephemeralRetryCount,
		})
		if err == nil {
			return nil
		}
		lastErr = err

		retriable, signal := shouldRetryBuilderJobFailure(err)
		oomFailure := isBuilderOOMFailure(err, signal)
		ephemeralFailure := isBuilderEphemeralStorageFailure(err, signal)
		maxAttempts := defaultBuilderJobMaxAttempts
		if oomFailure || oomRetryCount > 0 || ephemeralFailure || ephemeralRetryCount > 0 {
			maxAttempts = oomBuilderJobMaxAttempts
		}
		logger.Printf(
			"builder job attempt failed kind=%s name=%s image=%s attempt=%d/%d retriable=%t signal=%s err=%v",
			strings.TrimSpace(kind),
			strings.TrimSpace(jobName),
			strings.TrimSpace(imageRef),
			attempt,
			maxAttempts,
			retriable,
			strings.TrimSpace(signal),
			err,
		)
		if !retriable || attempt >= maxAttempts {
			break
		}
		if oomFailure {
			oomRetryCount++
		}
		if ephemeralFailure {
			ephemeralRetryCount++
		}

		delay := builderRetryBackoff(attempt)
		logger.Printf(
			"builder job retry scheduled kind=%s name=%s image=%s next_attempt=%d/%d after=%s",
			strings.TrimSpace(kind),
			strings.TrimSpace(jobName),
			strings.TrimSpace(imageRef),
			attempt+1,
			maxAttempts,
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

func isBuilderOOMFailure(err error, signal string) bool {
	message := strings.ToLower(strings.TrimSpace(signal))
	if err != nil {
		message += " " + strings.ToLower(err.Error())
	}
	return strings.Contains(message, "oomkilled") || strings.Contains(message, "out of memory")
}

func isBuilderEphemeralStorageFailure(err error, signal string) bool {
	message := strings.ToLower(strings.TrimSpace(signal))
	if err != nil {
		message += " " + strings.ToLower(err.Error())
	}
	return strings.Contains(message, "ephemeral-storage") ||
		strings.Contains(message, "ephemeral local storage") ||
		strings.Contains(message, "no space left on device")
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
