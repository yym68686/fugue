package api

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultBillingImageStorageRefreshDebounce = 2 * time.Second
	defaultBillingImageStorageRefreshTimeout  = 15 * time.Second
)

type billingImageStorageRefreshScheduler struct {
	debounce time.Duration
	timeout  time.Duration

	mu          sync.Mutex
	nextRunByID map[string]time.Time
	group       singleflight.Group
}

func newBillingImageStorageRefreshScheduler(debounce, timeout time.Duration) billingImageStorageRefreshScheduler {
	if debounce <= 0 {
		debounce = defaultBillingImageStorageRefreshDebounce
	}
	if timeout <= 0 {
		timeout = defaultBillingImageStorageRefreshTimeout
	}
	return billingImageStorageRefreshScheduler{
		debounce:    debounce,
		timeout:     timeout,
		nextRunByID: make(map[string]time.Time),
	}
}

func (s *billingImageStorageRefreshScheduler) schedule(
	tenantID string,
	logger *log.Logger,
	refresh func(context.Context, string) error,
) {
	if s == nil || refresh == nil {
		return
	}
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return
	}

	s.mu.Lock()
	if s.nextRunByID == nil {
		s.nextRunByID = make(map[string]time.Time)
	}
	s.nextRunByID[tenantID] = time.Now().Add(s.debounceDuration())
	s.mu.Unlock()

	go func() {
		_, err, _ := s.group.Do(tenantID, func() (any, error) {
			return nil, s.run(tenantID, refresh)
		})
		if err != nil && logger != nil {
			logger.Printf("async billing image storage refresh failed for tenant=%s: %v", tenantID, err)
		}
	}()
}

func (s *billingImageStorageRefreshScheduler) run(
	tenantID string,
	refresh func(context.Context, string) error,
) error {
	for {
		nextRunAt, ok := s.nextRunAt(tenantID)
		if !ok {
			return nil
		}

		delay := time.Until(nextRunAt)
		if delay > 0 {
			timer := time.NewTimer(delay)
			<-timer.C
		}

		latestRunAt, ok := s.nextRunAt(tenantID)
		if !ok {
			return nil
		}
		if latestRunAt.After(nextRunAt) {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.refreshTimeout())
		err := refresh(ctx, tenantID)
		cancel()

		s.mu.Lock()
		pendingRunAt, stillPending := s.nextRunByID[tenantID]
		if stillPending && !pendingRunAt.After(nextRunAt) {
			delete(s.nextRunByID, tenantID)
			stillPending = false
		}
		s.mu.Unlock()

		if err != nil {
			if stillPending {
				continue
			}
			return err
		}
		if !stillPending {
			return nil
		}
	}
}

func (s *billingImageStorageRefreshScheduler) nextRunAt(tenantID string) (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nextRunAt, ok := s.nextRunByID[strings.TrimSpace(tenantID)]
	return nextRunAt, ok
}

func (s *billingImageStorageRefreshScheduler) debounceDuration() time.Duration {
	if s == nil || s.debounce <= 0 {
		return defaultBillingImageStorageRefreshDebounce
	}
	return s.debounce
}

func (s *billingImageStorageRefreshScheduler) refreshTimeout() time.Duration {
	if s == nil || s.timeout <= 0 {
		return defaultBillingImageStorageRefreshTimeout
	}
	return s.timeout
}
