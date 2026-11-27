package imagebilling

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	defaultDebounce = 2 * time.Second
	defaultTimeout  = 15 * time.Second
)

type Scheduler struct {
	debounce time.Duration
	timeout  time.Duration

	mu          sync.Mutex
	nextRunByID map[string]time.Time
	group       singleflight.Group
	wg          sync.WaitGroup
}

func NewScheduler(debounce, timeout time.Duration) *Scheduler {
	if debounce <= 0 {
		debounce = defaultDebounce
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Scheduler{
		debounce:    debounce,
		timeout:     timeout,
		nextRunByID: make(map[string]time.Time),
	}
}

func (s *Scheduler) Schedule(
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

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_, err, _ := s.group.Do(tenantID, func() (any, error) {
			return nil, s.run(tenantID, refresh)
		})
		if err != nil && logger != nil {
			logger.Printf("async billing image storage refresh failed for tenant=%s: %v", tenantID, err)
		}
	}()
}

func (s *Scheduler) run(
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

func (s *Scheduler) nextRunAt(tenantID string) (time.Time, bool) {
	if s == nil {
		return time.Time{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nextRunAt, ok := s.nextRunByID[strings.TrimSpace(tenantID)]
	return nextRunAt, ok
}

func (s *Scheduler) debounceDuration() time.Duration {
	if s == nil || s.debounce <= 0 {
		return defaultDebounce
	}
	return s.debounce
}

func (s *Scheduler) refreshTimeout() time.Duration {
	if s == nil || s.timeout <= 0 {
		return defaultTimeout
	}
	return s.timeout
}

func (s *Scheduler) Wait() {
	if s == nil {
		return
	}
	s.wg.Wait()
}
