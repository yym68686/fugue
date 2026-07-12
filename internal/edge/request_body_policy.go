package edge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

var errEdgeRequestBodyPolicyTooLarge = errors.New("edge request body policy limit exceeded")

type edgeRequestBodyPolicyDeadlineContextKey struct{}

type edgeRequestBodyPolicyGuard struct {
	slots chan struct{}
}

type edgePolicyLimitedReadCloser struct {
	reader   io.ReadCloser
	ctx      context.Context
	maxBytes int64
	read     int64
	once     sync.Once
	closeErr error
}

func (s *Service) applyEdgeRequestBodyPolicy(w http.ResponseWriter, r *http.Request, route model.EdgeRouteBinding) (*http.Request, func(), bool, int) {
	if s == nil || r == nil || r.URL == nil {
		return r, nil, false, 0
	}
	policy, ok := model.MatchEdgeRequestBodyPolicy(route.RequestBodyPolicies, r.Method, r.URL.Path)
	if !ok {
		return r, nil, false, 0
	}
	if r.ContentLength > policy.MaxBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return r, nil, true, http.StatusRequestEntityTooLarge
	}

	guard := s.edgeRequestBodyPolicyGuard(route.AppID, policy)
	if guard == nil || !guard.tryAcquire() {
		retryAfter := policy.RetryAfterSeconds
		if retryAfter <= 0 {
			retryAfter = 5
		}
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		http.Error(w, "too many concurrent requests", http.StatusTooManyRequests)
		return r, nil, true, http.StatusTooManyRequests
	}

	timeout := time.Duration(policy.TimeoutSeconds) * time.Second
	deadline := time.Now().Add(timeout)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	ctx = context.WithValue(ctx, edgeRequestBodyPolicyDeadlineContextKey{}, deadline)
	r = r.WithContext(ctx)
	responseController := http.NewResponseController(w)
	readDeadlineSet := responseController.SetReadDeadline(deadline) == nil
	if r.Body != nil {
		r.Body = &edgePolicyLimitedReadCloser{
			reader:   r.Body,
			ctx:      ctx,
			maxBytes: policy.MaxBytes,
		}
	}
	release := func() {
		cancel()
		if readDeadlineSet {
			_ = responseController.SetReadDeadline(time.Time{})
		}
		guard.release()
	}
	return r, release, false, 0
}

func (s *Service) edgeRequestBodyPolicyGuard(appID string, policy model.EdgeRequestBodyPolicy) *edgeRequestBodyPolicyGuard {
	if s == nil || policy.MaxConcurrent <= 0 {
		return nil
	}
	key := strings.Join([]string{
		strings.TrimSpace(appID),
		strings.TrimSpace(policy.Name),
		strconv.Itoa(policy.MaxConcurrent),
	}, "\x00")
	s.requestBodyPolicyMu.Lock()
	defer s.requestBodyPolicyMu.Unlock()
	if s.requestBodyPolicyGuards == nil {
		s.requestBodyPolicyGuards = map[string]*edgeRequestBodyPolicyGuard{}
	}
	if guard := s.requestBodyPolicyGuards[key]; guard != nil {
		return guard
	}
	guard := &edgeRequestBodyPolicyGuard{slots: make(chan struct{}, policy.MaxConcurrent)}
	s.requestBodyPolicyGuards[key] = guard
	return guard
}

func (g *edgeRequestBodyPolicyGuard) tryAcquire() bool {
	if g == nil || g.slots == nil {
		return false
	}
	select {
	case g.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (g *edgeRequestBodyPolicyGuard) release() {
	if g == nil || g.slots == nil {
		return
	}
	select {
	case <-g.slots:
	default:
	}
}

func (r *edgePolicyLimitedReadCloser) Read(buffer []byte) (int, error) {
	if r == nil || r.reader == nil {
		return 0, io.EOF
	}
	if r.maxBytes <= 0 {
		return 0, errEdgeRequestBodyPolicyTooLarge
	}
	remaining := r.maxBytes - r.read
	if remaining > 0 {
		if int64(len(buffer)) > remaining {
			buffer = buffer[:remaining]
		}
		n, err := r.reader.Read(buffer)
		r.read += int64(n)
		return n, r.normalizeReadError(err)
	}

	var probe [1]byte
	n, err := r.reader.Read(probe[:])
	if n > 0 {
		return 0, fmt.Errorf("%w: max_bytes=%d", errEdgeRequestBodyPolicyTooLarge, r.maxBytes)
	}
	return 0, r.normalizeReadError(err)
}

func (r *edgePolicyLimitedReadCloser) normalizeReadError(err error) error {
	if err == nil {
		return nil
	}
	if r != nil && r.ctx != nil && errors.Is(r.ctx.Err(), context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		return context.DeadlineExceeded
	}
	return err
}

func (r *edgePolicyLimitedReadCloser) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	r.once.Do(func() {
		r.closeErr = r.reader.Close()
	})
	return r.closeErr
}

func edgeRequestBodyPolicyErrorIsTooLarge(err error) bool {
	return errors.Is(err, errEdgeRequestBodyPolicyTooLarge) || strings.Contains(strings.ToLower(errString(err)), errEdgeRequestBodyPolicyTooLarge.Error())
}

func edgeRequestBodyPolicyErrorIsTimeout(req *http.Request, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if req == nil {
		return false
	}
	if errors.Is(req.Context().Err(), context.DeadlineExceeded) {
		return true
	}
	deadline, ok := req.Context().Value(edgeRequestBodyPolicyDeadlineContextKey{}).(time.Time)
	return ok && !deadline.IsZero() && !time.Now().Before(deadline)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
