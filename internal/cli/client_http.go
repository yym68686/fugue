package cli

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"time"
)

type requestObserver func(httpObservedRequest)

type httpTimingMetrics struct {
	DNS              time.Duration `json:"-"`
	Connect          time.Duration `json:"-"`
	TLS              time.Duration `json:"-"`
	TTFB             time.Duration `json:"-"`
	Total            time.Duration `json:"-"`
	ReusedConnection bool          `json:"reused_connection,omitempty"`
}

type httpObservedRequest struct {
	Method          string
	URL             string
	StatusCode      int
	ResponseHeaders http.Header
	ResponseSize    int
	ServerTiming    string
	Timing          httpTimingMetrics
	Error           string
}

type httpPreparedResponse struct {
	Method     string
	URL        string
	Status     string
	StatusCode int
	Headers    http.Header
	Payload    []byte
	Timing     httpTimingMetrics
}

type httpResponseError struct {
	err     error
	status  int
	headers http.Header
}

func (e *httpResponseError) Error() string { return e.err.Error() }
func (e *httpResponseError) Unwrap() error { return e.err }

func (c *Client) doPrepared(httpReq *http.Request) (httpPreparedResponse, error) {
	if httpReq == nil {
		return httpPreparedResponse{}, fmt.Errorf("request is required")
	}

	if c == nil || c.httpClient == nil {
		return httpPreparedResponse{}, fmt.Errorf("http client is not configured")
	}

	if c.context != nil {
		httpReq = httpReq.Clone(c.context)
	}

	attempts := 1
	if isRetryableReadMethod(httpReq.Method) {
		attempts += c.readRetryCount
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		reqForAttempt, err := cloneRequestForAttempt(httpReq)
		if err != nil {
			return httpPreparedResponse{}, err
		}
		result, err := c.doPreparedOnce(reqForAttempt)
		if err == nil {
			if isRetryableReadMethod(httpReq.Method) && isRetryableHTTPStatus(result.StatusCode) && attempt < attempts {
				if err := waitRequestRetry(httpReq.Context(), c.readRetryDelay); err != nil {
					return result, err
				}
				continue
			}
			return result, nil
		}
		lastErr = err
		if !isRetryableHTTPClientError(err) || attempt == attempts {
			return result, err
		}
		if err := waitRequestRetry(httpReq.Context(), c.readRetryDelay); err != nil {
			return result, err
		}
	}
	return httpPreparedResponse{}, lastErr
}

func (c *Client) doPreparedOnce(httpReq *http.Request) (httpPreparedResponse, error) {
	if httpReq == nil {
		return httpPreparedResponse{}, fmt.Errorf("request is required")
	}

	if strings.TrimSpace(c.token) != "" && strings.TrimSpace(httpReq.Header.Get("Authorization")) == "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	if strings.TrimSpace(c.cookie) != "" && strings.TrimSpace(httpReq.Header.Get("Cookie")) == "" {
		httpReq.Header.Set("Cookie", c.cookie)
	}

	startedAt := time.Now()
	traceMetrics := httpTimingMetrics{}
	var traceMu sync.Mutex
	var (
		dnsStartedAt     time.Time
		connectStartedAt time.Time
		tlsStartedAt     time.Time
		firstByteAt      time.Time
	)

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) {
			traceMu.Lock()
			defer traceMu.Unlock()
			dnsStartedAt = time.Now()
		},
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			traceMu.Lock()
			defer traceMu.Unlock()
			if !dnsStartedAt.IsZero() && traceMetrics.DNS == 0 {
				traceMetrics.DNS = time.Since(dnsStartedAt)
			}
		},
		ConnectStart: func(_, _ string) {
			traceMu.Lock()
			defer traceMu.Unlock()
			if connectStartedAt.IsZero() {
				connectStartedAt = time.Now()
			}
		},
		ConnectDone: func(_, _ string, _ error) {
			traceMu.Lock()
			defer traceMu.Unlock()
			if !connectStartedAt.IsZero() && traceMetrics.Connect == 0 {
				traceMetrics.Connect = time.Since(connectStartedAt)
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			traceMu.Lock()
			defer traceMu.Unlock()
			traceMetrics.ReusedConnection = info.Reused
		},
		TLSHandshakeStart: func() {
			traceMu.Lock()
			defer traceMu.Unlock()
			tlsStartedAt = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			traceMu.Lock()
			defer traceMu.Unlock()
			if !tlsStartedAt.IsZero() && traceMetrics.TLS == 0 {
				traceMetrics.TLS = time.Since(tlsStartedAt)
			}
		},
		GotFirstResponseByte: func() {
			traceMu.Lock()
			defer traceMu.Unlock()
			if firstByteAt.IsZero() {
				firstByteAt = time.Now()
			}
		},
	}
	// Hooks may run concurrently or after the request has completed. Publish
	// a value snapshot so late callbacks cannot race with timing consumers.
	snapshotTiming := func() httpTimingMetrics {
		traceMu.Lock()
		defer traceMu.Unlock()
		metrics := traceMetrics
		metrics.Total = time.Since(startedAt)
		if !firstByteAt.IsZero() {
			metrics.TTFB = firstByteAt.Sub(startedAt)
		}
		return metrics
	}

	ctx := httpReq.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	httpReq = httpReq.Clone(httptrace.WithClientTrace(ctx, trace))

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		metrics := snapshotTiming()
		if c.observer != nil {
			c.observer(httpObservedRequest{
				Method: httpReq.Method,
				URL:    httpReq.URL.String(),
				Timing: metrics,
				Error:  err.Error(),
			})
		}
		return httpPreparedResponse{}, err
	}
	defer resp.Body.Close()

	payload, readErr := io.ReadAll(resp.Body)
	metrics := snapshotTiming()

	result := httpPreparedResponse{
		Method:     httpReq.Method,
		URL:        httpReq.URL.String(),
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Headers:    cloneHTTPHeaders(resp.Header),
		Payload:    payload,
		Timing:     metrics,
	}
	if c.observer != nil {
		observed := httpObservedRequest{
			Method:          result.Method,
			URL:             result.URL,
			StatusCode:      result.StatusCode,
			ResponseHeaders: cloneHTTPHeaders(result.Headers),
			ResponseSize:    len(result.Payload),
			ServerTiming:    strings.TrimSpace(result.Headers.Get("Server-Timing")),
			Timing:          result.Timing,
		}
		if readErr != nil {
			observed.Error = fmt.Sprintf("read response: %v", readErr)
		}
		c.observer(observed)
	}
	if readErr != nil {
		return result, fmt.Errorf("read response: %w", readErr)
	}
	return result, nil
}

func cloneHTTPHeaders(headers http.Header) http.Header {
	if headers == nil {
		return http.Header{}
	}
	cloned := make(http.Header, len(headers))
	for key, values := range headers {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func cloneRequestForAttempt(req *http.Request) (*http.Request, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}
	if req.Body == nil || req.GetBody == nil {
		return req.Clone(req.Context()), nil
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, fmt.Errorf("clone request body: %w", err)
	}
	cloned := req.Clone(req.Context())
	cloned.Body = body
	return cloned, nil
}

func isRetryableReadMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func isRetryableHTTPStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func isRetryableHTTPClientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if isRetryableHTTPClientError(urlErr.Err) {
			return true
		}
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "tls handshake timeout") ||
		strings.Contains(message, "client.timeout exceeded") ||
		strings.Contains(message, "server closed idle connection") ||
		strings.Contains(message, "connection reset by peer")
}

func waitRequestRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
