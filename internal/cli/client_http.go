package cli

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
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

func (c *Client) doPrepared(httpReq *http.Request) (httpPreparedResponse, error) {
	if httpReq == nil {
		return httpPreparedResponse{}, fmt.Errorf("request is required")
	}

	if c == nil || c.httpClient == nil {
		return httpPreparedResponse{}, fmt.Errorf("http client is not configured")
	}

	if strings.TrimSpace(c.token) != "" && strings.TrimSpace(httpReq.Header.Get("Authorization")) == "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	if strings.TrimSpace(c.cookie) != "" && strings.TrimSpace(httpReq.Header.Get("Cookie")) == "" {
		httpReq.Header.Set("Cookie", c.cookie)
	}

	startedAt := time.Now()
	metrics := httpTimingMetrics{}
	var (
		dnsStartedAt     time.Time
		connectStartedAt time.Time
		tlsStartedAt     time.Time
		firstByteAt      time.Time
	)

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) {
			dnsStartedAt = time.Now()
		},
		DNSDone: func(_ httptrace.DNSDoneInfo) {
			if !dnsStartedAt.IsZero() && metrics.DNS == 0 {
				metrics.DNS = time.Since(dnsStartedAt)
			}
		},
		ConnectStart: func(_, _ string) {
			if connectStartedAt.IsZero() {
				connectStartedAt = time.Now()
			}
		},
		ConnectDone: func(_, _ string, _ error) {
			if !connectStartedAt.IsZero() && metrics.Connect == 0 {
				metrics.Connect = time.Since(connectStartedAt)
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			metrics.ReusedConnection = info.Reused
		},
		TLSHandshakeStart: func() {
			tlsStartedAt = time.Now()
		},
		TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
			if !tlsStartedAt.IsZero() && metrics.TLS == 0 {
				metrics.TLS = time.Since(tlsStartedAt)
			}
		},
		GotFirstResponseByte: func() {
			if firstByteAt.IsZero() {
				firstByteAt = time.Now()
			}
		},
	}

	ctx := httpReq.Context()
	if ctx == nil {
		ctx = context.Background()
	}
	httpReq = httpReq.Clone(httptrace.WithClientTrace(ctx, trace))

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		metrics.Total = time.Since(startedAt)
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
	metrics.Total = time.Since(startedAt)
	if !firstByteAt.IsZero() {
		metrics.TTFB = firstByteAt.Sub(startedAt)
	}

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
		return httpPreparedResponse{}, fmt.Errorf("read response: %w", readErr)
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
