package clientmeasurement

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeproof"
)

func MeasureRound(ctx context.Context, plan model.EdgeClientProbePlan) (model.EdgeClientProbeReport, error) {
	report := model.EdgeClientProbeReport{Schema: "fugue.client-probe-report/v1", Plan: plan, Outcomes: make([]model.EdgeClientProbeOutcome, len(plan.Permits))}
	if plan.Schema != "fugue.client-probe-plan/v1" || len(plan.Permits) < 2 || len(plan.Permits) > 8 {
		return report, errors.New("bounded signed client probe plan required")
	}
	for _, permit := range plan.Permits {
		if err := ValidatePermit(permit); err != nil {
			return report, err
		}
	}
	var workers sync.WaitGroup
	for index, permit := range plan.Permits {
		workers.Add(1)
		go func(index int, permit model.EdgeClientProbePermit) {
			defer workers.Done()
			report.Outcomes[index] = measure(ctx, permit)
		}(index, permit)
	}
	workers.Wait()
	return report, nil
}

func measure(ctx context.Context, permit model.EdgeClientProbePermit) model.EdgeClientProbeOutcome {
	result := model.EdgeClientProbeOutcome{AttemptID: permit.AttemptID, StartedAt: time.Now().UTC()}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{Proxy: nil, ForceAttemptHTTP2: false, DisableCompression: true, DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: permit.Hostname}, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", net.JoinHostPort(permit.Address, "443"))
		}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 85 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	target := url.URL{Scheme: "https", Host: permit.Hostname, Path: permit.Path}
	probeContext, cancel := context.WithDeadline(ctx, permit.ExpiresAt.Add(-2*time.Second))
	defer cancel()
	var traceMu sync.Mutex
	established, tlsReady := false, false
	trace := &httptrace.ClientTrace{
		ConnectDone: func(network, address string, err error) {
			traceMu.Lock()
			established = err == nil
			traceMu.Unlock()
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			traceMu.Lock()
			tlsReady = err == nil
			traceMu.Unlock()
		},
	}
	request, err := http.NewRequestWithContext(httptrace.WithClientTrace(probeContext, trace), http.MethodGet, target.String(), nil)
	if err != nil {
		result.Failure, result.CompletedAt = "response", time.Now().UTC()
		return result
	}
	raw, _ := json.Marshal(permit)
	request.Header.Set(RequestHeader, base64.RawURLEncoding.EncodeToString(raw))
	request.Header.Set(routeproof.RequestHeader, "1")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("User-Agent", "fugue-authenticated-network-probe/v1")
	response, err := client.Do(request)
	if err != nil {
		traceMu.Lock()
		result.Failure = "response"
		if !established {
			result.Failure = "connect"
		} else if !tlsReady {
			result.Failure = "tls"
		}
		traceMu.Unlock()
		result.CompletedAt = time.Now().UTC()
		return result
	}
	defer response.Body.Close()
	return receiveProbeResponse(result, permit, response)
}

func receiveProbeResponse(result model.EdgeClientProbeOutcome, permit model.EdgeClientProbePermit, response *http.Response) model.EdgeClientProbeOutcome {
	result.HTTPStatus = response.StatusCode
	if response.StatusCode != http.StatusOK || response.ContentLength != BodyBytes || response.Header.Get("Content-Encoding") != "" || len(response.Header.Values(AttestationHeader)) != 1 || len(response.Header.Get(AttestationHeader)) > 16384 {
		result.Failure, result.CompletedAt = "response", time.Now().UTC()
		return result
	}
	raw, err := base64.RawURLEncoding.DecodeString(response.Header.Get(AttestationHeader))
	var attestation model.EdgeClientProbeAttestation
	if err != nil || json.Unmarshal(raw, &attestation) != nil || ValidateAttestation(attestation, permit) != nil {
		result.Failure, result.CompletedAt = "integrity", time.Now().UTC()
		return result
	}
	result.Attestation = &attestation
	first := make([]byte, 1)
	count, firstErr := io.ReadFull(response.Body, first)
	started := time.Now()
	remaining, readErr := io.ReadAll(io.LimitReader(response.Body, BodyBytes))
	result.BodySeconds = time.Since(started).Seconds()
	body := append(first[:count], remaining...)
	result.BytesReceived, result.BodySHA256 = len(body), Digest(body)
	result.CompletedAt = time.Now().UTC()
	if firstErr != nil || readErr != nil || len(body) != BodyBytes {
		result.Failure = "body"
	} else if result.BodySHA256 != permit.BodySHA256 {
		result.Failure = "integrity"
	}
	return result
}
