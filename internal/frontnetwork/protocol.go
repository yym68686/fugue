package frontnetwork

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

const Schema = "fugue.public-front-network/v1"
const Path = "/connection"
const Timeout = 300 * time.Millisecond

type readFailure struct {
	reason string
	cause  error
}

func (failure readFailure) Error() string { return failure.reason + ": " + failure.cause.Error() }
func (failure readFailure) Unwrap() error { return failure.cause }

func FailureReason(err error) string {
	var failure readFailure
	if errors.As(err, &failure) {
		return failure.reason
	}
	return "unknown"
}

type Request struct {
	Schema     string `json:"schema"`
	Nonce      string `json:"nonce"`
	EdgeID     string `json:"edge_id"`
	GroupID    string `json:"edge_group_id"`
	Slot       string `json:"slot"`
	RemoteAddr string `json:"remote_addr"`
}

type Response struct {
	Schema  string                        `json:"schema"`
	Nonce   string                        `json:"nonce"`
	EdgeID  string                        `json:"edge_id"`
	GroupID string                        `json:"edge_group_id"`
	Sample  model.EdgeClientNetworkSample `json:"sample"`
}

func ParseRemote(remote string) (netip.AddrPort, error) {
	address, err := netip.ParseAddrPort(remote)
	if err != nil || address.Port() == 0 || address.Addr().Zone() != "" || !address.Addr().IsGlobalUnicast() || address.Addr().IsPrivate() || address.Addr().Is4In6() {
		return netip.AddrPort{}, errors.New("public TCP peer endpoint required")
	}
	return address, nil
}

func Scope(address netip.Addr) string {
	bits := 48
	if address.Is4() {
		bits = 24
	}
	return "tcp_peer:" + netip.PrefixFrom(address, bits).Masked().String()
}

func ValidateRequest(request Request) error {
	if request.Schema != Schema || len(request.Nonce) != 32 || request.EdgeID == "" || len(request.EdgeID) > 128 || request.GroupID == "" || len(request.GroupID) > 128 ||
		strings.ContainsAny(request.EdgeID+request.GroupID, " \t\r\n\x00") || (request.Slot != "a" && request.Slot != "b") {
		return errors.New("invalid Front observation identity")
	}
	if _, err := hex.DecodeString(request.Nonce); err != nil {
		return err
	}
	_, err := ParseRemote(request.RemoteAddr)
	return err
}

func Read(ctx context.Context, socketPath, edgeID, groupID, slot, remote string) (Response, error) {
	if !filepath.IsAbs(socketPath) || len(socketPath) > 100 {
		return Response{}, readFailure{"socket_path_invalid", errors.New("bounded absolute Front socket path required")}
	}
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	transport := &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return read(ctx, client, "http://front"+Path, "", edgeID, groupID, slot, remote, false)
}

func ReadAPI(ctx context.Context, client *http.Client, baseURL, token, edgeID, groupID, slot, remote string) (Response, error) {
	target, err := url.Parse(baseURL)
	if err != nil || client == nil || token == "" || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Scheme != "https" && target.Scheme != "http") {
		return Response{}, readFailure{"api_config_invalid", errors.New("invalid Front observation API")}
	}
	target.Path = strings.TrimSuffix(target.Path, "/") + "/v1/edge/network-observation"
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	reader := *client
	reader.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return read(ctx, &reader, target.String(), token, edgeID, groupID, slot, remote, true)
}

func read(ctx context.Context, client *http.Client, target, token, edgeID, groupID, slot, remote string, requireBackend bool) (Response, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return Response{}, readFailure{"nonce_failed", err}
	}
	query := Request{Schema: Schema, Nonce: hex.EncodeToString(nonce), EdgeID: edgeID, GroupID: groupID, Slot: slot, RemoteAddr: remote}
	if err := ValidateRequest(query); err != nil {
		return Response{}, readFailure{"request_invalid", err}
	}
	raw, _ := json.Marshal(query)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(raw))
	if err != nil {
		return Response{}, readFailure{"request_invalid", err}
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return Response{}, readFailure{"transport_failed", err}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		reason := "http_rejected"
		switch response.StatusCode {
		case http.StatusBadRequest:
			reason = "request_rejected"
		case http.StatusNotFound:
			reason = "connection_missing"
		case http.StatusTooManyRequests:
			reason = "rate_limited"
		case http.StatusServiceUnavailable:
			reason = "unavailable"
		}
		return Response{}, readFailure{reason, errors.New("Front connection evidence unavailable")}
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(payload) > 4096 {
		return Response{}, readFailure{"response_unreadable", errors.New("Front connection evidence exceeds bound")}
	}
	var result Response
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil || !json.Valid(payload) {
		return Response{}, readFailure{"response_invalid", errors.New("invalid Front connection response")}
	}
	peer, _ := ParseRemote(remote)
	now := time.Now().UTC()
	if result.Schema != Schema || result.Nonce != query.Nonce || result.EdgeID != edgeID || result.GroupID != groupID || result.Sample.Slot != slot ||
		result.Sample.Scope != Scope(peer.Addr()) || result.Sample.ObservedAt.After(now) || now.Sub(result.Sample.ObservedAt) > time.Second || model.ValidateEdgeClientNetworkSample(&result.Sample) != nil || requireBackend && result.Sample.Backend == nil {
		return Response{}, readFailure{"binding_mismatch", errors.New("Front connection binding mismatch")}
	}
	return result, nil
}
