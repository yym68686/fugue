package edgecontrol

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/staticedgecontract"
)

// This capability has no implicit command, token projection, RBAC or production
// enablement. The file must contain a separately projected exclusive-audience
// bound Pod token; RouteIntent's reader credential/issuer is never exchanged.
type PendingRootClientConfig struct {
	Owner, Namespace, Endpoint, PodTokenFile, CAFile string
}

type PendingRootClient struct {
	owner, endpoint, tokenFile string
	client                     *http.Client
}

var pendingRootNamespace = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func NewPendingRootClient(config PendingRootClientConfig) (*PendingRootClient, error) {
	group := strings.TrimPrefix(config.Owner, "edge-control/")
	if !strings.HasPrefix(config.Owner, "edge-control/") || len(config.Owner) > 256 || !edgeGroupIDPattern.MatchString(group) || !pendingRootNamespace.MatchString(config.Namespace) {
		return nil, ErrGroupPendingRoot
	}
	host := "fugue-api-tls." + config.Namespace + ".svc"
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Scheme != "https" || u.Host != host+":8443" || u.User != nil || u.EscapedPath() != inputarchive.PendingRootHTTPPath || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, ErrGroupPendingRoot
	}
	for _, path := range []string{config.PodTokenFile, config.CAFile} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, ErrGroupPendingRoot
		}
	}
	if config.PodTokenFile == config.CAFile {
		return nil, ErrGroupPendingRoot
	}
	roots, err := loadRouteIntentCAPool(config.CAFile)
	if err != nil {
		return nil, ErrGroupPendingRoot
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: host, NextProtos: []string{"http/1.1"}},
		ForceAttemptHTTP2: false, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: time.Minute,
		IdleConnTimeout: 30 * time.Second, MaxIdleConns: 2, MaxIdleConnsPerHost: 2,
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	client := &http.Client{Transport: transport, Timeout: time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &PendingRootClient{owner: config.Owner, endpoint: u.String(), tokenFile: config.PodTokenFile, client: client}, nil
}

// One request, no retries. A timeout/lost ACK is completion-not-confirmed: the
// native caller preserves LKG and its mandatory failure audit, while any remote
// append-only root/reservation remains protected. A receipt is not a restore pin.
func (c *PendingRootClient) RetainPendingCompilerInputExternalRoot(ctx context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootReceipt, error) {
	var receipt inputarchive.PendingRootReceipt
	if c == nil || ctx == nil || c.client == nil || r.Registration.Owner != c.owner || r.SourceSchema != "fugue.edge-control.pending-root-source.v1" {
		return receipt, ErrGroupPendingRoot
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	raw, err := inputarchive.MarshalPendingRootHTTP(r)
	if err != nil || ctx.Err() != nil {
		return receipt, ErrGroupPendingRoot
	}
	secret, err := readPrivateProjectedFile(c.tokenFile, 32768)
	if err != nil {
		return receipt, ErrGroupPendingRoot
	}
	token := strings.TrimSpace(string(secret))
	// Syntax/audience preflight only; signature and exact live owner are checked
	// by server-side TokenReview, never by trusting this unsigned JWT payload.
	if !inputarchive.PodTokenHasExclusiveAudience(token, inputarchive.PendingRootAudience) || ctx.Err() != nil {
		return receipt, ErrGroupPendingRoot
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(raw))
	if err != nil {
		return receipt, ErrGroupPendingRoot
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		return receipt, ErrGroupPendingRoot
	}
	defer response.Body.Close()
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || response.StatusCode != http.StatusOK || response.Header.Get("Cache-Control") != "no-store" {
		return receipt, ErrGroupPendingRoot
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (16<<10)+1))
	if err != nil || len(body) > 16<<10 || staticedgecontract.StrictJSON(body, &receipt) != nil || !receipt.Matches(r.Registration) || ctx.Err() != nil {
		return inputarchive.PendingRootReceipt{}, ErrGroupPendingRoot
	}
	return receipt, nil
}
