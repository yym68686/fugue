package participation

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
)

const maxDNSObservationBytes = 4096

type DNSObserver struct {
	origin string
	token  string
	client *http.Client
}

// NewDNSObserver accepts only the topology's HTTPS origin and a
// purpose-issued token. It never follows redirects or uses an ambient proxy.
func NewDNSObserver(origin, token string) (*DNSObserver, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.Opaque != "" || u.User != nil ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.Contains(origin, "#") {
		return nil, errors.New("participation DNS observation requires an approved HTTPS origin")
	}
	token = strings.TrimSpace(token)
	if len(token) > 4096 || !strings.HasPrefix(token, "fugue_pc_v1.") || strings.ContainsAny(token, " \t\r\n") {
		return nil, errors.New("participation DNS observation requires a dedicated component token")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &DNSObserver{origin: origin, token: token, client: client}, nil
}

func (o *DNSObserver) Observe(ctx context.Context, node string) (model.EdgeParticipationObservation, error) {
	var result model.EdgeParticipationObservation
	if o == nil || o.client == nil || node == "" {
		return result, errors.New("participation DNS observation is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		o.origin+"/v1/edge/nodes/"+url.PathEscape(node)+"/participation-observation", nil)
	if err != nil {
		return result, errors.New("invalid participation DNS observation request")
	}
	req.Header.Set("Authorization", "Bearer "+o.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := o.client.Do(req)
	if err != nil {
		return result, errors.New("could not read the participation DNS observation endpoint")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, errors.New("participation DNS observation endpoint did not return a successful observation")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxDNSObservationBytes+1))
	if err != nil || len(raw) > maxDNSObservationBytes {
		return result, errors.New("participation DNS observation exceeds its response boundary")
	}
	// Require explicit fields, including a false heartbeat result. Missing
	// JSON fields must not be interpreted as negative evidence.
	var wire struct {
		model.EdgeParticipationObservation
		HeartbeatFresh *bool   `json:"heartbeat_fresh"`
		DNSReason      *string `json:"dns_reason"`
	}
	if decodeStrict(raw, &wire) != nil || wire.HeartbeatFresh == nil || wire.DNSReason == nil {
		return result, errors.New("participation DNS observation is incomplete or malformed")
	}
	result = wire.EdgeParticipationObservation
	result.HeartbeatFresh, result.DNSReason = *wire.HeartbeatFresh, *wire.DNSReason
	return result, nil
}

func bindDNSObservation(r model.EdgeParticipationObservation, node, group, address string, now time.Time) (*ParticipationTransport, error) {
	public, err := netip.ParseAddr(r.PublicIPv4)
	if err != nil || !public.Is4() || r.PublicIPv4 != address || r.Node != node || r.Group != group ||
		now.IsZero() || r.EvaluatedAt.IsZero() || r.EvaluatedAt.After(now.Add(5*time.Second)) || now.Sub(r.EvaluatedAt) > 15*time.Second {
		return nil, errors.New("participation DNS observation is stale or does not match the exact serving member")
	}
	if r.HeartbeatFresh && (r.FreshUntil == nil || !r.FreshUntil.After(now) || !r.FreshUntil.After(r.EvaluatedAt)) {
		return nil, errors.New("participation heartbeat reported fresh without current positive evidence")
	}
	switch r.DNSState {
	case "allowed":
		if r.DNSReason != "" || r.DNSServingEvidence != "not_evaluated" {
			return nil, errors.New("inconsistent allowed DNS observation")
		}
	case "suspended":
		if r.DNSReason == "" ||
			(r.DNSServingEvidence != "excluded_from_verified_full" && r.DNSServingEvidence != "excluded_across_full_transition") {
			return nil, errors.New("a DNS policy hold is not verified serving exclusion")
		}
	default:
		return nil, errors.New("participation DNS policy is not proven")
	}
	return &ParticipationTransport{Node: node, Group: group, PublicIPv4: r.PublicIPv4, ExternalIPs: []string{address},
		DNSState: r.DNSState, DNSReason: r.DNSReason, DNSServingEvidence: r.DNSServingEvidence, HeartbeatFresh: r.HeartbeatFresh}, nil
}

func sameDNSObservation(a, b model.EdgeParticipationObservation) bool {
	a.EvaluatedAt, b.EvaluatedAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(a, b)
}
