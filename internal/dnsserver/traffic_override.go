package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficoverride"
	miekgdns "github.com/miekg/dns"
)

const trafficOverrideFeedPath = "/v1/dns/traffic-overrides"

type trafficOverrideFeedResponse struct {
	Feed model.TrafficOverrideFeed `json:"feed"`
}

func (s *Service) startTrafficOverrideLoop(ctx context.Context) {
	if !s.Config.TrafficOverrideEnabled || s.trafficOverrideInterval() <= 0 {
		return
	}
	go func() {
		if err := s.syncTrafficOverrides(ctx); err != nil {
			s.logTrafficOverrideError(err)
		}
		ticker := time.NewTicker(s.trafficOverrideInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.syncTrafficOverrides(ctx); err != nil {
					s.logTrafficOverrideError(err)
				}
			}
		}
	}()
}

func (s *Service) trafficOverrideInterval() time.Duration {
	if s.Config.TrafficOverrideInterval <= 0 {
		return 5 * time.Second
	}
	return s.Config.TrafficOverrideInterval
}

func (s *Service) syncTrafficOverrides(ctx context.Context) error {
	if !s.Config.TrafficOverrideEnabled {
		return nil
	}
	req, err := s.newTrafficOverrideRequest(ctx)
	if err != nil {
		return err
	}
	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("fetch traffic override feed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("traffic override feed returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload trafficOverrideFeedResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return fmt.Errorf("decode traffic override feed: %w", err)
	}
	active, err := validateTrafficOverrideFeed(payload.Feed, time.Now().UTC(), s.Config.Zone)
	if err != nil {
		return err
	}
	s.overrideMu.Lock()
	s.overrides = active
	s.overrideGen = payload.Feed.Generation
	s.overrideMu.Unlock()
	return nil
}

func (s *Service) newTrafficOverrideRequest(ctx context.Context) (*http.Request, error) {
	base, err := url.Parse(strings.TrimRight(strings.TrimSpace(s.Config.APIURL), "/"))
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, errors.New("invalid FUGUE_API_URL")
	}
	base.Path = strings.TrimRight(base.Path, "/") + trafficOverrideFeedPath
	query := base.Query()
	query.Set("token", strings.TrimSpace(s.Config.EdgeToken))
	if value := strings.TrimSpace(s.Config.DNSNodeID); value != "" {
		query.Set("dns_node_id", value)
	}
	if value := strings.TrimSpace(s.Config.EdgeGroupID); value != "" {
		query.Set("edge_group_id", value)
	}
	if value := normalizeName(s.Config.Zone); value != "" {
		query.Set("zone", value)
	}
	base.RawQuery = query.Encode()
	return http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
}

func validateTrafficOverrideFeed(feed model.TrafficOverrideFeed, now time.Time, zone string) (map[string]model.TrafficOverride, error) {
	if feed.Schema != model.TrafficOverrideFeedSchemaV1 {
		return nil, fmt.Errorf("traffic override feed schema %q is unsupported", feed.Schema)
	}
	if feed.Generation == 0 || feed.SigningKey.CurrentKeyID == "" || feed.SigningKey.CurrentPublicKey == "" {
		return nil, errors.New("traffic override feed signing key is incomplete")
	}
	active := make(map[string]model.TrafficOverride, len(feed.Overrides))
	for _, override := range feed.Overrides {
		if override.State != model.TrafficOverrideStateStaged || !override.ExpiresAt.After(now) {
			continue
		}
		if zone != "" && !trafficOverrideNameWithinZone(override.Hostname, zone) {
			return nil, fmt.Errorf("traffic override hostname %q is outside zone", override.Hostname)
		}
		if err := trafficoverride.VerifyWithKeyring(override, model.TrafficOverrideSigningKeyring{
			Schema:            feed.SigningKey.Schema,
			Generation:        feed.SigningKey.Generation,
			CurrentKeyID:      feed.SigningKey.CurrentKeyID,
			CurrentPublicKey:  feed.SigningKey.CurrentPublicKey,
			PreviousKeyID:     feed.SigningKey.PreviousKeyID,
			PreviousPublicKey: feed.SigningKey.PreviousPublicKey,
		}); err != nil {
			return nil, fmt.Errorf("verify traffic override %q: %w", override.Hostname, err)
		}
		for _, answer := range override.Answers {
			if net.ParseIP(strings.TrimSpace(answer)) == nil {
				return nil, fmt.Errorf("traffic override %q contains an empty answer", override.Hostname)
			}
		}
		active[normalizeName(override.Hostname)] = override
	}
	return active, nil
}

func trafficOverrideNameWithinZone(name, zone string) bool {
	name = normalizeName(name)
	zone = normalizeName(zone)
	return name != "" && zone != "" && (name == zone || strings.HasSuffix(name, "."+zone))
}

func (s *Service) overlayRecords(name string, qtype uint16) []model.EdgeDNSRecord {
	if !s.Config.TrafficOverrideEnabled || (qtype != miekgdns.TypeA && qtype != miekgdns.TypeAAAA) {
		return nil
	}
	s.overrideMu.RLock()
	override, ok := s.overrides[normalizeName(name)]
	s.overrideMu.RUnlock()
	if !ok || !override.ExpiresAt.After(time.Now().UTC()) {
		return nil
	}
	values := make([]string, 0, len(override.Answers))
	for _, answer := range override.Answers {
		if qtype == miekgdns.TypeA && strings.Contains(answer, ":") {
			continue
		}
		if qtype == miekgdns.TypeAAAA && !strings.Contains(answer, ":") {
			continue
		}
		values = append(values, answer)
	}
	if len(values) == 0 {
		return nil
	}
	return []model.EdgeDNSRecord{{
		Name:             normalizeName(name),
		Type:             map[uint16]string{miekgdns.TypeA: "A", miekgdns.TypeAAAA: "AAAA"}[qtype],
		Values:           values,
		TTL:              s.ttl(),
		RecordKind:       "traffic_override",
		Status:           "active",
		RecordGeneration: override.RouteGeneration,
	}}
}

func (s *Service) logTrafficOverrideError(err error) {
	if err != nil && s.Logger != nil {
		s.Logger.Printf("dns traffic override sync failed; serving previous overlay: %s", s.redact(err.Error()))
	}
}
