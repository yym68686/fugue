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
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficoverride"
	miekgdns "github.com/miekg/dns"
)

const (
	trafficOverrideFeedPath   = "/v1/dns/traffic-overrides"
	trafficOverrideFeedSchema = "traffic-override-feed.fugue.dev/v1"
)

type trafficOverrideSettings struct {
	enabled  bool
	interval time.Duration
	path     string
}

type trafficOverrideFeed struct {
	Schema      string                                `json:"schema"`
	Generation  uint64                                `json:"generation"`
	GeneratedAt time.Time                             `json:"generated_at"`
	Overrides   []model.TrafficOverride               `json:"overrides"`
	SigningKey  model.TrafficOverrideSigningKeyStatus `json:"signing_key"`
}

type trafficOverrideFeedResponse struct {
	Feed trafficOverrideFeed `json:"feed"`
}

type trafficOverrideCacheFile struct {
	Schema     string                                `json:"schema"`
	Generation uint64                                `json:"generation"`
	SavedAt    time.Time                             `json:"saved_at"`
	Overrides  []model.TrafficOverride               `json:"overrides"`
	SigningKey model.TrafficOverrideSigningKeyStatus `json:"signing_key"`
}

func trafficOverrideSettingsFromEnv() trafficOverrideSettings {
	interval := 5 * time.Second
	if raw := strings.TrimSpace(os.Getenv("FUGUE_DNS_TRAFFIC_OVERRIDE_INTERVAL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			interval = parsed
		}
	}
	path := strings.TrimSpace(os.Getenv("FUGUE_DNS_TRAFFIC_OVERRIDE_PATH"))
	if path == "" {
		path = "/var/lib/fugue/dns/traffic-override.json"
	}
	enabled := strings.EqualFold(strings.TrimSpace(os.Getenv("FUGUE_DNS_TRAFFIC_OVERRIDE_ENABLED")), "true")
	return trafficOverrideSettings{enabled: enabled, interval: interval, path: path}
}

func (s *Service) startTrafficOverrideLoop(ctx context.Context) {
	if !s.override.enabled || s.trafficOverrideInterval() <= 0 {
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
				s.activatePrepared(time.Now().UTC())
			}
		}
	}()
}

func (s *Service) trafficOverrideInterval() time.Duration {
	if s.override.interval <= 0 {
		return 5 * time.Second
	}
	return s.override.interval
}

func (s *Service) syncTrafficOverrides(ctx context.Context) error {
	if !s.override.enabled {
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
	if err := s.persistTrafficOverrideCache(payload.Feed, active); err != nil {
		return err
	}
	s.overrideMu.Lock()
	s.prepared = active
	s.overrideGen = payload.Feed.Generation
	s.activatePreparedLocked(time.Now().UTC())
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

func validateTrafficOverrideFeed(feed trafficOverrideFeed, now time.Time, zone string) (map[string]model.TrafficOverride, error) {
	if feed.Schema != trafficOverrideFeedSchema {
		return nil, fmt.Errorf("traffic override feed schema %q is unsupported", feed.Schema)
	}
	if feed.Generation == 0 || feed.SigningKey.CurrentKeyID == "" || feed.SigningKey.CurrentPublicKey == "" {
		return nil, errors.New("traffic override feed signing key is incomplete")
	}
	keyring := model.TrafficOverrideSigningKeyring{
		Schema:            feed.SigningKey.Schema,
		Generation:        feed.SigningKey.Generation,
		CurrentKeyID:      feed.SigningKey.CurrentKeyID,
		CurrentPublicKey:  feed.SigningKey.CurrentPublicKey,
		PreviousKeyID:     feed.SigningKey.PreviousKeyID,
		PreviousPublicKey: feed.SigningKey.PreviousPublicKey,
	}
	active := make(map[string]model.TrafficOverride, len(feed.Overrides))
	for _, override := range feed.Overrides {
		if override.State != model.TrafficOverrideStateStaged || !override.ExpiresAt.After(now) {
			continue
		}
		if override.ActivateAt.IsZero() || !override.ActivateAt.Before(override.ExpiresAt) {
			return nil, fmt.Errorf("traffic override %q has invalid activate_at", override.Hostname)
		}
		if zone != "" && !trafficOverrideNameWithinZone(override.Hostname, zone) {
			return nil, fmt.Errorf("traffic override hostname %q is outside zone", override.Hostname)
		}
		if err := trafficoverride.VerifyWithKeyring(override, keyring); err != nil {
			return nil, fmt.Errorf("verify traffic override %q: %w", override.Hostname, err)
		}
		for _, answer := range override.Answers {
			if net.ParseIP(strings.TrimSpace(answer)) == nil {
				return nil, fmt.Errorf("traffic override %q contains invalid answer", override.Hostname)
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

func (s *Service) persistTrafficOverrideCache(feed trafficOverrideFeed, active map[string]model.TrafficOverride) error {
	path := strings.TrimSpace(s.override.path)
	if path == "" {
		return errors.New("traffic override cache path is empty")
	}
	entries := make([]model.TrafficOverride, 0, len(active))
	for _, override := range active {
		entries = append(entries, override)
	}
	cache := trafficOverrideCacheFile{
		Schema:     feed.Schema,
		Generation: feed.Generation,
		SavedAt:    time.Now().UTC(),
		Overrides:  entries,
		SigningKey: feed.SigningKey,
	}
	data, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("encode traffic override cache: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create traffic override cache directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".traffic-override-*.tmp")
	if err != nil {
		return fmt.Errorf("create traffic override cache temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod traffic override cache temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write traffic override cache: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync traffic override cache: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close traffic override cache temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("activate traffic override cache: %w", err)
	}
	dirHandle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open traffic override cache directory: %w", err)
	}
	defer dirHandle.Close()
	if err := dirHandle.Sync(); err != nil {
		return fmt.Errorf("fsync traffic override cache directory: %w", err)
	}
	return nil
}

func (s *Service) loadTrafficOverrideCache() error {
	data, err := os.ReadFile(strings.TrimSpace(s.override.path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var cache trafficOverrideCacheFile
	if err := json.Unmarshal(data, &cache); err != nil {
		return fmt.Errorf("decode traffic override cache: %w", err)
	}
	active, err := validateTrafficOverrideFeed(trafficOverrideFeed{
		Schema:      cache.Schema,
		Generation:  cache.Generation,
		GeneratedAt: cache.SavedAt,
		Overrides:   cache.Overrides,
		SigningKey:  cache.SigningKey,
	}, time.Now().UTC(), s.Config.Zone)
	if err != nil {
		return err
	}
	s.overrideMu.Lock()
	s.prepared = active
	s.overrideGen = cache.Generation
	s.activatePreparedLocked(time.Now().UTC())
	s.overrideMu.Unlock()
	return nil
}

func (s *Service) activatePrepared(now time.Time) {
	s.overrideMu.Lock()
	s.activatePreparedLocked(now.UTC())
	s.overrideMu.Unlock()
}

// activatePreparedLocked keeps the currently serving overlay until the
// prepared artifact's activation time. A node that misses a future feed never
// replaces a positive LKG with an empty answer.
func (s *Service) activatePreparedLocked(now time.Time) {
	active := make(map[string]model.TrafficOverride, len(s.overrides)+len(s.prepared))
	for hostname, override := range s.overrides {
		active[hostname] = override
	}
	for hostname, override := range s.prepared {
		if !override.ExpiresAt.After(now) {
			delete(active, hostname)
			continue
		}
		if !override.ActivateAt.After(now) {
			active[hostname] = override
		}
	}
	for hostname := range active {
		if _, prepared := s.prepared[hostname]; !prepared {
			delete(active, hostname)
		}
	}
	s.overrides = active
}

func (s *Service) overlayRecords(name string, qtype uint16) []model.EdgeDNSRecord {
	if !s.override.enabled || (qtype != miekgdns.TypeA && qtype != miekgdns.TypeAAAA) {
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
