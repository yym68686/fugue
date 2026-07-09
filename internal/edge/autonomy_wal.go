package edge

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

const edgeAutonomyWALInterval = time.Minute

func (s *Service) recordEdgeAutonomyWAL(action, safetyClass, subject, generation string, evidence map[string]string, expiresAt *time.Time, now time.Time) {
	if s == nil {
		return
	}
	walPath := strings.TrimSpace(s.Config.AutonomyWALPath)
	if walPath == "" {
		return
	}
	action = strings.TrimSpace(action)
	if action == "" {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	evidence = cloneStringMap(evidence)
	if _, ok := evidence["edge_id"]; !ok {
		evidence["edge_id"] = strings.TrimSpace(s.Config.EdgeID)
	}
	if _, ok := evidence["edge_group_id"]; !ok {
		evidence["edge_group_id"] = strings.TrimSpace(s.Config.EdgeGroupID)
	}
	if _, ok := evidence["cache_path"]; !ok && strings.TrimSpace(s.Config.CachePath) != "" {
		evidence["cache_path"] = strings.TrimSpace(s.Config.CachePath)
	}
	dedupeKey := edgeAutonomyWALDedupeKey(action, subject, generation, evidence)
	if !s.reserveEdgeAutonomyWAL(dedupeKey, now) {
		return
	}
	record, err := localwal.NewRecord("edge-worker", strings.TrimSpace(s.Config.EdgeID), action, evidence, generation, expiresAt, now)
	if err != nil {
		s.logAutonomyWALError(action, err)
		return
	}
	record.Subject = strings.TrimSpace(subject)
	record.SafetyClass = strings.TrimSpace(safetyClass)
	if err := localwal.Append(walPath, record); err != nil {
		s.logAutonomyWALError(action, err)
	}
}

func (s *Service) recordEdgeLKGWriteWAL(cached cacheFile) {
	generation := edgeCacheGeneration(cached.Bundle)
	if generation == "" {
		return
	}
	expiresAt := edgeBundleExpiresAt(cached.Bundle, cached.CachedAt, s.Config.MaxStale)
	evidence := map[string]string{
		"route_count":         fmt.Sprintf("%d", len(cached.Bundle.Routes)),
		"tls_allowlist_count": fmt.Sprintf("%d", len(cached.Bundle.TLSAllowlist)),
		"cached_at":           cached.CachedAt.UTC().Format(time.RFC3339),
	}
	if !cached.Bundle.ValidUntil.IsZero() {
		evidence["bundle_valid_until"] = cached.Bundle.ValidUntil.UTC().Format(time.RFC3339)
	}
	s.recordEdgeAutonomyWAL("lkg_write", "L0_observe_only", generation, generation, evidence, expiresAt, cached.CachedAt)
}

func (s *Service) recordEdgeServeLKGWAL(reason string, err error) {
	status := s.Status()
	generation := firstNonEmpty(status.ServingGeneration, status.LKGGeneration, status.BundleVersion)
	if generation == "" {
		return
	}
	now := time.Now().UTC()
	expiresAt := status.BundleValidUntil
	if expiresAt == nil && s.Config.MaxStale > 0 {
		value := now.Add(s.Config.MaxStale)
		expiresAt = &value
	}
	evidence := map[string]string{
		"reason":              strings.TrimSpace(reason),
		"status":              strings.TrimSpace(status.Status),
		"stale_cache":         edgeBoolString(status.StaleCache),
		"max_stale_exceeded":  edgeBoolString(status.MaxStaleExceeded),
		"route_count":         fmt.Sprintf("%d", status.RouteCount),
		"tls_allowlist_count": fmt.Sprintf("%d", status.TLSAllowlistCount),
		"caddy_enabled":       edgeBoolString(status.CaddyEnabled),
		"caddy_last_error":    strings.TrimSpace(status.CaddyLastError),
	}
	if err != nil {
		evidence["last_error"] = s.redact(err.Error())
	}
	s.recordEdgeAutonomyWAL("serve_lkg", "L0_observe_only", generation, generation, evidence, expiresAt, now)
}

func (s *Service) recordEdgeCaddyReloadWAL(reason string, err error) {
	status := s.Status()
	generation := firstNonEmpty(status.CaddyAppliedVersion, status.ServingGeneration, status.LKGGeneration, status.BundleVersion)
	if generation == "" {
		return
	}
	now := time.Now().UTC()
	evidence := map[string]string{
		"reason":                strings.TrimSpace(reason),
		"result":                "success",
		"caddy_enabled":         edgeBoolString(status.CaddyEnabled),
		"caddy_listen_addr":     strings.TrimSpace(status.CaddyListenAddr),
		"caddy_tls_mode":        strings.TrimSpace(status.CaddyTLSMode),
		"caddy_applied_version": strings.TrimSpace(status.CaddyAppliedVersion),
		"route_count":           fmt.Sprintf("%d", status.RouteCount),
	}
	if err != nil {
		evidence["result"] = "error"
		evidence["caddy_last_error"] = s.redact(err.Error())
	}
	s.recordEdgeAutonomyWAL("caddy_reload_lkg", "L2_local_reload", generation, generation, evidence, status.BundleValidUntil, now)
}

func (s *Service) reserveEdgeAutonomyWAL(key string, now time.Time) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	s.walMu.Lock()
	defer s.walMu.Unlock()
	if s.walActionLast == nil {
		s.walActionLast = map[string]time.Time{}
	}
	if last, ok := s.walActionLast[key]; ok && now.Sub(last) < edgeAutonomyWALInterval {
		return false
	}
	for existing, last := range s.walActionLast {
		if now.Sub(last) >= edgeAutonomyWALInterval {
			delete(s.walActionLast, existing)
		}
	}
	s.walActionLast[key] = now
	return true
}

func edgeAutonomyWALDedupeKey(action, subject, generation string, evidence map[string]string) string {
	return strings.Join([]string{
		strings.TrimSpace(action),
		strings.TrimSpace(subject),
		strings.TrimSpace(generation),
		strings.TrimSpace(evidence["reason"]),
		strings.TrimSpace(evidence["route_count"]),
		strings.TrimSpace(evidence["caddy_last_error"]),
	}, "|")
}

func (s *Service) logAutonomyWALError(action string, err error) {
	if s == nil || s.Logger == nil || err == nil {
		return
	}
	s.Logger.Printf("edge autonomy wal append failed; action=%s error=%s", strings.TrimSpace(action), s.redact(err.Error()))
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+3)
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	return out
}

func edgeBoolString(value bool) string {
	return fmt.Sprintf("%t", value)
}

func edgeBundleExpiresAt(bundle model.EdgeRouteBundle, cachedAt time.Time, maxStale time.Duration) *time.Time {
	if !bundle.ValidUntil.IsZero() {
		value := bundle.ValidUntil.UTC()
		if maxStale > 0 {
			value = value.Add(maxStale)
		}
		return &value
	}
	if cachedAt.IsZero() {
		cachedAt = time.Now().UTC()
	}
	if maxStale <= 0 {
		maxStale = 24 * time.Hour
	}
	value := cachedAt.UTC().Add(maxStale)
	return &value
}
