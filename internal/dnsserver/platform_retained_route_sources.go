package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"time"

	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformsafety"
)

type dnsRetainedRouteSourceReader struct {
	client   platformconsumer.Client
	identity platformconsumer.Identity
}

// Refresh source facts under the retained DNS artifact's own LKG authority.
// Persist the authenticated source context only after positive snapshot checks,
// preserving the exact DNS identity, original checkpoint age and previous file.
// This never acknowledges the pending candidate or renews its configuration.
func (s *Service) refreshRetainedDNSRouteSources(ctx context.Context, old *dnsServingState, probe dnsReadinessProbeFunc, reason string, reader dnsRetainedRouteSourceReader) (state *dnsServingState, refreshErr error) {
	defer func() {
		if refreshErr != nil && s.Logger != nil {
			s.Logger.Printf("DNS retained source refresh rejected artifact_id=%s release_id=%s: %v", old.record.Candidate.Artifact.ID, old.record.Candidate.Release.ID, refreshErr)
		}
	}()
	var grant *model.PlatformLKGSnapshot
	read := func(ctx context.Context) (model.PlatformDNSRouteSourceSnapshot, error) {
		result, err := reader.client.RetainedDNSRouteSources(ctx, reader.identity, old.record.Candidate.Assignment, old.record.Candidate.Release)
		if err != nil {
			return result.Snapshot, err
		}
		if !platformsafety.EvaluatePlatformLKGSnapshot(result.RetainedLKG, old.record.Parent, s.platformDNSKeys(), time.Now().UTC()).Pass {
			return result.Snapshot, errors.New("retained DNS LKG authorization invalid")
		}
		if grant != nil && !reflect.DeepEqual(*grant, result.RetainedLKG) {
			return result.Snapshot, platformconsumer.ErrAssignmentChanged
		}
		grant = &result.RetainedLKG
		return result.Snapshot, nil
	}
	payload, facts, err := s.observeDNSRouteSourcesWithReader(ctx, old.record.Candidate, old.payload, old.record.RouteSources, probe, read)
	if err != nil {
		return nil, err
	}
	if payload.routeSources == nil || grant == nil {
		return nil, errors.New("retained DNS source context missing")
	}
	payload.routeSources.RetainedDNSLKG = grant
	now := time.Now().UTC()
	boundDNSRouteSourceFacts(payload, facts, now)
	record := old.record
	record.RouteSources = payload.routeSources
	if err := s.signDNSCheckpoint(&record); err != nil {
		return nil, err
	}
	st, err := buildDNSServingState(record, payload, old.routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, now)
	if err != nil {
		return nil, err
	}
	st.fallback = reason
	// Per-record negative facts must not block unrelated records from observing
	// their authorized new source. Such a view is volatile; only a completely
	// positive snapshot may replace the durable positive source checkpoint.
	if !dnsServingReady(st, time.Now().UTC()) || probeDNSServingSnapshot(st) != nil {
		return st, nil
	}
	next, err := read(ctx)
	if err != nil {
		return nil, err
	}
	if next.SelectionDigest != payload.routeSources.Snapshot.SelectionDigest {
		return nil, platformconsumer.ErrAssignmentChanged
	}
	path := strings.TrimSpace(s.Config.CachePath) + ".platform-serving.json"
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return nil, errors.New("DNS serving cache path unavailable")
	}
	raw, err := json.Marshal(st.record)
	if err != nil {
		return nil, err
	}
	if err = lkgcache.PreservePrevious(path, func(data []byte) bool { _, decodeErr := s.decodeDNSCheckpoint(data); return decodeErr == nil }); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err = lkgcache.AtomicWriteFile(path, raw, 0600); err != nil {
		return nil, err
	}
	return st, nil
}
