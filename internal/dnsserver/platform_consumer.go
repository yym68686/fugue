package dnsserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	dns "github.com/miekg/dns"
)

// This is a staged candidate, never a serving LKG or an apply-success receipt.
type PlatformCandidateStatus struct {
	Query             *DNSQueryStatus     `json:"query,omitempty"`
	Readiness         *DNSReadinessStatus `json:"readiness,omitempty"`
	State             string              `json:"state"`
	ArtifactID        string              `json:"artifact_id,omitempty"`
	Digest            string              `json:"digest,omitempty"`
	ReleaseSetID      string              `json:"release_set_id,omitempty"`
	RecordCount       int                 `json:"record_count"`
	Sequence          int64               `json:"sequence,omitempty"`
	VerifiedAt        time.Time           `json:"verified_at,omitempty"`
	ReportedAt        time.Time           `json:"reported_at,omitempty"`
	LastError         string              `json:"last_error,omitempty"`
	ConsumerViewCount int                 `json:"consumer_view_count"`
	ProbeRecordCount  int                 `json:"probe_record_count"`
}

type dnsPlatformCandidate struct {
	Artifact   model.PlatformArtifact           `json:"artifact"`
	Assignment model.PlatformConsumerAssignment `json:"assignment"`
	Release    model.PlatformArtifactRelease    `json:"release"`
	Sequence   int64                            `json:"sequence"`
	VerifiedAt time.Time                        `json:"verified_at"`
}

func (s *Service) runPlatformShadowConsumer(ctx context.Context) {
	if s.PlatformTokenFile == "" {
		return
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.SyncPlatformShadowOnce(ctx); err != nil && ctx.Err() == nil {
			s.mu.Lock()
			s.platformCandidate.State = "failed"
			s.platformCandidate.LastError = err.Error()
			s.mu.Unlock()
			s.Logger.Printf("dns platform candidate failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// SyncPlatformShadowOnce validates a real assigned artifact and durably stages
// it. The active DNS bundle and its cache are never written by this method.
func (s *Service) SyncPlatformShadowOnce(ctx context.Context) error {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient}
	identity, assignment, artifact, release, err := client.Sync(ctx, model.PlatformConsumerComponentDNSServer, s.Config.DNSNodeID, "global", model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return err
	}
	chosen := &assignment
	candidate := dnsPlatformCandidate{Artifact: artifact, Assignment: assignment, Release: release}
	counts, err := s.verifyPlatformDNSCandidate(candidate, *chosen)
	if err != nil {
		return err
	}
	readiness, err := s.observePlatformDNSReadiness(ctx, candidate, *chosen)
	if err != nil {
		return err
	}
	query, err := s.evaluatePlatformDNSQueries(candidate, *chosen, readiness)
	if err != nil {
		return err
	}
	if err = client.CheckAssignment(ctx, identity, assignment); err != nil {
		return err
	}
	// Persist a monotonic cursor before sending it. A lost response consumes the
	// sequence; a restart cannot replay it. Corrupt state is never reset silently.
	cachePath := s.Config.CachePath + ".platform-shadow.json"
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("platform candidate cache path is required")
	}
	var previous dnsPlatformCandidate
	if raw, readErr := platformconsumer.ReadFile(cachePath, 8<<20); readErr == nil {
		if json.Unmarshal(raw, &previous) != nil || previous.Sequence <= 0 || previous.Sequence == math.MaxInt64 {
			return errors.New("platform candidate cursor is corrupt")
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return errors.New("platform candidate cursor unavailable")
	}
	candidate.Sequence = max(previous.Sequence+1, time.Now().UnixNano())
	candidate.VerifiedAt = time.Now().UTC()
	raw, err := json.Marshal(candidate)
	if err != nil {
		return errors.New("encode platform candidate failed")
	}
	if err = lkgcache.AtomicWriteFile(cachePath, raw, 0600); err != nil {
		return errors.New("persist platform candidate failed")
	}
	if readiness != nil {
		readiness.Facts = sortedDNSReadinessFacts(readiness.Facts)
		raw, err := json.Marshal(readiness)
		if err != nil {
			return errors.New("encode DNS readiness facts failed")
		}
		if err = lkgcache.AtomicWriteFile(s.Config.CachePath+".platform-dns-readiness.json", raw, 0600); err != nil {
			return errors.New("persist DNS readiness facts failed")
		}
	}
	if query != nil {
		raw, err := json.Marshal(query)
		if err != nil {
			return errors.New("encode DNS query receipt failed")
		}
		if err = lkgcache.AtomicWriteFile(s.Config.CachePath+".platform-query-shadow.json", raw, 0600); err != nil {
			return errors.New("persist DNS query receipt failed")
		}
	}
	status := s.Status()
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return errors.New("platform heartbeat nonce unavailable")
	}
	heartbeat := platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ConsumerID: identity.Component + ":" + identity.NodeID, Component: identity.Component, NodeID: identity.NodeID,
		ArtifactKind: chosen.ArtifactKind, ScopeKey: chosen.ScopeKey, ReleaseSetID: chosen.ReleaseSetID, ExpectedConsumerSetID: chosen.ExpectedConsumerSetID,
		FencingToken: chosen.FencingToken, ProtocolVersion: model.PlatformConsumerProtocolVersionV1, SchemaVersion: model.PlatformConsumerSchemaVersionV1,
		Sequence: candidate.Sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: chosen.GenerationSequence,
		DesiredGeneration: chosen.ExpectedGeneration, ActualGeneration: status.ServingGeneration, LKGGeneration: status.LKGGeneration,
		ApplyStatus: "staged", ProbeStatus: "shadow_validated", ServingLKG: status.StaleCache, LKGExpired: status.MaxStaleExceeded,
	}
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		return errors.New("encode platform heartbeat evidence failed")
	}
	s.mu.Lock()
	s.platformCandidate = PlatformCandidateStatus{State: "shadow_verified", ArtifactID: chosen.ArtifactID, Digest: chosen.ContentHash, ReleaseSetID: chosen.ReleaseSetID, RecordCount: counts.records, ConsumerViewCount: counts.views, ProbeRecordCount: counts.probes, Sequence: candidate.Sequence, VerifiedAt: candidate.VerifiedAt}
	if readiness != nil {
		s.platformCandidate.Readiness = &readiness.Status
	}
	if query != nil {
		s.platformCandidate.Query = &query.Status
	}
	s.mu.Unlock()
	var receipt model.PlatformConsumerHeartbeatResponse
	if err = client.PostJSON(ctx, "/v1/platform-state/consumers/trusted-heartbeat", identity.Token, heartbeat, &receipt); err != nil {
		return err
	}
	if !receipt.Consumer.IdentityVerified || receipt.Consumer.ConsumerID != heartbeat.ConsumerID || receipt.Consumer.Sequence != heartbeat.Sequence || receipt.Consumer.ExpectedConsumerSetID != chosen.ExpectedConsumerSetID || receipt.Consumer.EvidenceHash != heartbeat.EvidenceHash {
		return errors.New("platform heartbeat receipt mismatch")
	}
	s.mu.Lock()
	s.platformCandidate.ReportedAt = time.Now().UTC()
	s.mu.Unlock()
	s.Logger.Printf("dns platform candidate verified; artifact=%s digest=%s records=%d sequence=%d serving=%s", chosen.ArtifactID, chosen.ContentHash, counts.records, candidate.Sequence, status.ServingGeneration)
	return nil
}

type dnsCandidateCounts struct{ records, views, probes int }

func (s *Service) verifyPlatformDNSCandidate(c dnsPlatformCandidate, a model.PlatformConsumerAssignment) (dnsCandidateCounts, error) {
	if !reflect.DeepEqual(a, c.Assignment) || a.ExpectedConsumerSetID == "" ||
		a.FencingToken <= 0 || a.GenerationSequence <= 0 || a.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow {
		return dnsCandidateCounts{}, errors.New("DNS candidate assignment mismatch")
	}
	artifact := c.Artifact
	if artifact.ID != a.ArtifactID || artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle ||
		artifact.ScopeKey != a.ScopeKey || artifact.Generation != a.ExpectedGeneration ||
		artifact.ContentHash != a.ContentHash || artifact.GenerationSequence != a.GenerationSequence ||
		artifact.Status != model.PlatformArtifactStatusValidated {
		return dnsCandidateCounts{}, errors.New("DNS candidate artifact binding mismatch")
	}
	if c.Release.ID != a.ArtifactReleaseID || c.Release.ArtifactID != a.ReleaseSetID ||
		c.Release.ArtifactKind != model.PlatformArtifactKindReleaseSet ||
		artifact.Metadata["release_set_generation"] != c.Release.Generation ||
		c.Release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		c.Release.FencingToken != a.FencingToken || c.Release.Status != model.PlatformArtifactReleaseStatusActive {
		return dnsCandidateCounts{}, errors.New("DNS candidate release binding mismatch")
	}
	if !platformsafety.EvaluateArtifactIntegrity(c.Artifact, bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)).Pass {
		return dnsCandidateCounts{}, errors.New("DNS candidate signature or digest rejected")
	}
	var payload struct {
		Schema        string                           `json:"schema_version"`
		Generation    string                           `json:"generation"`
		Records       []platformconfig.DNSIntent       `json:"records"`
		ConsumerViews []platformconfig.DNSConsumerView `json:"consumer_views,omitempty"`
		ReadinessPlan *platformconfig.DNSReadinessPlan `json:"readiness_plan,omitempty"`
		QueryViews    []platformconfig.DNSQueryView    `json:"query_views,omitempty"`
		Policy        platformconfig.PolicySnapshot    `json:"policy"`
		Lineage       platformconfig.Lineage           `json:"lineage"`
	}
	raw, err := json.Marshal(c.Artifact.Content)
	if err != nil {
		return dnsCandidateCounts{}, errors.New("DNS candidate content invalid")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&payload) != nil || payload.Schema != platformconfig.SchemaVersion || payload.Generation != c.Artifact.Metadata["intent_generation"] || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return dnsCandidateCounts{}, errors.New("DNS candidate schema invalid")
	}
	owners := map[string]bool{}
	consumers := []platformconfig.DNSConsumerIntent{}
	for _, v := range payload.ConsumerViews {
		if !owners[v.NodeID] {
			consumers = append(consumers, platformconfig.DNSConsumerIntent{NodeID: v.NodeID})
			owners[v.NodeID] = true
		}
	}
	if err := platformconfig.ValidateDNSClientPolicyOwnership(payload.Policy.DNSClientPolicies, consumers); err != nil {
		return dnsCandidateCounts{}, err
	}
	if err := platformconfig.ValidateDNSIntents(payload.Records); err != nil {
		return dnsCandidateCounts{}, errors.New("DNS candidate intent semantics invalid")
	}
	policyDigest, err := platformconfig.Digest(payload.Policy)
	if err != nil || payload.Policy.Scope != a.ScopeKey || payload.Lineage.PolicyDigest != policyDigest || payload.Lineage.IntentGeneration != payload.Generation || payload.Lineage.PolicyGeneration != payload.Policy.Generation {
		return dnsCandidateCounts{}, errors.New("DNS candidate policy lineage invalid")
	}
	for key, value := range platformconfig.LineageMetadata(payload.Lineage) {
		if value == "" || c.Artifact.Metadata[key] != value {
			return dnsCandidateCounts{}, errors.New("DNS candidate lineage binding invalid")
		}
	}
	if err := platformconfig.ValidateDNSReadinessPlan(payload.ReadinessPlan, payload.Policy.DNSReadiness); err != nil {
		return dnsCandidateCounts{}, errors.New("DNS readiness plan invalid")
	}
	if err := platformconfig.ValidateDNSQueryViews(payload.QueryViews, payload.Records, payload.ConsumerViews, payload.ReadinessPlan, payload.Policy); err != nil {
		return dnsCandidateCounts{}, errors.New("DNS query view invalid")
	}
	active, err := platformconfig.DNSRecordsAt(payload.Records, time.Now().UTC())
	if err != nil {
		return dnsCandidateCounts{}, errors.New("DNS candidate expiry invalid")
	}
	counts := dnsCandidateCounts{records: len(active)}
	wireRecords := active
	if len(payload.ConsumerViews) > 0 {
		wireRecords = nil
		if err := platformconfig.ValidateDNSConsumerViews(payload.ConsumerViews, payload.Records); err != nil {
			return dnsCandidateCounts{}, errors.New("DNS consumer views invalid")
		}
		foundPrimary := false
		for _, view := range payload.ConsumerViews {
			if view.NodeID != s.Config.DNSNodeID {
				continue
			}
			materialized, viewErr := platformconfig.MaterializeDNSConsumerView(payload.Records, payload.ConsumerViews, s.Config.DNSNodeID, s.Config.EdgeGroupID, view.Zone, time.Now().UTC())
			if viewErr != nil {
				return dnsCandidateCounts{}, errors.New("DNS signed consumer view binding rejected")
			}
			if view.Zone == s.Config.Zone {
				foundPrimary = true
			}
			counts.views++
			counts.probes += len(view.Records)
			// Validate all owned zones with the actual record wire encoder below.
			wireRecords = append(wireRecords, materialized...)
		}
		if !foundPrimary {
			return dnsCandidateCounts{}, errors.New("DNS consumer has no assigned primary zone")
		}
	}
	for _, record := range wireRecords {
		if record.Hostname == "" || record.TTL <= 0 || record.TTL > 2147483647 || len(record.Values) == 0 {
			return dnsCandidateCounts{}, errors.New("DNS candidate record is incomplete")
		}
		rrs := rrForEdgeDNSRecord(model.EdgeDNSRecord{Name: record.Hostname, Type: record.Type, Values: record.Values, TTL: record.TTL}, record.Hostname)
		if len(rrs) != len(record.Values) {
			return dnsCandidateCounts{}, errors.New("DNS candidate record cannot be decoded")
		}
		response := new(dns.Msg)
		response.Answer = rrs
		if _, err := response.Pack(); err != nil {
			return dnsCandidateCounts{}, errors.New("DNS candidate record cannot be encoded")
		}
	}
	return counts, nil
}
