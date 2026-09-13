package dnsserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	dns "github.com/miekg/dns"
)

// This is a staged candidate, never a serving LKG or an apply-success receipt.
type PlatformCandidateStatus struct {
	State        string    `json:"state"`
	ArtifactID   string    `json:"artifact_id,omitempty"`
	Digest       string    `json:"digest,omitempty"`
	ReleaseSetID string    `json:"release_set_id,omitempty"`
	RecordCount  int       `json:"record_count"`
	Sequence     int64     `json:"sequence,omitempty"`
	VerifiedAt   time.Time `json:"verified_at,omitempty"`
	ReportedAt   time.Time `json:"reported_at,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
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
	base, err := url.Parse(s.Config.APIURL)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" || base.User != nil {
		return errors.New("platform API endpoint is invalid")
	}
	base.RawQuery = ""
	base.Fragment = ""
	base.Path = strings.TrimRight(base.Path, "/")
	podToken, err := readPlatformCandidateFile(s.PlatformTokenFile, 32768)
	if err != nil {
		return errors.New("platform Pod credential unavailable")
	}
	var identity struct {
		Token         string    `json:"token"`
		ExpiresAt     time.Time `json:"expires_at"`
		Component     string    `json:"component"`
		NodeID        string    `json:"node_id"`
		ScopeKey      string    `json:"scope_key"`
		ArtifactKinds []string  `json:"artifact_kinds"`
	}
	if err = s.platformJSON(ctx, base.String()+"/v1/platform-state/consumers/identity", strings.TrimSpace(string(podToken)), http.MethodPost, nil, &identity); err != nil {
		return err
	}
	if identity.Token == "" || identity.Component != model.PlatformConsumerComponentDNSServer || identity.NodeID != s.Config.DNSNodeID || !identity.ExpiresAt.After(time.Now().Add(10*time.Second)) {
		return errors.New("platform credential identity mismatch")
	}
	var assignments model.PlatformConsumerAssignmentResponse
	if err = s.platformJSON(ctx, base.String()+"/v1/platform-state/consumers/assignment", identity.Token, http.MethodGet, nil, &assignments); err != nil {
		return err
	}
	var chosen *model.PlatformConsumerAssignment
	for i := range assignments.Assignments {
		a := &assignments.Assignments[i]
		if a.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle && a.ScopeKey == identity.ScopeKey && a.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
			if chosen != nil {
				return errors.New("ambiguous DNS shadow assignment")
			}
			chosen = a
		}
	}
	if chosen == nil {
		return errors.New("DNS shadow assignment unavailable")
	}
	var candidate dnsPlatformCandidate
	endpoint := base.String() + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(chosen.ArtifactID) + "?expected_consumer_set_id=" + url.QueryEscape(chosen.ExpectedConsumerSetID)
	if err = s.platformJSON(ctx, endpoint, identity.Token, http.MethodGet, nil, &candidate); err != nil {
		return err
	}
	count, err := s.verifyPlatformDNSCandidate(candidate, *chosen)
	if err != nil {
		return err
	}
	// Persist a monotonic cursor before sending it. A lost response consumes the
	// sequence; a restart cannot replay it. Corrupt state is never reset silently.
	cachePath := s.Config.CachePath + ".platform-shadow.json"
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("platform candidate cache path is required")
	}
	var previous dnsPlatformCandidate
	if raw, readErr := readPlatformCandidateFile(cachePath, 8<<20); readErr == nil {
		if json.Unmarshal(raw, &previous) != nil || previous.Sequence <= 0 {
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
	s.platformCandidate = PlatformCandidateStatus{State: "shadow_verified", ArtifactID: chosen.ArtifactID, Digest: chosen.ContentHash, ReleaseSetID: chosen.ReleaseSetID, RecordCount: count, Sequence: candidate.Sequence, VerifiedAt: candidate.VerifiedAt}
	s.mu.Unlock()
	var receipt model.PlatformConsumerHeartbeatResponse
	if err = s.platformJSON(ctx, base.String()+"/v1/platform-state/consumers/trusted-heartbeat", identity.Token, http.MethodPost, heartbeat, &receipt); err != nil {
		return err
	}
	if !receipt.Consumer.IdentityVerified || receipt.Consumer.ConsumerID != heartbeat.ConsumerID || receipt.Consumer.Sequence != heartbeat.Sequence || receipt.Consumer.ExpectedConsumerSetID != chosen.ExpectedConsumerSetID || receipt.Consumer.EvidenceHash != heartbeat.EvidenceHash {
		return errors.New("platform heartbeat receipt mismatch")
	}
	s.mu.Lock()
	s.platformCandidate.ReportedAt = time.Now().UTC()
	s.mu.Unlock()
	s.Logger.Printf("dns platform candidate verified; artifact=%s digest=%s records=%d sequence=%d serving=%s", chosen.ArtifactID, chosen.ContentHash, count, candidate.Sequence, status.ServingGeneration)
	return nil
}

func (s *Service) verifyPlatformDNSCandidate(c dnsPlatformCandidate, a model.PlatformConsumerAssignment) (int, error) {
	if !reflect.DeepEqual(a, c.Assignment) || a.ExpectedConsumerSetID == "" ||
		a.FencingToken <= 0 || a.GenerationSequence <= 0 || a.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow {
		return 0, errors.New("DNS candidate assignment mismatch")
	}
	artifact := c.Artifact
	if artifact.ID != a.ArtifactID || artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle ||
		artifact.ScopeKey != a.ScopeKey || artifact.Generation != a.ExpectedGeneration ||
		artifact.ContentHash != a.ContentHash || artifact.GenerationSequence != a.GenerationSequence ||
		artifact.Status != model.PlatformArtifactStatusValidated {
		return 0, errors.New("DNS candidate artifact binding mismatch")
	}
	if c.Release.ID != a.ArtifactReleaseID || c.Release.ArtifactID != a.ReleaseSetID ||
		c.Release.ArtifactKind != model.PlatformArtifactKindReleaseSet ||
		artifact.Metadata["release_set_generation"] != c.Release.Generation ||
		c.Release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		c.Release.FencingToken != a.FencingToken || c.Release.Status != model.PlatformArtifactReleaseStatusActive {
		return 0, errors.New("DNS candidate release binding mismatch")
	}
	if !platformsafety.EvaluateArtifactIntegrity(c.Artifact, bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)).Pass {
		return 0, errors.New("DNS candidate signature or digest rejected")
	}
	var payload struct {
		Schema     string                        `json:"schema_version"`
		Generation string                        `json:"generation"`
		Records    []platformconfig.DNSIntent    `json:"records"`
		Policy     platformconfig.PolicySnapshot `json:"policy"`
		Lineage    platformconfig.Lineage        `json:"lineage"`
	}
	raw, err := json.Marshal(c.Artifact.Content)
	if err != nil {
		return 0, errors.New("DNS candidate content invalid")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&payload) != nil || payload.Schema != platformconfig.SchemaVersion || payload.Generation != c.Artifact.Metadata["intent_generation"] || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return 0, errors.New("DNS candidate schema invalid")
	}
	for _, record := range payload.Records {
		if record.Hostname == "" || record.TTL <= 0 || record.TTL > 2147483647 || len(record.Values) == 0 {
			return 0, errors.New("DNS candidate record is incomplete")
		}
		rrs := rrForEdgeDNSRecord(model.EdgeDNSRecord{Name: record.Hostname, Type: record.Type, Values: record.Values, TTL: record.TTL}, record.Hostname)
		if len(rrs) != len(record.Values) {
			return 0, errors.New("DNS candidate record cannot be decoded")
		}
		response := new(dns.Msg)
		response.Answer = rrs
		if _, err := response.Pack(); err != nil {
			return 0, errors.New("DNS candidate record cannot be encoded")
		}
	}
	return len(payload.Records), nil
}

func readPlatformCandidateFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("platform file exceeds limit")
	}
	return b, nil
}

func (s *Service) platformJSON(ctx context.Context, endpoint, token, method string, in, out any) error {
	var data []byte
	var err error
	if in != nil {
		data, err = json.Marshal(in)
		if err != nil {
			return errors.New("encode platform request failed")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return errors.New("platform request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	client := *s.HTTPClient
	client.Timeout = 15 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("platform request unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("platform request rejected: HTTP %d", resp.StatusCode)
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 8<<20))
	if dec.Decode(out) != nil || dec.Decode(&struct{}{}) != io.EOF {
		return errors.New("platform response invalid")
	}
	return nil
}
