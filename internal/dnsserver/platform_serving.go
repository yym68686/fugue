package dnsserver

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"strings"
	"time"

	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

type dnsServingCheckpoint struct {
	Schema    string                 `json:"schema"`
	NodeID    string                 `json:"node_id"`
	GroupID   string                 `json:"group_id"`
	Parent    model.PlatformArtifact `json:"parent"`
	Candidate dnsPlatformCandidate   `json:"candidate"`
	AppliedAt time.Time              `json:"applied_at"`
	Positive  bool                   `json:"positive"`
	KeyID     string                 `json:"key_id"`
	Signature string                 `json:"signature,omitempty"`
}
type DNSServingStatus struct {
	State          string              `json:"state"`
	ReleaseSetID   string              `json:"release_set_id,omitempty"`
	ArtifactID     string              `json:"artifact_id,omitempty"`
	Digest         string              `json:"digest,omitempty"`
	ReleaseChannel string              `json:"release_channel,omitempty"`
	FencingToken   int64               `json:"fencing_token,omitempty"`
	Zones          int                 `json:"zones"`
	Readiness      *DNSReadinessStatus `json:"readiness,omitempty"`
	FallbackReason string              `json:"fallback_reason,omitempty"`
	LastError      string              `json:"last_error,omitempty"`
	ReportedAt     time.Time           `json:"reported_at,omitempty"`
}

func checkpointMAC(c dnsServingCheckpoint, key string) string {
	c.Signature = ""
	raw, _ := json.Marshal(c)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte("fugue/dns/positive-checkpoint/v1\x00"))
	mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *Service) signDNSCheckpoint(c *dnsServingCheckpoint) error {
	k := s.platformDNSKeys()
	if k.PrimaryKey == "" || k.PrimaryKeyID == "" {
		return errors.New("DNS checkpoint signing key unavailable")
	}
	c.KeyID = k.PrimaryKeyID
	c.Signature = checkpointMAC(*c, k.PrimaryKey)
	return nil
}
func (s *Service) decodeDNSCheckpoint(raw []byte) (dnsServingCheckpoint, error) {
	var c dnsServingCheckpoint
	if len(raw) > 16<<20 || json.Unmarshal(raw, &c) != nil || c.Schema != "fugue.dns.positive-checkpoint/v1" || !c.Positive || c.NodeID != s.Config.DNSNodeID || c.GroupID != s.Config.EdgeGroupID || c.AppliedAt.IsZero() || c.AppliedAt.After(time.Now().Add(time.Second)) {
		return c, errors.New("DNS positive checkpoint invalid")
	}
	k := s.platformDNSKeys()
	if _, revoked := k.RevokedKeyIDs[strings.ToLower(c.KeyID)]; revoked {
		return c, errors.New("DNS checkpoint key revoked")
	}
	key := ""
	if c.KeyID == k.PrimaryKeyID {
		key = k.PrimaryKey
	} else if c.KeyID == k.PreviousKeyID {
		key = k.PreviousKey
	}
	if key == "" || !hmac.Equal([]byte(c.Signature), []byte(checkpointMAC(c, key))) {
		return c, errors.New("DNS checkpoint signature invalid")
	}
	if _, _, err := s.verifyDNSServingRelease(c.Parent, c.Candidate); err != nil {
		return c, err
	}
	return c, nil
}

func (s *Service) loadDNSServingCache() (bool, error) {
	if s.PlatformTokenFile == "" {
		return false, nil
	}
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return true, errors.New("DNS serving cache path unavailable")
	}
	path := s.Config.CachePath + ".platform-serving.json"
	raw, err := platformconsumer.ReadFile(path, 16<<20)
	missing := errors.Is(err, os.ErrNotExist)
	candidates := []lkgcache.Candidate{{Path: path, Data: raw}}
	candidates = append(candidates, lkgcache.FallbackCandidates(path)...)
	if missing && len(candidates) == 1 {
		return false, nil
	}
	for _, item := range candidates {
		c, e := s.decodeDNSCheckpoint(item.Data)
		if e != nil {
			err = e
			continue
		}
		p, routeID, e := s.verifyDNSServingRelease(c.Parent, c.Candidate)
		if e != nil {
			err = e
			continue
		}
		st, e := buildDNSServingState(c, p, routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, nil, time.Now().UTC())
		if e != nil {
			err = e
			continue
		}
		st.fallback = "restart_requires_fresh_readiness"
		s.platformServing.Store(st)
		s.platformServingBound.Store(true)
		return true, nil
	}
	s.platformServingBound.Store(true) // corrupted positive state must not downgrade to ambient configuration
	return true, fmt.Errorf("DNS serving recovery unavailable: %w", err)
}

func (s *Service) SyncPlatformDNSServingOnce(ctx context.Context) error {
	return s.syncPlatformDNSServingOnce(ctx, routeprobe.Probe, s.probeDNSServingListener)
}

func (s *Service) syncPlatformDNSServingOnce(ctx context.Context, probe dnsReadinessProbeFunc, wireProbe func(*dnsServingState) error) error {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient}
	id, a, artifact, release, err := client.SyncServing(ctx, model.PlatformConsumerComponentDNSServer, s.Config.DNSNodeID, "global", model.PlatformArtifactKindDNSAnswerBundle)
	old := s.platformServing.Load()
	wasBound := s.platformServingBound.Load()
	if err != nil {
		if old != nil {
			s.refreshDNSServingFacts(ctx, old, probe, "control_plane_unavailable")
			return err
		}
		if errors.Is(err, platformconsumer.ErrNoServingAssignment) && !s.platformServingBound.Load() {
			s.mu.Lock()
			s.platformServingError = ""
			s.mu.Unlock()
			return nil
		}
		return err
	}
	parent, err := client.ReleaseSet(ctx, id, a, release)
	if err != nil {
		return err
	}
	candidate := dnsPlatformCandidate{Artifact: artifact, Assignment: a, Release: release}
	p, routeID, err := s.verifyDNSServingRelease(parent, candidate)
	if err != nil {
		return err
	}
	if old != nil {
		prev := old.record.Candidate.Release
		if prev.ReleaseChannel == release.ReleaseChannel && (release.FencingToken < prev.FencingToken || (release.FencingToken == prev.FencingToken && (release.ID != prev.ID || artifact.ContentHash != old.record.Candidate.Artifact.ContentHash))) {
			return errors.New("DNS serving release replay rejected")
		}
		if prev.ReleaseChannel != release.ReleaseChannel && !release.ReleasedAt.After(prev.ReleasedAt) {
			return errors.New("DNS serving channel replay rejected")
		}
	}
	// Same-release observations use only volatile fresh proof cache. Persisted
	// checkpoint readiness is never reused after restart.
	same := old != nil && reflect.DeepEqual(old.record.Candidate.Assignment, a)
	if same && !old.checkedAt.After(time.Now()) && time.Since(old.checkedAt) < time.Duration(p.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second && dnsServingReady(old, time.Now()) {
		if old.fallback != "" {
			recovered := *old
			recovered.fallback = ""
			s.platformServing.Store(&recovered)
			return s.reportDNSServing(ctx, client, id, &recovered)
		}
		return s.reportDNSServing(ctx, client, id, old)
	}
	facts := collectDNSReadinessFacts(ctx, p.Plan, p.Policy.DNSReadiness, probe)
	for i := range facts {
		if !dnsProofMatchesRelease(facts[i].Proof, parent, candidate, routeID) {
			facts[i].Ready = false
			facts[i].Reason = "traffic_release_mismatch"
		}
	}
	now := time.Now().UTC()
	checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: s.Config.DNSNodeID, GroupID: s.Config.EdgeGroupID, Parent: parent, Candidate: candidate, AppliedAt: now, Positive: true}
	st, err := buildDNSServingState(checkpoint, p, routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, now)
	if err != nil {
		return err
	}
	if !dnsServingReady(st, now) {
		if same {
			st.record = old.record
			st.fallback = "readiness_incomplete"
			s.platformServing.Store(st)
			_ = s.reportDNSServingState(ctx, client, id, st, false)
		}
		return errors.New("DNS candidate required readiness is incomplete")
	}
	if err = probeDNSServingSnapshot(st); err != nil {
		return err
	}
	if err = client.CheckServingAssignment(ctx, id, a); err != nil {
		return err
	}
	if err = s.signDNSCheckpoint(&st.record); err != nil {
		return err
	}
	s.platformServing.Store(st)
	s.platformServingBound.Store(true)
	rollback := func() {
		s.platformServing.Store(old)
		s.platformServingBound.Store(wasBound)
	}
	if err = wireProbe(st); err != nil {
		rollback()
		if same {
			_ = s.reportDNSServingState(ctx, client, id, old, false)
		}
		return err
	}
	if err = client.CheckServingAssignment(ctx, id, a); err != nil {
		rollback()
		return err
	}
	if !dnsServingReady(st, time.Now()) {
		rollback()
		return errors.New("DNS readiness expired during apply")
	}
	raw, err := json.Marshal(st.record)
	if err != nil {
		rollback()
		return err
	}
	path := s.Config.CachePath + ".platform-serving.json"
	if strings.TrimSpace(s.Config.CachePath) == "" {
		rollback()
		return errors.New("DNS serving cache path unavailable")
	}
	if old != nil && !same {
		if err = lkgcache.PreservePrevious(path, func(data []byte) bool { _, e := s.decodeDNSCheckpoint(data); return e == nil }); err != nil && !errors.Is(err, os.ErrNotExist) {
			rollback()
			return err
		}
	}
	if err = lkgcache.AtomicWriteFile(path, raw, 0600); err != nil {
		rollback()
		return err
	}
	return s.reportDNSServing(ctx, client, id, st)
}

func dnsServingReady(st *dnsServingState, now time.Time) bool {
	if st == nil || st.record.AppliedAt.Add(time.Duration(st.payload.Policy.MaxStaleSeconds)*time.Second).Before(now) {
		return false
	}
	status := summarizeDNSReadiness(st.payload.Plan, st.payload.Policy.DNSReadiness, st.facts, "", st.checkedAt, now)
	return status.ReadyRecords == status.Records
}
func (s *Service) refreshDNSServingFacts(ctx context.Context, old *dnsServingState, probe dnsReadinessProbeFunc, reason string) {
	facts := collectDNSReadinessFacts(ctx, old.payload.Plan, old.payload.Policy.DNSReadiness, probe)
	for i := range facts {
		if !dnsProofMatchesRelease(facts[i].Proof, old.record.Parent, old.record.Candidate, old.routeID) {
			facts[i].Ready = false
			facts[i].Reason = "traffic_release_mismatch"
		}
	}
	st, err := buildDNSServingState(old.record, old.payload, old.routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, time.Now().UTC())
	if err == nil {
		st.fallback = reason
		s.platformServing.Store(st)
	}
}

func probeDNSServingSnapshot(st *dnsServingState) error {
	for _, zone := range st.zoneOrder {
		for name, rows := range st.zones[zone].records {
			for _, entry := range rows {
				req := new(dns.Msg)
				req.SetQuestion(dns.Fqdn(name), dns.StringToType[entry.record.Type])
				response := st.answer(req, "", time.Now().UTC())
				if response.Rcode != dns.RcodeSuccess {
					return errors.New("DNS artifact wire probe failed")
				}
				raw, err := response.Pack()
				if err != nil {
					return err
				}
				var decoded dns.Msg
				if decoded.Unpack(raw) != nil {
					return errors.New("DNS artifact wire decode failed")
				}
			}
		}
	}
	return nil
}
func (s *Service) probeDNSServingListener(st *dnsServingState) error {
	for _, network := range []string{"udp", "tcp"} {
		address := s.Config.UDPAddr
		if network == "tcp" {
			address = s.Config.TCPAddr
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return errors.New("DNS serving listener unavailable")
		}
		// Go accepts :port as an unspecified listener. Probe the local
		// IPv4 endpoint exactly as for an explicit 0.0.0.0 binding.
		if host == "" {
			host = "127.0.0.1"
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return errors.New("DNS listener must bind an IP")
		}
		if ip.IsUnspecified() {
			if ip.To4() != nil {
				host = "127.0.0.1"
			} else {
				host = "::1"
			}
		} else if !ip.IsLoopback() {
			return errors.New("DNS serving probe requires local listener")
		}
		client := dns.Client{Net: network, Timeout: 2 * time.Second}
		for _, zone := range st.zoneOrder {
			req := new(dns.Msg)
			req.SetQuestion(dns.Fqdn(zone), dns.TypeSOA)
			response, _, err := client.Exchange(req, net.JoinHostPort(host, port))
			if err != nil || response == nil || response.Rcode != dns.RcodeSuccess || len(response.Answer) != 1 || !response.Authoritative {
				return errors.New("DNS active listener probe failed")
			}
			soa, ok := response.Answer[0].(*dns.SOA)
			if !ok || soa.Serial != uint32(st.record.Candidate.Artifact.GenerationSequence) || soa.Ns != dns.Fqdn(st.zones[zone].authority.Nameservers[0]) {
				return errors.New("DNS listener serves another artifact")
			}
		}
	}
	return nil
}

func (s *Service) reportDNSServing(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, st *dnsServingState) error {
	return s.reportDNSServingState(ctx, client, id, st, true)
}
func (s *Service) reportDNSServingState(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, st *dnsServingState, positive bool) error {
	a := st.record.Candidate.Assignment
	if (positive && !dnsServingReady(st, time.Now())) || s.platformServing.Load() != st {
		return errors.New("DNS serving evidence changed")
	}
	if err := client.CheckServingAssignment(ctx, id, a); err != nil {
		return err
	}
	path := s.Config.CachePath + ".platform-serving-cursor.json"
	var sequence int64
	if raw, err := platformconsumer.ReadFile(path, 1024); err == nil {
		if json.Unmarshal(raw, &sequence) != nil || sequence <= 0 || sequence == math.MaxInt64 {
			return errors.New("DNS serving cursor corrupt")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	sequence = max(sequence+1, time.Now().UnixNano())
	raw, _ := json.Marshal(sequence)
	if err := lkgcache.AtomicWriteFile(path, raw, 0600); err != nil {
		return err
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.Component + ":" + id.NodeID, Component: id.Component, NodeID: id.NodeID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: a.GenerationSequence, DesiredGeneration: a.ExpectedGeneration, ActualGeneration: a.ExpectedGeneration, LKGGeneration: a.ExpectedGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
	h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
	if !positive {
		h.ProbeStatus = "failed"
		h.LastError = "DNS serving readiness or listener probe failed"
		h.ServingLKG = true
	}
	h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	var receipt model.PlatformConsumerHeartbeatResponse
	if err := client.PostJSON(ctx, "/v1/platform-state/consumers/trusted-heartbeat", id.Token, h, &receipt); err != nil {
		return err
	}
	if !receipt.Consumer.IdentityVerified || receipt.Consumer.ConsumerID != h.ConsumerID || receipt.Consumer.Sequence != sequence || receipt.Consumer.EvidenceHash != h.EvidenceHash || receipt.Consumer.ExpectedConsumerSetID != a.ExpectedConsumerSetID {
		return errors.New("DNS serving receipt mismatch")
	}
	s.mu.Lock()
	s.platformServingReported = time.Now().UTC()
	s.platformServingError = h.LastError
	s.mu.Unlock()
	return nil
}
