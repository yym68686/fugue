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

	"fugue/internal/dnsroutesource"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"github.com/miekg/dns"
)

type dnsServingCheckpoint struct {
	RouteSources *dnsroutesource.Context `json:"route_sources,omitempty"`
	Schema       string                  `json:"schema"`
	NodeID       string                  `json:"node_id"`
	GroupID      string                  `json:"group_id"`
	Parent       model.PlatformArtifact  `json:"parent"`
	Candidate    dnsPlatformCandidate    `json:"candidate"`
	AppliedAt    time.Time               `json:"applied_at"`
	Positive     bool                    `json:"positive"`
	KeyID        string                  `json:"key_id"`
	Signature    string                  `json:"signature,omitempty"`
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

type dnsReleaseBridge struct {
	parent    model.PlatformArtifact
	candidate dnsPlatformCandidate
	payload   dnsServingPayload
	routeID   string
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
	if len(raw) > 32<<20 || json.Unmarshal(raw, &c) != nil || c.Schema != "fugue.dns.positive-checkpoint/v1" || !c.Positive || c.NodeID != s.Config.DNSNodeID || c.GroupID != s.Config.EdgeGroupID || c.AppliedAt.IsZero() || c.AppliedAt.After(time.Now().Add(time.Second)) {
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
	if _, _, err := s.verifiedDNSCheckpointPayload(c); err != nil {
		return c, err
	}
	return c, nil
}

var errDNSCheckpointMissing = errors.New("DNS positive serving checkpoint missing")

func (s *Service) loadDNSServingCache() (loaded bool, resultErr error) {
	defer func() {
		if loaded {
			s.mu.Lock()
			s.platformServingRecoveryFailed = resultErr != nil && !errors.Is(resultErr, errDNSCheckpointMissing)
			s.mu.Unlock()
		}
	}()
	if s.PlatformTokenFile == "" {
		return false, nil
	}
	// Enrollment selects artifact authority even on a fresh disk. Losing all
	// checkpoints cannot silently re-enable business/ambient configuration.
	s.platformServingBound.Store(true)
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return true, errors.New("DNS serving cache path unavailable")
	}
	path := s.Config.CachePath + ".platform-serving.json"
	raw, err := platformconsumer.ReadFile(path, 32<<20)
	missing := errors.Is(err, os.ErrNotExist)
	candidates := []lkgcache.Candidate{{Path: path, Data: raw}}
	candidates = append(candidates, lkgcache.FallbackCandidates(path)...)
	if missing && len(candidates) == 1 {
		return true, errDNSCheckpointMissing
	}
	for _, item := range candidates {
		c, e := s.decodeDNSCheckpoint(item.Data)
		if e != nil {
			err = e
			continue
		}
		p, routeID, e := s.verifiedDNSCheckpointPayload(c)
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
	return s.syncPlatformDNSServing(ctx, routeprobe.Probe, s.probeDNSServingListener)
}

var errDNSReleaseConverging = errors.New("DNS release is waiting for exact route proofs")

func (s *Service) syncPlatformDNSServing(ctx context.Context, probe dnsReadinessProbeFunc, wireProbe func(*dnsServingState) error) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = s.syncPlatformDNSServingOnce(ctx, probe, wireProbe)
		if !errors.Is(err, platformconsumer.ErrAssignmentChanged) || ctx.Err() != nil {
			return err
		}
	}
	return err
}

func (s *Service) syncPlatformDNSServingOnce(ctx context.Context, probe dnsReadinessProbeFunc, wireProbe func(*dnsServingState) error) (syncErr error) {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	old := s.platformServing.Load()
	publicationObservation := &DNSDecisionPublicationState{ObservedAt: time.Now().UTC(), Outcome: "publication_unavailable"}
	if previous := s.decisionPublication.Load(); previous != nil {
		publicationObservation.LKG = previous.LKG
	}
	if old != nil && old.record.Positive {
		lkg := dnsPublication(old.record)
		publicationObservation.LKG = &lkg
	}
	defer func() {
		publicationObservation.ObservedAt = time.Now().UTC()
		loaded := s.platformServing.Load()
		if loaded != nil && loaded.record.Positive {
			lkg := dnsPublication(loaded.record)
			publicationObservation.LKG = &lkg
		}
		if syncErr == nil {
			publicationObservation.Outcome = "sync_succeeded"
		} else if errors.Is(syncErr, platformconsumer.ErrAssignmentChanged) {
			publicationObservation.Outcome = "assignment_changed"
		} else if errors.Is(syncErr, platformconsumer.ErrDNSBackendNotSelected) {
			publicationObservation.Outcome = "backend_not_selected"
		} else if loaded != nil && loaded.record.Positive && loaded.fallback == "" && publicationObservation.Desired != nil && loaded.record.Candidate.Artifact.ContentHash == publicationObservation.Desired.Digest {
			publicationObservation.Outcome = "serving_observation_failed"
		} else if publicationObservation.DesiredKnown {
			publicationObservation.Outcome = "candidate_rejected"
			publicationObservation.Rejected = true
		}
		s.decisionPublication.Store(publicationObservation)
	}()
	fallbackReason := "candidate_rejected"
	var bridge *dnsReleaseBridge
	var observations []dnsReadinessFact
	defer func() {
		// An assignment race invalidates this observation, including the bridge
		// just downloaded. Do not replace still-valid serving facts with probes
		// checked against that superseded publication. The bounded outer retry
		// rereads authority; existing proof deadlines continue to expire normally.
		if errors.Is(syncErr, platformconsumer.ErrAssignmentChanged) || errors.Is(syncErr, platformconsumer.ErrDNSBackendNotSelected) {
			return
		}
		// Rejection must not starve the retained artifact's independent probes.
		// An applied candidate or an explicit negative readiness observation
		// already replaced the runtime view and must not be overwritten here.
		if syncErr != nil && ctx.Err() == nil && old != nil && s.platformServing.Load() == old {
			if refreshed, err := s.refreshedDNSServingFacts(ctx, old, probe, fallbackReason, bridge, observations); err == nil {
				s.platformServing.Store(refreshed)
			}
		}
	}()
	client := s.platformConsumerClient()
	id, a, artifact, release, err := client.SyncServing(ctx, model.PlatformConsumerComponentDNSServer, s.Config.DNSNodeID, platformcontrol.ConfiguredConsumerScope(s.Config.PlatformScopeKey), model.PlatformArtifactKindDNSAnswerBundle)
	wasBound := s.platformServingBound.Load()
	if err != nil {
		fallbackReason = "control_plane_unavailable"
		if errors.Is(err, platformconsumer.ErrNoServingAssignment) && !s.platformServingBound.Load() {
			s.mu.Lock()
			s.platformServingError = ""
			s.mu.Unlock()
			return nil
		}
		return err
	}
	candidate := dnsPlatformCandidate{Artifact: artifact, Assignment: a, Release: release}
	desired := dnsPublication(dnsServingCheckpoint{Candidate: candidate})
	publicationObservation.Desired, publicationObservation.DesiredKnown = &desired, true
	parent, err := client.ReleaseSet(ctx, id, a, release)
	if err != nil {
		return err
	}
	desired.ParentDigest = parent.ContentHash
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
	bridge = &dnsReleaseBridge{parent: parent, candidate: candidate, payload: p, routeID: routeID}
	// Same-release observations use only volatile fresh proof cache. Persisted
	// checkpoint readiness is never reused after restart.
	same := old != nil && reflect.DeepEqual(old.record.Candidate.Assignment, a)
	if same && old.record.RouteSources == nil && len(p.dnsSourceApprovals()) == 0 && !old.checkedAt.After(time.Now()) && time.Since(old.checkedAt) < time.Duration(p.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second && dnsServingReady(old, time.Now()) {
		if old.fallback != "" {
			recovered := *old
			recovered.fallback = ""
			s.platformServing.Store(&recovered)
			return s.reportDNSServing(ctx, client, id, &recovered)
		}
		return s.reportDNSServing(ctx, client, id, old)
	}
	var previousSources *dnsroutesource.Context
	if old != nil {
		previousSources = old.record.RouteSources
	}
	p, sourceFacts, err := s.observeDNSRouteSources(ctx, client, id, candidate, p, previousSources, probe)
	if err != nil {
		return err
	}
	bridge.payload = p
	facts := sourceFacts
	if p.routeSources == nil {
		facts = collectDNSReadinessFacts(ctx, p.Plan, p.Policy.DNSReadiness, probe)
	}
	// Keep the original observations for the retained release. Candidate
	// assignment filtering below must not erase a valid old-release proof.
	observations = append([]dnsReadinessFact(nil), facts...)
	now := time.Now().UTC()
	if same && dnsSameSourceContext(old.record.RouteSources, p.routeSources) {
		facts = retainValidDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, old.facts, facts, now)
		facts = retainDNSFactsForSourceRenewal(old, facts, now)
	}
	for i := range facts {
		if !dnsFactMatchesRelease(facts[i], parent, candidate, routeID, p) {
			facts[i].Ready = false
			facts[i].Reason = "traffic_release_mismatch"
		}
	}
	checkpoint := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: s.Config.DNSNodeID, GroupID: s.Config.EdgeGroupID, Parent: parent, Candidate: candidate, AppliedAt: now, Positive: true, RouteSources: p.routeSources}
	if same {
		for _, fact := range facts {
			if fact.Reason == "retained_valid_proof" {
				checkpoint = old.record
				break
			}
		}
	}
	st, err := buildDNSServingState(checkpoint, p, routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, now)
	if err != nil {
		return err
	}
	if !dnsServingReady(st, now) {
		// Negative observations also belong to an exact assignment. A publisher
		// may advance while probing, so check authority before replacing any
		// previously positive facts or refreshing with this candidate's bridge.
		if err = client.CheckServingAssignment(ctx, id, a); err != nil {
			return err
		}
		if same {
			st.record.AppliedAt = old.record.AppliedAt
			st.record.Positive = false
			st.fallback = "readiness_incomplete"
			s.platformServing.Store(st)
			_ = s.reportDNSServingState(ctx, client, id, st, false)
		} else if hosts := dnsTransitionRecords(old, bridge); len(hosts) > 0 {
			retained, err := s.refreshedDNSServingFacts(ctx, old, probe, fallbackReason, bridge, observations)
			if err != nil {
				return err
			}
			// Refresh may perform additional probes. Recheck authority after all
			// observations and publish the retained and pending views atomically.
			if err := client.CheckServingAssignment(ctx, id, a); err != nil {
				return err
			}
			st.record.Positive = false
			st.record.AppliedAt = old.record.AppliedAt
			retained.transition = &dnsRecordTransition{state: st, hosts: hosts}
			s.platformServing.Store(retained)
		}
		summary := dnsReadinessFailureSummary(p.Plan, p.Policy.DNSReadiness, facts, now)
		if !same && old != nil && old.record.Positive && a.ConvergenceDeadline.After(now) &&
			dnsServingReady(s.platformServing.Load(), now) && dnsKnownReleaseOverlap(old, p, observations, facts, now) {
			return fmt.Errorf("%w: %s", errDNSReleaseConverging, summary)
		}
		return fmt.Errorf("DNS candidate required readiness is incomplete: %s", summary)
	}
	if err = probeDNSServingSnapshot(st); err != nil {
		return err
	}
	if err = client.CheckServingAssignment(ctx, id, a); err != nil {
		return err
	}
	if err = s.checkDNSRouteSources(ctx, client, id, candidate, p.routeSources); err != nil {
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
	if err = s.checkDNSRouteSources(ctx, client, id, candidate, p.routeSources); err != nil {
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
func (s *Service) refreshedDNSServingFacts(ctx context.Context, old *dnsServingState, probe dnsReadinessProbeFunc, reason string, bridge *dnsReleaseBridge, observations []dnsReadinessFact) (*dnsServingState, error) {
	facts := collectRetainedDNSReadinessFacts(ctx, old, bridge, observations, probe)
	now := time.Now().UTC()
	// A retained positive release may keep serving while an individual probe
	// has a transient transport failure. Keep only proofs that are still valid;
	// candidate releases never use this path and therefore still require fresh
	// evidence for every probe.
	facts = retainValidDNSReadinessFacts(old.payload.Plan, old.payload.Policy.DNSReadiness, old.facts, facts, now)
	if bridge == nil || reflect.DeepEqual(old.record.Candidate.Assignment, bridge.candidate.Assignment) {
		facts = retainDNSFactsForSourceRenewal(old, facts, now)
	}
	bridgeAllowed := compatibleDNSReleaseProbes(old, bridge)
	for i := range facts {
		if !dnsFactMatchesRelease(facts[i], old.record.Parent, old.record.Candidate, old.routeID, old.payload) &&
			!(bridgeAllowed[facts[i].ProbeID] && facts[i].Ready && dnsFactMatchesRelease(facts[i], bridge.parent, bridge.candidate, bridge.routeID, bridge.payload)) {
			facts[i].Ready = false
			facts[i].Reason = "traffic_release_mismatch"
		}
	}
	boundDNSRouteSourceFacts(old.payload, facts, now)
	st, err := buildDNSServingState(old.record, old.payload, old.routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, now)
	if err == nil {
		st.fallback = reason
		if bridge == nil {
			// Control-plane loss cannot renew pending evidence. Its original
			// proof deadlines and retained checkpoint deadline still apply.
			st.transition = old.transition
		}
	}
	return st, err
}

// A failed candidate scan already contains current observations for unchanged
// requirements. Reuse them with their original timestamps and apply the old
// release/verified-successor checks below; only changed or missing requirements
// need another probe. This avoids a second full scan starving serving proofs.
func collectRetainedDNSReadinessFacts(ctx context.Context, old *dnsServingState, bridge *dnsReleaseBridge, observations []dnsReadinessFact, probe dnsReadinessProbeFunc) []dnsReadinessFact {
	if bridge == nil || bridge.payload.Plan == nil || len(observations) == 0 {
		return collectDNSReadinessFacts(ctx, old.payload.Plan, old.payload.Policy.DNSReadiness, probe)
	}
	observedRequirements := make(map[string]platformconfig.DNSReadinessProbe, len(bridge.payload.Plan.Probes))
	for _, requirement := range bridge.payload.Plan.Probes {
		observedRequirements[requirement.ID] = requirement
	}
	byID := make(map[string]dnsReadinessFact, len(observations))
	duplicates := make(map[string]bool)
	for _, fact := range observations {
		if _, exists := byID[fact.ProbeID]; exists {
			duplicates[fact.ProbeID] = true
		}
		byID[fact.ProbeID] = fact
	}
	missing := *old.payload.Plan
	missing.Probes = nil
	bridgeAllowed := compatibleDNSReleaseProbes(old, bridge)
	facts := make([]dnsReadinessFact, len(old.payload.Plan.Probes))
	for i, requirement := range old.payload.Plan.Probes {
		fact, found := byID[requirement.ID]
		usable := !fact.Ready || dnsFactMatchesRelease(fact, old.record.Parent, old.record.Candidate, old.routeID, old.payload) ||
			bridgeAllowed[requirement.ID] && dnsFactMatchesRelease(fact, bridge.parent, bridge.candidate, bridge.routeID, bridge.payload)
		if observedRequirements[requirement.ID] == requirement && found && !duplicates[requirement.ID] && usable {
			limit := fact.Proof.CheckedAt.Add(time.Duration(platformconfig.DNSReadinessFactMaxAge(requirement, old.payload.Policy.DNSReadiness)) * time.Second)
			if limit.Before(fact.Proof.ValidUntil) {
				fact.Proof.ValidUntil = limit
			}
			facts[i] = fact
		} else {
			missing.Probes = append(missing.Probes, requirement)
		}
	}
	collected := collectDNSReadinessFacts(ctx, &missing, old.payload.Policy.DNSReadiness, probe)
	next := 0
	for i := range facts {
		if facts[i].ProbeID == "" {
			facts[i] = collected[next]
			next++
		}
	}
	return facts
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
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.BoundConsumerID(), Component: id.Component, NodeID: id.NodeID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", Sequence: sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: a.GenerationSequence, DesiredGeneration: a.ExpectedGeneration, ActualGeneration: a.ExpectedGeneration, LKGGeneration: a.ExpectedGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
	h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1, platformcontrol.CellDNSCapabilityV1, platformcontrol.DNSAuthorityTransitionCapabilityV1, platformcontrol.DNSRouteSourcesCapabilityV1}
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
