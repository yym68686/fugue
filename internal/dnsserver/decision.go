package dnsserver

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/weightedselector"
	"github.com/miekg/dns"
)

const DNSDecisionSchema = "fugue.dns-decision/v1"
const DNSDecisionMaxBytes = 256 << 10

type DNSDecisionPublication struct {
	ReleaseSetID        string `json:"release_set_id,omitempty"`
	ParentDigest        string `json:"parent_digest,omitempty"`
	ArtifactID          string `json:"artifact_id,omitempty"`
	Digest              string `json:"digest,omitempty"`
	Generation          string `json:"generation,omitempty"`
	FencingToken        int64  `json:"fencing_token,omitempty"`
	PolicyDigest        string `json:"policy_digest,omitempty"`
	InputSnapshotDigest string `json:"input_snapshot_digest,omitempty"`
	ReleaseChannel      string `json:"release_channel,omitempty"`
}

type DNSDecisionPublicationState struct {
	ObservedAt     time.Time               `json:"observed_at"`
	DesiredKnown   bool                    `json:"desired_known"`
	Desired        *DNSDecisionPublication `json:"desired,omitempty"`
	Loaded         *DNSDecisionPublication `json:"loaded,omitempty"`
	LKG            *DNSDecisionPublication `json:"lkg,omitempty"`
	ServingLKG     bool                    `json:"serving_lkg"`
	Rejected       bool                    `json:"rejected"`
	Outcome        string                  `json:"outcome,omitempty"`
	FallbackReason string                  `json:"fallback_reason,omitempty"`
}

type DNSDecisionFilter struct {
	EdgeID string `json:"edge_id"`
	IP     string `json:"ip"`
	Reason string `json:"reason"`
}

type DNSDecisionRank struct {
	Candidate model.EdgeDNSAnswerCandidate `json:"candidate"`
	SortScore int                          `json:"sort_score"`
}

type DNSDecisionRecord struct {
	RecordName             string                         `json:"record_name"`
	Policy                 model.DNSAnswerPolicy          `json:"policy"`
	InputCandidates        []model.EdgeDNSAnswerCandidate `json:"input_candidates"`
	MaterializedCandidates []model.EdgeDNSAnswerCandidate `json:"materialized_candidates"`
	Ranking                []DNSDecisionRank              `json:"ranking"`
	Filtered               []DNSDecisionFilter            `json:"filtered"`
	SelectionAt            time.Time                      `json:"selection_at"`
	SelectedEdgeGroupID    string                         `json:"selected_edge_group_id"`
	SelectionResult        string                         `json:"selection_result"`
	ExplorationKind        string                         `json:"exploration_kind"`
	CooldownUntil          time.Time                      `json:"cooldown_until,omitempty"`
	CooldownResult         string                         `json:"cooldown_result"`
	ScopeSource            string                         `json:"scope_source"`
	ScopeResolution        string                         `json:"scope_resolution"`
	MatchedScopeKey        string                         `json:"matched_scope_key"`
	Answered               []model.EdgeDNSAnswerCandidate `json:"answered"`
}

type DNSDecisionReceipt struct {
	Schema            string                      `json:"schema"`
	DecisionID        string                      `json:"decision_id"`
	ProcessID         string                      `json:"process_id"`
	NodeID            string                      `json:"node_id"`
	ObservedAt        time.Time                   `json:"observed_at"`
	Hostname          string                      `json:"hostname"`
	QType             uint16                      `json:"qtype"`
	QueryID           uint16                      `json:"query_id"`
	Transport         string                      `json:"transport"`
	RCode             int                         `json:"rcode"`
	RRSet             []string                    `json:"rrset"`
	Authority         []string                    `json:"authority"`
	Additional        []string                    `json:"additional"`
	WriteSucceeded    bool                        `json:"write_succeeded"`
	Publication       DNSDecisionPublicationState `json:"publication"`
	AnswerPublication *DNSDecisionPublication     `json:"answer_publication,omitempty"`
	Records           []DNSDecisionRecord         `json:"records"`
	ReplayInput       []byte                      `json:"replay_input"`
	EvidenceDigest    string                      `json:"evidence_digest"`
}

type dnsDecisionHint struct {
	Country     string `json:"country,omitempty"`
	Region      string `json:"region,omitempty"`
	ASN         string `json:"asn,omitempty"`
	EdgeGroupID string `json:"edge_group_id,omitempty"`
	Source      string `json:"source"`
}

type dnsDecisionBucket struct {
	Purpose string `json:"purpose"`
	Modulus int    `json:"modulus"`
	Value   int    `json:"value"`
}

type dnsDecisionEntropy struct {
	Buckets   []dnsDecisionBucket `json:"buckets"`
	replaying bool
	cursor    int
	err       error
}

type dnsDecisionEntry struct {
	Record model.EdgeDNSRecord             `json:"record"`
	Plan   platformconfig.DNSReadinessPlan `json:"plan"`
	Facts  []dnsReadinessFact              `json:"facts"`
}

type dnsDecisionSelection struct {
	At      time.Time          `json:"at"`
	Entropy dnsDecisionEntropy `json:"entropy"`
}

type dnsDecisionStage struct {
	Publication        DNSDecisionPublication              `json:"publication"`
	AppliedAt          time.Time                           `json:"applied_at"`
	GenerationSequence int64                               `json:"generation_sequence"`
	MaxStaleSeconds    int                                 `json:"max_stale_seconds"`
	Readiness          *platformconfig.DNSReadinessPolicy  `json:"readiness"`
	Authorities        []platformconfig.DNSAuthorityPolicy `json:"authorities"`
	Zone               string                              `json:"zone"`
	RecordName         string                              `json:"record_name"`
	Exists             bool                                `json:"exists"`
	Entries            []dnsDecisionEntry                  `json:"entries"`
	Hint               dnsDecisionHint                     `json:"hint"`
	Selections         []*dnsDecisionSelection             `json:"selections"`
	QueryAt            time.Time                           `json:"query_at"`
}

type dnsDecisionReplay struct {
	Version        string              `json:"version"`
	Query          []byte              `json:"query"`
	Stages         []*dnsDecisionStage `json:"stages"`
	NoServingState bool                `json:"no_serving_state,omitempty"`
}

type dnsDecisionCapture struct {
	input             dnsDecisionReplay
	records           []DNSDecisionRecord
	replaying         bool
	stageCursor       int
	selectionCursor   int
	err               error
	observations      []dnsDecisionRecordInput
	answerPublication *DNSDecisionPublication
}

type dnsDecisionRecordInput struct {
	entry        dnsServingRecord
	materialized *model.EdgeDNSRecord
	hint         dnsGeoHint
	at           time.Time
	audit        edgeDNSAnswerAudit
	failure      string
}

func dnsPublication(record dnsServingCheckpoint) DNSDecisionPublication {
	candidate := record.Candidate
	return DNSDecisionPublication{ReleaseSetID: candidate.Assignment.ReleaseSetID, ParentDigest: record.Parent.ContentHash,
		ArtifactID: candidate.Artifact.ID, Digest: candidate.Artifact.ContentHash, Generation: candidate.Artifact.Generation,
		FencingToken: candidate.Release.FencingToken, ReleaseChannel: candidate.Release.ReleaseChannel,
		PolicyDigest: candidate.Artifact.Metadata["policy_digest"], InputSnapshotDigest: candidate.Artifact.Metadata["input_snapshot_digest"]}
}

func decisionPublicationState(state *dnsServingState, observed *DNSDecisionPublicationState) DNSDecisionPublicationState {
	out := DNSDecisionPublicationState{ObservedAt: time.Now().UTC(), Outcome: "not_observed"}
	if observed != nil {
		out = *observed
	}
	if state != nil {
		loaded := dnsPublication(state.record)
		out.Loaded = &loaded
		if state.record.Positive {
			out.LKG = &loaded
		}
		out.FallbackReason = state.fallback
		out.ServingLKG = out.LKG != nil && out.LKG.Digest == loaded.Digest && (state.fallback != "" || out.Desired != nil && *out.Desired != loaded)
	}
	return out
}

func (capture *dnsDecisionCapture) begin(state *dnsServingState, request *dns.Msg, remote string, now time.Time) (*dnsDecisionStage, dnsGeoHint) {
	hint := platformDNSHintForQuery(state.matcher, request, remote)
	if capture == nil {
		return nil, hint
	}
	if capture.replaying {
		if capture.stageCursor >= len(capture.input.Stages) {
			capture.err = errors.New("replay stage missing")
			return nil, hint
		}
		stage := capture.input.Stages[capture.stageCursor]
		capture.stageCursor++
		capture.selectionCursor = 0
		return stage, dnsGeoHint{Country: stage.Hint.Country, Region: stage.Hint.Region, ASN: stage.Hint.ASN, EdgeGroupID: stage.Hint.EdgeGroupID, Source: stage.Hint.Source}
	}
	if len(state.zoneOrder) > 64 {
		capture.err = errors.New("decision authority bound exceeded")
		return nil, hint
	}
	stage := &dnsDecisionStage{Publication: dnsPublication(state.record), AppliedAt: state.record.AppliedAt,
		GenerationSequence: state.record.Candidate.Artifact.GenerationSequence, MaxStaleSeconds: state.payload.Policy.MaxStaleSeconds,
		Readiness: state.payload.Policy.DNSReadiness, QueryAt: now, Entries: []dnsDecisionEntry{}, Selections: []*dnsDecisionSelection{},
		Hint: dnsDecisionHint{Country: hint.Country, Region: hint.Region, ASN: hint.ASN, EdgeGroupID: hint.EdgeGroupID, Source: hint.Source}}
	name := ""
	if len(request.Question) == 1 {
		name = normalizeName(request.Question[0].Name)
	}
	for _, zone := range state.zoneOrder {
		value := state.zones[zone]
		stage.Authorities = append(stage.Authorities, value.authority)
		if stage.Zone != "" || !nameWithinZone(name, zone) {
			continue
		}
		stage.Zone = zone
		stage.RecordName = name
		entries, exists := value.records[name]
		if !exists {
			stage.RecordName = edgeDNSWildcardName(name)
			entries, exists = value.records[stage.RecordName]
		}
		stage.Exists = exists
		if len(entries) > 16 {
			capture.err = errors.New("decision entry bound exceeded")
			continue
		}
		for _, entry := range entries {
			if len(entry.record.Candidates) > 256 || len(entry.record.ScopedCandidates) > 64 || len(entry.facts) > 1024 || len(entry.plan.Probes) > 1024 {
				capture.err = errors.New("decision candidate or proof bound exceeded")
				continue
			}
			stage.Entries = append(stage.Entries, dnsDecisionEntry{Record: entry.record, Plan: entry.plan, Facts: entry.facts})
		}
	}
	if len(capture.input.Stages) >= 2 {
		capture.err = errors.New("decision transition bound exceeded")
		return nil, hint
	}
	capture.input.Stages = append(capture.input.Stages, stage)
	return stage, hint
}

func (capture *dnsDecisionCapture) selection(stage *dnsDecisionStage, hint dnsGeoHint) (time.Time, dnsGeoHint) {
	now := time.Now().UTC()
	if capture == nil || stage == nil {
		return now, hint
	}
	if capture.replaying {
		if capture.selectionCursor >= len(stage.Selections) {
			capture.err = errors.New("replay selection missing")
			return now, hint
		}
		selection := stage.Selections[capture.selectionCursor]
		capture.selectionCursor++
		selection.Entropy.replaying = true
		selection.Entropy.cursor = 0
		selection.Entropy.err = nil
		hint.decisionEntropy = &selection.Entropy
		return selection.At, hint
	}
	selection := &dnsDecisionSelection{At: now, Entropy: dnsDecisionEntropy{Buckets: []dnsDecisionBucket{}}}
	stage.Selections = append(stage.Selections, selection)
	hint.decisionEntropy = &selection.Entropy
	return now, hint
}

func dnsDecisionBucketValue(hint dnsGeoHint, seed string, modulus int, purpose string) int {
	entropy := hint.decisionEntropy
	if entropy != nil && entropy.replaying {
		if entropy.cursor >= len(entropy.Buckets) {
			entropy.err = errors.New("exploration bucket missing")
			return 0
		}
		bucket := entropy.Buckets[entropy.cursor]
		entropy.cursor++
		if bucket.Modulus != modulus || bucket.Purpose != purpose || bucket.Value < 0 || bucket.Value >= modulus {
			entropy.err = errors.New("exploration bucket differs")
			return 0
		}
		return bucket.Value
	}
	value := weightedselector.Bucket(seed, modulus)
	if entropy != nil {
		entropy.Buckets = append(entropy.Buckets, dnsDecisionBucket{Purpose: purpose, Modulus: modulus, Value: value})
	}
	return value
}

func (capture *dnsDecisionCapture) record(entry dnsServingRecord, materialized *model.EdgeDNSRecord, hint dnsGeoHint, at time.Time, audit edgeDNSAnswerAudit, failure string) {
	if capture == nil {
		return
	}
	if !capture.replaying {
		hint.IP = ""
		hint.decisionEntropy = nil
		if len(capture.observations) >= 32 {
			capture.err = errors.New("decision record bound exceeded")
			return
		}
		capture.observations = append(capture.observations, dnsDecisionRecordInput{entry: entry, materialized: materialized, hint: hint, at: at, audit: audit, failure: failure})
		return
	}
	capture.describeRecord(entry, materialized, hint, at, audit, failure)
}

func (capture *dnsDecisionCapture) describeRecord(entry dnsServingRecord, materialized *model.EdgeDNSRecord, hint dnsGeoHint, at time.Time, audit edgeDNSAnswerAudit, failure string) {
	input := entry.record.Candidates
	policy := entry.record.AnswerPolicy
	cooldownUntil := time.Time{}
	cooldownResult := "not_recorded"
	if scoped, ok := edgeDNSScopedCandidatesForHint(entry.record.ScopedCandidates, hint); ok {
		input = scoped.Candidates
		cooldownUntil = scoped.CooldownUntil
		switch {
		case scoped.CooldownUntil.IsZero():
			cooldownResult = "not_active"
		case at.Before(scoped.CooldownUntil):
			cooldownResult = "active"
		default:
			cooldownResult = "expired"
		}
		policy.SelectedEdgeGroupID = scoped.SelectedEdgeGroupID
		if scoped.PolicyKind != "" {
			policy.PolicyKind = scoped.PolicyKind
		}
		if scoped.Reason != "" {
			policy.Reason = scoped.Reason
		}
	}
	output := []model.EdgeDNSAnswerCandidate{}
	if materialized != nil {
		output = materialized.Candidates
		if scoped, ok := edgeDNSScopedCandidatesForHint(materialized.ScopedCandidates, hint); ok {
			output = scoped.Candidates
		}
	}
	record := DNSDecisionRecord{RecordName: entry.record.Name, Policy: policy, InputCandidates: append([]model.EdgeDNSAnswerCandidate{}, input...),
		MaterializedCandidates: append([]model.EdgeDNSAnswerCandidate{}, output...), Filtered: []DNSDecisionFilter{}, SelectionAt: at,
		Ranking:             append([]DNSDecisionRank{}, audit.Ranking...),
		SelectedEdgeGroupID: audit.SelectedEdgeGroupID, SelectionResult: firstNonEmpty(failure, audit.SelectionResult), ExplorationKind: audit.ExplorationKind,
		CooldownUntil: cooldownUntil, CooldownResult: cooldownResult, ScopeSource: firstNonEmpty(hint.Source, "none"),
		ScopeResolution: firstNonEmpty(audit.ClientScopeResolution, edgeDNSClientScopeResolution(hint, false)), MatchedScopeKey: firstNonEmpty(audit.MatchedScopeKey, "global"),
		Answered: append([]model.EdgeDNSAnswerCandidate{}, audit.Answered...)}
	materializedCandidates := make(map[string]model.EdgeDNSAnswerCandidate, len(output))
	for _, candidate := range output {
		materializedCandidates[edgeDNSCandidateDecisionKey(candidate)] = candidate
	}
	answeredKeys := make(map[string]bool, len(audit.Answered))
	for _, candidate := range audit.Answered {
		answeredKeys[edgeDNSCandidateDecisionKey(candidate)] = true
	}
	filtered := make(map[string]bool, len(audit.Filtered))
	for _, candidate := range audit.Filtered {
		key := edgeDNSCandidateDecisionKey(candidate.Candidate)
		filtered[key] = true
		record.Filtered = append(record.Filtered, DNSDecisionFilter{EdgeID: candidate.Candidate.EdgeID, IP: candidate.Candidate.IP, Reason: candidate.Reason})
	}
	for _, candidate := range input {
		key := edgeDNSCandidateDecisionKey(candidate)
		if !answeredKeys[key] && !filtered[key] {
			reason := "materialization_filtered"
			if actual, ok := materializedCandidates[key]; ok {
				reason = "not_selected_by_answer_limit"
				switch {
				case edgeDNSCandidateLKGInvalid(actual):
					reason = "candidate_lkg_invalid"
				case policy.HealthRequired && !actual.Healthy:
					reason = "health_required"
				case policy.RouteReadyRequired && !actual.RouteReady:
					reason = "route_ready_required"
				case policy.PolicyKind != model.DNSAnswerPolicyKindDisabled && !actual.TLSReady:
					reason = "tls_ready_required"
				}
			}
			record.Filtered = append(record.Filtered, DNSDecisionFilter{EdgeID: candidate.EdgeID, IP: candidate.IP, Reason: reason})
		}
	}
	capture.records = append(capture.records, record)
}

func dnsRRStrings(records []dns.RR) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, record.String())
	}
	return out
}

func dnsDecisionRedactOPT(records []dns.RR) []dns.RR {
	result := []dns.RR{}
	for _, record := range records {
		option, ok := record.(*dns.OPT)
		if !ok || option == nil {
			continue
		}
		redacted := &dns.OPT{Hdr: option.Hdr}
		for _, value := range option.Option {
			subnet, ok := value.(*dns.EDNS0_SUBNET)
			if !ok || subnet == nil {
				continue
			}
			copy := *subnet
			if subnet.Address.To4() != nil {
				copy.Address = net.IPv4zero.To4()
			} else if subnet.Address.To16() != nil {
				copy.Address = net.IPv6zero.To16()
			} else {
				copy.Address = nil
			}
			redacted.Option = append(redacted.Option, &copy)
		}
		result = append(result, redacted)
	}
	return result
}

func dnsDecisionDigest(receipt DNSDecisionReceipt) string {
	receipt.EvidenceDigest = ""
	raw, err := json.Marshal(receipt)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(digest[:])
}

type DNSDecisionReplayResult struct {
	DecisionID string              `json:"decision_id"`
	Matched    bool                `json:"matched"`
	RCode      int                 `json:"rcode"`
	RRSet      []string            `json:"rrset"`
	Authority  []string            `json:"authority"`
	Records    []DNSDecisionRecord `json:"records"`
}

func ReplayDNSDecision(receipt DNSDecisionReceipt) (result DNSDecisionReplayResult, replayErr error) {
	defer func() {
		if recover() != nil {
			result = DNSDecisionReplayResult{}
			replayErr = errors.New("malformed DNS replay input")
		}
	}()
	if receipt.Schema != DNSDecisionSchema || len(receipt.ReplayInput) > DNSDecisionMaxBytes || receipt.EvidenceDigest == "" || dnsDecisionDigest(receipt) != receipt.EvidenceDigest {
		return DNSDecisionReplayResult{}, errors.New("decision schema, bounds or evidence digest invalid")
	}
	var input dnsDecisionReplay
	if err := json.Unmarshal(receipt.ReplayInput, &input); err != nil || input.Version != DNSDecisionSchema || len(input.Stages) > 2 || len(input.Stages) == 0 && !input.NoServingState {
		return DNSDecisionReplayResult{}, errors.New("decision replay input invalid")
	}
	var query dns.Msg
	if err := query.Unpack(input.Query); err != nil {
		return DNSDecisionReplayResult{}, errors.New("decision query invalid")
	}
	if len(query.Question) != 1 {
		return DNSDecisionReplayResult{}, errors.New("decision query question invalid")
	}
	question := query.Question[0]
	if normalizeName(question.Name) != normalizeName(receipt.Hostname) || question.Qtype != receipt.QType || query.Id != receipt.QueryID {
		return DNSDecisionReplayResult{}, errors.New("decision receipt query metadata differs")
	}
	if input.NoServingState {
		response := new(dns.Msg)
		response.SetRcode(&query, dns.RcodeServerFailure)
		result = DNSDecisionReplayResult{DecisionID: receipt.DecisionID, RCode: response.Rcode, RRSet: dnsRRStrings(response.Answer), Authority: dnsRRStrings(response.Ns), Records: []DNSDecisionRecord{}}
		result.Matched = len(input.Stages) == 0 && receipt.Publication.Loaded == nil && receipt.AnswerPublication == nil && result.RCode == receipt.RCode && reflect.DeepEqual(result.RRSet, receipt.RRSet) && reflect.DeepEqual(result.Authority, receipt.Authority) && reflect.DeepEqual([]string{}, receipt.Additional) && reflect.DeepEqual(result.Records, receipt.Records)
		if !result.Matched {
			return result, errors.New("absent serving state replay differs")
		}
		return result, nil
	}
	if len(input.Stages) == 0 || input.Stages[0] == nil || !input.Stages[0].QueryAt.Equal(receipt.ObservedAt) {
		return DNSDecisionReplayResult{}, errors.New("decision receipt observation time differs")
	}
	states := make([]*dnsServingState, 0, len(input.Stages))
	for _, stage := range input.Stages {
		if stage == nil || stage.QueryAt.IsZero() || len(stage.Entries) > 16 || len(stage.Authorities) > 64 || len(stage.Selections) > 32 {
			return DNSDecisionReplayResult{}, errors.New("replay stage invalid")
		}
		for _, authority := range stage.Authorities {
			if len(authority.Nameservers) == 0 {
				return DNSDecisionReplayResult{}, errors.New("replay authority invalid")
			}
		}
		for _, selection := range stage.Selections {
			if selection == nil || selection.At.IsZero() || len(selection.Entropy.Buckets) > 8 {
				return DNSDecisionReplayResult{}, errors.New("replay selection invalid")
			}
		}
		state := &dnsServingState{record: dnsServingCheckpoint{AppliedAt: stage.AppliedAt, Candidate: dnsPlatformCandidate{Artifact: model.PlatformArtifact{GenerationSequence: stage.GenerationSequence}}},
			payload: dnsServingPayload{Policy: platformconfig.PolicySnapshot{MaxStaleSeconds: stage.MaxStaleSeconds, DNSReadiness: stage.Readiness}}, zones: map[string]dnsServingZone{}}
		for _, authority := range stage.Authorities {
			state.zoneOrder = append(state.zoneOrder, authority.Zone)
			zone := dnsServingZone{authority: authority, records: map[string][]dnsServingRecord{}}
			if authority.Zone == stage.Zone && stage.Exists {
				zone.records[stage.RecordName] = []dnsServingRecord{}
				for _, entry := range stage.Entries {
					zone.records[stage.RecordName] = append(zone.records[stage.RecordName], dnsServingRecord{record: entry.Record, plan: entry.Plan, facts: entry.Facts})
				}
			}
			state.zones[authority.Zone] = zone
		}
		states = append(states, state)
	}
	if len(states) == 2 {
		states[0].transition = &dnsRecordTransition{state: states[1], hosts: map[string]bool{input.Stages[0].RecordName: true}}
	}
	capture := &dnsDecisionCapture{input: input, replaying: true, records: []DNSDecisionRecord{}}
	response := states[0].answerObserved(&query, "", input.Stages[0].QueryAt, capture)
	if capture.err != nil {
		return DNSDecisionReplayResult{}, capture.err
	}
	if capture.stageCursor != len(input.Stages) {
		return DNSDecisionReplayResult{}, errors.New("unconsumed replay stages")
	}
	for _, stage := range input.Stages {
		for _, selection := range stage.Selections {
			if selection == nil || selection.Entropy.err != nil || selection.Entropy.cursor != len(selection.Entropy.Buckets) {
				return DNSDecisionReplayResult{}, errors.New("replay exploration inputs differ")
			}
		}
	}
	result = DNSDecisionReplayResult{DecisionID: receipt.DecisionID, RCode: response.Rcode, RRSet: dnsRRStrings(response.Answer), Authority: dnsRRStrings(response.Ns), Records: capture.records}
	result.Matched = result.RCode == receipt.RCode && reflect.DeepEqual(result.RRSet, receipt.RRSet) && reflect.DeepEqual(result.Authority, receipt.Authority) && reflect.DeepEqual(dnsRRStrings(dnsDecisionRedactOPT(response.Extra)), receipt.Additional) && reflect.DeepEqual(result.Records, receipt.Records) && reflect.DeepEqual(capture.answerPublication, receipt.AnswerPublication)
	if !result.Matched {
		return result, fmt.Errorf("recorded decision %s does not replay identically", receipt.DecisionID)
	}
	return result, nil
}

func ValidateDNSDecisionFilter(hostname, decisionID string, limit int) error {
	if limit < 1 || limit > 20 || len(decisionID) > 128 || strings.ContainsAny(decisionID, "\r\n\x00") {
		return errors.New("invalid decision filter")
	}
	if strings.ContainsAny(hostname, " \t\r\n\x00/\\") {
		return errors.New("invalid hostname")
	}
	if hostname != "" {
		if _, ok := dns.IsDomainName(hostname); !ok || len(hostname) > 253 {
			return errors.New("invalid hostname")
		}
	}
	return nil
}
