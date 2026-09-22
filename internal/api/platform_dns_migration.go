package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

type platformDNSMigrationComparison struct {
	ArtifactID                 string                           `json:"artifact_id"`
	ArtifactDigest             string                           `json:"artifact_digest"`
	ArtifactGeneration         string                           `json:"artifact_generation"`
	NodeID                     string                           `json:"node_id"`
	EdgeGroupID                string                           `json:"edge_group_id"`
	Zone                       string                           `json:"zone"`
	SourceGeneration           string                           `json:"source_generation"`
	SourceDigest               string                           `json:"source_digest"`
	SourceScopeKey             string                           `json:"source_scope_key"`
	CapturedAt                 time.Time                        `json:"captured_at"`
	SourceRecordCount          int                              `json:"source_record_count"`
	ArtifactRecordCount        int                              `json:"artifact_record_count"`
	MatchingRecordCount        int                              `json:"matching_record_count"`
	ExpiredCandidateValueCount int                              `json:"expired_candidate_value_count"`
	Equivalent                 bool                             `json:"equivalent"`
	Differences                []platformDNSMigrationDifference `json:"differences"`
}

type platformDNSMigrationDifference struct {
	Hostname string   `json:"hostname"`
	Type     string   `json:"type"`
	Kind     string   `json:"kind"`
	Fields   []string `json:"fields"`
}

func (s *Server) handleComparePlatformDNSMigration(w http.ResponseWriter, r *http.Request) {
	if !mustPrincipal(r).IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	id, nodeID, zone := strings.TrimSpace(r.URL.Query().Get("artifact_id")), strings.TrimSpace(r.URL.Query().Get("node_id")), r.URL.Query().Get("zone")
	if id == "" || len(id) > 256 || nodeID == "" || len(nodeID) > 256 || zone == "" || len(zone) > 253 || zone != normalizeExternalAppDomain(zone) {
		httpx.WriteError(w, http.StatusBadRequest, "artifact_id, physical node_id and canonical zone are required")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "artifact not found")
		} else {
			httpx.WriteError(w, http.StatusServiceUnavailable, "artifact storage unavailable")
		}
		return
	}
	if artifact.ID != id || artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || artifact.ScopeKey != "global" || artifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()).Pass {
		httpx.WriteError(w, http.StatusConflict, "comparison requires an exact trusted validated global DNS artifact")
		return
	}
	records, group, err := platformDNSArtifactView(artifact, nodeID, zone)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, "DNS artifact has no compatible exact consumer view")
		return
	}
	nodes, err := s.store.ListDNSNodes(group)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS inventory unavailable")
		return
	}
	var reference *model.DNSNode
	for i := range nodes {
		node := &nodes[i]
		if firstNonEmpty(node.PhysicalNodeID, node.ID) == nodeID && node.EdgeGroupID == group && node.Zone == zone {
			if reference != nil {
				httpx.WriteError(w, http.StatusServiceUnavailable, "DNS zone alias ownership is ambiguous")
				return
			}
			reference = node
		}
	}
	if reference == nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS reference zone identity is unavailable")
		return
	}
	options, ok := s.edgeDNSBundleOptionsForDNSNode(*reference)
	if !ok {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS reference endpoint is unavailable")
		return
	}
	now := time.Now().UTC()
	source, found, err := s.edgeDNSBundleArtifactForOptions(options, now)
	if err != nil || !found {
		httpx.WriteError(w, http.StatusServiceUnavailable, "trusted published DNS reference is unavailable")
		return
	}
	comparison, err := comparePlatformDNSRecords(source.Records, records, now)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS records cannot be compared unambiguously")
		return
	}
	comparison.ArtifactID, comparison.ArtifactDigest, comparison.ArtifactGeneration = artifact.ID, artifact.ContentHash, artifact.Generation
	comparison.NodeID, comparison.EdgeGroupID, comparison.Zone = nodeID, group, zone
	comparison.SourceGeneration, comparison.SourceScopeKey = source.Generation, edgeDNSBundleArtifactScopeKey(options)
	comparison.SourceDigest, err = platformconfig.Digest(source)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS reference digest unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, comparison)
}

func comparePlatformDNSRecords(source, candidate []model.EdgeDNSRecord, now time.Time) (platformDNSMigrationComparison, error) {
	result := platformDNSMigrationComparison{CapturedAt: now, SourceRecordCount: len(source), ArtifactRecordCount: len(candidate), Differences: []platformDNSMigrationDifference{}}
	left, _, err := dnsMigrationRecordIndex(source, now)
	if err != nil {
		return result, err
	}
	right, expired, err := dnsMigrationRecordIndex(candidate, now)
	if err != nil {
		return result, err
	}
	result.ExpiredCandidateValueCount = expired
	keys := map[string]bool{}
	for k := range left {
		keys[k] = true
	}
	for k := range right {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		parts := strings.SplitN(key, "\x00", 2)
		difference := platformDNSMigrationDifference{Hostname: parts[0], Type: parts[1], Fields: []string{}}
		l, lok := left[key]
		rr, rok := right[key]
		switch {
		case !rok:
			difference.Kind = "missing_from_artifact"
		case !lok:
			difference.Kind = "extra_in_artifact"
		default:
			fields := map[string]bool{}
			for k := range l {
				fields[k] = true
			}
			for k := range rr {
				fields[k] = true
			}
			for k := range fields {
				if !reflect.DeepEqual(l[k], rr[k]) {
					difference.Fields = append(difference.Fields, k)
				}
			}
			if len(difference.Fields) == 0 {
				result.MatchingRecordCount++
				continue
			}
			sort.Strings(difference.Fields)
			difference.Kind = "changed"
		}
		result.Differences = append(result.Differences, difference)
	}
	result.Equivalent = len(result.Differences) == 0
	return result, nil
}

func dnsMigrationRecordIndex(records []model.EdgeDNSRecord, now time.Time) (map[string]map[string]any, int, error) {
	if now.IsZero() {
		return nil, 0, fmt.Errorf("DNS comparison requires observation time")
	}
	base := make([]platformconfig.DNSIntent, 0, len(records))
	for _, r := range records {
		base = append(base, platformconfig.DNSIntent{Hostname: r.Name, Type: r.Type, Values: r.Values, TTL: r.TTL, ValueExpirations: r.ValueExpirations})
	}
	if err := platformconfig.ValidateDNSIntents(base); err != nil {
		return nil, 0, err
	}
	out := make(map[string]map[string]any, len(records))
	expired := 0
	for i, record := range records {
		if len(record.ValueExpirations) > 0 && (len(record.Candidates) > 0 || len(record.ScopedCandidates) > 0) {
			return nil, 0, fmt.Errorf("leased DNS records cannot contain dynamic candidates")
		}
		active, err := platformconfig.DNSRecordsAt([]platformconfig.DNSIntent{base[i]}, now)
		if err != nil {
			return nil, 0, err
		}
		count := 0
		if len(active) == 0 {
			record.Values = nil
		} else {
			record.Values = append([]string(nil), active[0].Values...)
			record.TTL = active[0].TTL
			count = len(record.Values)
		}
		expired += len(records[i].Values) - count
		sort.Strings(record.Values)
		record.RecordGeneration = ""
		raw, err := json.Marshal(record)
		if err != nil {
			return nil, 0, err
		}
		var fields map[string]any
		if err = json.Unmarshal(raw, &fields); err != nil {
			return nil, 0, err
		}
		delete(fields, "record_generation")
		out[record.Name+"\x00"+record.Type] = fields
	}
	return out, expired, nil
}
