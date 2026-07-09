package peerhealth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/localwal"
	"fugue/internal/model"
)

func NewSignal(issuerNodeID, subjectEdgeID, status string, evidence map[string]string, observedAt time.Time, ttl time.Duration) model.PeerHealthSignal {
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return model.PeerHealthSignal{
		SchemaVersion: model.AutonomySchemaVersionV1,
		SignalID:      model.NewID("peersig"),
		IssuerNodeID:  strings.TrimSpace(issuerNodeID),
		SubjectEdgeID: strings.TrimSpace(subjectEdgeID),
		Status:        normalizeStatus(status),
		Evidence:      cloneEvidence(evidence),
		EvidenceHash:  localwal.EvidenceHash(evidence),
		ObservedAt:    observedAt.UTC(),
		ExpiresAt:     observedAt.UTC().Add(ttl),
	}
}

func Sign(signal model.PeerHealthSignal, keyID string, secret []byte) (model.PeerHealthSignal, error) {
	if len(secret) == 0 {
		return model.PeerHealthSignal{}, fmt.Errorf("peer signal signing secret is required")
	}
	signal.KeyID = strings.TrimSpace(keyID)
	signal.Signature = ""
	payload, err := canonicalSignal(signal)
	if err != nil {
		return model.PeerHealthSignal{}, err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	signal.Signature = "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	return signal, nil
}

func Verify(signal model.PeerHealthSignal, secret []byte, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if signal.SchemaVersion != model.AutonomySchemaVersionV1 {
		return fmt.Errorf("unsupported peer signal schema_version %q", signal.SchemaVersion)
	}
	if strings.TrimSpace(signal.SignalID) == "" || strings.TrimSpace(signal.IssuerNodeID) == "" {
		return fmt.Errorf("peer signal id and issuer_node_id are required")
	}
	if signal.ExpiresAt.IsZero() || !now.Before(signal.ExpiresAt) {
		return fmt.Errorf("peer signal expired")
	}
	if got := localwal.EvidenceHash(signal.Evidence); signal.EvidenceHash == "" || !strings.EqualFold(got, signal.EvidenceHash) {
		return fmt.Errorf("peer signal evidence hash mismatch")
	}
	if len(secret) == 0 {
		return fmt.Errorf("peer signal verification secret is required")
	}
	signature := strings.TrimSpace(signal.Signature)
	if signature == "" {
		return fmt.Errorf("peer signal signature is required")
	}
	unsigned := signal
	unsigned.Signature = ""
	payload, err := canonicalSignal(unsigned)
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	want := "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(signature), []byte(want)) {
		return fmt.Errorf("peer signal signature mismatch")
	}
	return nil
}

func Decide(signals []model.PeerHealthSignal, now time.Time) model.PeerHealthDecision {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	active := make([]model.PeerHealthSignal, 0, len(signals))
	failureDomains := map[string]struct{}{}
	selfQuarantine := false
	subject := ""
	expiresAt := time.Time{}
	for _, signal := range signals {
		if signal.ExpiresAt.IsZero() || !now.Before(signal.ExpiresAt) {
			continue
		}
		status := normalizeStatus(signal.Status)
		if status != model.PeerSignalStatusSuspect &&
			status != model.PeerSignalStatusUnhealthy &&
			status != model.PeerSignalStatusSelfQuarantine {
			continue
		}
		active = append(active, signal)
		if subject == "" {
			subject = strings.TrimSpace(signal.SubjectEdgeID)
		}
		if signal.ExpiresAt.After(expiresAt) {
			expiresAt = signal.ExpiresAt
		}
		if strings.TrimSpace(signal.FailureDomain) != "" {
			failureDomains[strings.TrimSpace(signal.FailureDomain)] = struct{}{}
		}
		if status == model.PeerSignalStatusSelfQuarantine {
			selfQuarantine = true
		}
	}
	sort.SliceStable(active, func(i, j int) bool {
		return active[i].SignalID < active[j].SignalID
	})
	domains := sortedSet(failureDomains)
	decision := model.PeerHealthDecision{
		SubjectEdgeID:  subject,
		Decision:       model.PeerHealthDecisionClear,
		ExpiresAt:      expiresAt,
		SignalCount:    len(active),
		FailureDomains: domains,
		Signals:        active,
	}
	switch {
	case len(active) == 0:
		decision.Reason = "no_active_peer_signal"
	case selfQuarantine:
		decision.Decision = model.PeerHealthDecisionTemporaryFilter
		decision.Reason = "subject_self_quarantine"
	case len(domains) >= 2:
		decision.Decision = model.PeerHealthDecisionTemporaryFilter
		decision.Reason = "multi_failure_domain_peer_failure"
	default:
		decision.Decision = model.PeerHealthDecisionSuspect
		decision.Reason = "single_peer_failure_signal"
	}
	return decision
}

func PublicTLSProbeStatus(err error) string {
	if err == nil {
		return model.PeerSignalStatusPass
	}
	return model.PeerSignalStatusSuspect
}

func canonicalSignal(signal model.PeerHealthSignal) ([]byte, error) {
	if signal.EvidenceHash == "" {
		signal.EvidenceHash = localwal.EvidenceHash(signal.Evidence)
	}
	return json.Marshal(struct {
		SchemaVersion string            `json:"schema_version"`
		SignalID      string            `json:"signal_id"`
		IssuerNodeID  string            `json:"issuer_node_id"`
		IssuerEdgeID  string            `json:"issuer_edge_id,omitempty"`
		SubjectNodeID string            `json:"subject_node_id,omitempty"`
		SubjectEdgeID string            `json:"subject_edge_id,omitempty"`
		Status        string            `json:"status"`
		Scope         string            `json:"scope,omitempty"`
		FailureDomain string            `json:"failure_domain,omitempty"`
		EvidenceHash  string            `json:"evidence_hash"`
		ObservedAt    time.Time         `json:"observed_at"`
		ExpiresAt     time.Time         `json:"expires_at"`
		KeyID         string            `json:"key_id,omitempty"`
		Evidence      map[string]string `json:"evidence,omitempty"`
	}{
		SchemaVersion: signal.SchemaVersion,
		SignalID:      signal.SignalID,
		IssuerNodeID:  signal.IssuerNodeID,
		IssuerEdgeID:  signal.IssuerEdgeID,
		SubjectNodeID: signal.SubjectNodeID,
		SubjectEdgeID: signal.SubjectEdgeID,
		Status:        normalizeStatus(signal.Status),
		Scope:         signal.Scope,
		FailureDomain: signal.FailureDomain,
		EvidenceHash:  signal.EvidenceHash,
		ObservedAt:    signal.ObservedAt.UTC(),
		ExpiresAt:     signal.ExpiresAt.UTC(),
		KeyID:         signal.KeyID,
		Evidence:      cloneEvidence(signal.Evidence),
	})
}

func normalizeStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case model.PeerSignalStatusPass:
		return model.PeerSignalStatusPass
	case model.PeerSignalStatusUnhealthy:
		return model.PeerSignalStatusUnhealthy
	case model.PeerSignalStatusSelfQuarantine:
		return model.PeerSignalStatusSelfQuarantine
	case model.PeerSignalStatusSuspect:
		return model.PeerSignalStatusSuspect
	default:
		return model.PeerSignalStatusSuspect
	}
}

func cloneEvidence(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = strings.TrimSpace(value)
	}
	return out
}

func sortedSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
