package dnsserver

import (
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"
)

func TestDNSCheckpointReuseChecksActualFileAndCurrentTrust(t *testing.T) {
	s, parent, candidate := sharedDNSPayloadFixture(t, 0)
	record := dnsServingCheckpoint{Schema: "fugue.dns.positive-checkpoint/v1", NodeID: s.Config.DNSNodeID, GroupID: s.Config.EdgeGroupID, Parent: parent, Candidate: candidate, AppliedAt: time.Now().UTC(), Positive: true}
	if err := s.signDNSCheckpoint(&record); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	s.platformCheckpoint.Store(&dnsPersistedCheckpoint{sha256.Sum256(raw), record})
	if !s.validPreviousDNSCheckpoint(raw) {
		t.Fatal("valid materialized checkpoint rejected")
	}
	if s.validPreviousDNSCheckpoint(append(append([]byte{}, raw...), 'x')) {
		t.Fatal("corrupt replacement accepted")
	}
	s.Config.BundleRevokedKeyIDs = []string{s.Config.BundleSigningKeyID}
	if s.validPreviousDNSCheckpoint(raw) {
		t.Fatal("cache hid key revocation")
	}
	s.Config.BundleRevokedKeyIDs = nil
	record.Candidate.Artifact.Content["unexpected"] = true
	if !s.validPreviousDNSCheckpoint(raw) {
		t.Fatal("mutated memory prevented valid file recovery")
	}
	if s.validPreviousDNSCheckpoint([]byte(`{}`)) {
		t.Fatal("arbitrary file borrowed cached authority")
	}
}
