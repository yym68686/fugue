package model

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const DNSAnswerPolicyKindPhysicalQuality = "physical_quality"
const DNSPhysicalSelectionVersion = "physical-edge-network-v1"

type DNSPhysicalSelection struct {
	Version        string     `json:"version"`
	PrimaryEdgeID  string     `json:"primary_edge_id"`
	OrderedEdgeIDs []string   `json:"ordered_edge_ids"`
	EvidenceDigest string     `json:"evidence_digest"`
	DNSReceiptID   string     `json:"dns_receipt_id"`
	LoadedDigest   string     `json:"loaded_digest"`
	PolicyDigest   string     `json:"policy_digest"`
	Scope          string     `json:"scope"`
	CapturedAt     time.Time  `json:"captured_at"`
	PrimarySince   *time.Time `json:"primary_since,omitempty"`
}

func ValidateDNSPhysicalSelection(selection *DNSPhysicalSelection) error {
	if selection == nil || selection.Version != DNSPhysicalSelectionVersion || selection.PrimaryEdgeID == "" ||
		len(selection.OrderedEdgeIDs) == 0 || len(selection.OrderedEdgeIDs) > 256 || selection.OrderedEdgeIDs[0] != selection.PrimaryEdgeID ||
		selection.DNSReceiptID == "" || len(selection.DNSReceiptID) > 256 || selection.Scope == "" || len(selection.Scope) > 512 || selection.CapturedAt.IsZero() {
		return errors.New("invalid physical-edge selection identity")
	}
	if selection.PrimarySince != nil && (selection.PrimarySince.IsZero() || selection.PrimarySince.After(selection.CapturedAt)) {
		return errors.New("invalid physical-edge primary assignment time")
	}
	for _, digest := range []string{selection.EvidenceDigest, selection.LoadedDigest, selection.PolicyDigest} {
		decoded, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
		if err != nil || len(decoded) != 32 || len(digest) != 71 || !strings.HasPrefix(digest, "sha256:") || digest != strings.ToLower(digest) {
			return errors.New("invalid physical-edge selection digest")
		}
	}
	seen := make(map[string]bool, len(selection.OrderedEdgeIDs))
	for _, edgeID := range selection.OrderedEdgeIDs {
		if edgeID == "" || len(edgeID) > 128 || strings.TrimSpace(edgeID) != edgeID || seen[edgeID] {
			return errors.New("invalid physical-edge selection order")
		}
		seen[edgeID] = true
	}
	return nil
}

func CloneDNSPhysicalSelection(selection *DNSPhysicalSelection) *DNSPhysicalSelection {
	if selection == nil {
		return nil
	}
	cloned := *selection
	cloned.OrderedEdgeIDs = append([]string(nil), selection.OrderedEdgeIDs...)
	if selection.PrimarySince != nil {
		primarySince := *selection.PrimarySince
		cloned.PrimarySince = &primarySince
	}
	return &cloned
}
