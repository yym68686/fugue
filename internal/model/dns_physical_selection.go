package model

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const DNSAnswerPolicyKindPhysicalQuality = "physical_quality"
const DNSAnswerPolicyKindPhysicalOrder = "physical_order"
const DNSPhysicalSelectionVersion = "physical-edge-network-v1"

type DNSPhysicalOrder struct {
	PrimarySince   *time.Time `json:"primary_since,omitempty"`
	Version        string     `json:"version"`
	OrderedEdgeIDs []string   `json:"ordered_edge_ids"`
}

func ValidateDNSPhysicalOrder(order *DNSPhysicalOrder) error {
	if order == nil || order.Version != "physical-order-v1" || len(order.OrderedEdgeIDs) == 0 || len(order.OrderedEdgeIDs) > 256 {
		return errors.New("invalid physical-edge order")
	}
	if order.PrimarySince != nil && order.PrimarySince.IsZero() {
		return errors.New("invalid preserved primary assignment epoch")
	}
	seen := map[string]bool{}
	for _, edgeID := range order.OrderedEdgeIDs {
		if edgeID == "" || len(edgeID) > 128 || strings.ContainsAny(edgeID, " \t\r\n\x00") || seen[edgeID] {
			return errors.New("invalid physical-edge order identity")
		}
		seen[edgeID] = true
	}
	return nil
}

func CloneDNSPhysicalOrder(order *DNSPhysicalOrder) *DNSPhysicalOrder {
	if order == nil {
		return nil
	}
	cloned := &DNSPhysicalOrder{Version: order.Version, OrderedEdgeIDs: append([]string(nil), order.OrderedEdgeIDs...)}
	if order.PrimarySince != nil {
		at := *order.PrimarySince
		cloned.PrimarySince = &at
	}
	return cloned
}

type DNSPhysicalSelection struct {
	QualityState   string     `json:"quality_state,omitempty"`
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
	if selection.QualityState != "" && selection.QualityState != "learning" && selection.QualityState != "measured" {
		return errors.New("invalid physical-edge quality state")
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
