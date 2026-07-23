// Package nodelocalstate defines the immutable, fail-closed NodeLocal
// membership contract. It is intentionally dormant until a later consumer
// checkpoint wires it to Platform State.
package nodelocalstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
)

const (
	ScopeKey               = "production/cluster-dns"
	ContractKindMembership = "node_local_dns_membership"
	ContractVersionV1      = "nodelocal-membership-v1"
	ControllerCapabilityV1 = "nodelocal-membership-v1"

	TransitionNoop              = "noop"
	TransitionAddOneShadow      = "add_one_shadow"
	TransitionWorkloadUpdate    = "workload_contract_update"
	TransitionEnforcementUpdate = "enforcement_update"

	EnforcementReportOnly = "report_only"
	EnforcementEnabled    = "enforced"
	ActiveModeShadow      = "shadow"
	PreservedModeIptables = "iptables"
)

var (
	generationPattern = regexp.MustCompile(`^nodelocal-[0-9]{4,}$`)
	nodeNamePattern   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
	nodeUIDPattern    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	stableIDPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type ValidationError struct{ Code string }

func (e ValidationError) Error() string { return "invalid NodeLocal state contract: " + e.Code }

type NodeIdentity struct {
	FugueNodeID           string `json:"fugue_node_id"`
	KubernetesName        string `json:"kubernetes_name"`
	ExpectedKubernetesUID string `json:"expected_kubernetes_uid"`
}

type Compatibility struct {
	ControllerFloor        string `json:"controller_floor"`
	WorkloadContractDigest string `json:"workload_contract_digest"`
}

type Transition struct {
	Type string        `json:"type"`
	Node *NodeIdentity `json:"node,omitempty"`
}

type DesiredState struct {
	ActiveMode        string         `json:"active_mode"`
	PreservedMode     string         `json:"preserved_mode"`
	ActiveShadow      []NodeIdentity `json:"active_shadow"`
	PreservedIptables []NodeIdentity `json:"preserved_iptables"`
}

type Enforcement struct {
	Mode string `json:"mode"`
}

type AuditIntent struct {
	ActorType string `json:"actor_type"`
	ActorID   string `json:"actor_id"`
	Reason    string `json:"reason"`
	ChangeID  string `json:"change_id"`
}

type Document struct {
	ContractKind       string        `json:"contract_kind"`
	ContractVersion    string        `json:"contract_version"`
	Generation         string        `json:"generation"`
	BaseGeneration     string        `json:"base_generation"`
	PreviousGeneration string        `json:"previous_generation"`
	RollbackGeneration string        `json:"rollback_generation"`
	Compatibility      Compatibility `json:"compatibility"`
	Transition         Transition    `json:"transition"`
	Desired            DesiredState  `json:"desired"`
	Enforcement        Enforcement   `json:"enforcement"`
	Audit              AuditIntent   `json:"audit"`
}

type TransitionDecision struct {
	Allowed     bool
	Type        string
	Added       []NodeIdentity
	Removed     []NodeIdentity
	ReasonCodes []string
}

func Decode(content map[string]any) (Document, error) {
	if content == nil {
		return Document{}, validationError("content_missing")
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return Document{}, validationError("content_encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var document Document
	if err := decoder.Decode(&document); err != nil {
		return Document{}, validationError("content_schema")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Document{}, validationError("content_trailing_data")
	}
	if err := Validate(document); err != nil {
		return Document{}, err
	}
	return document, nil
}

func DecodeForArtifact(content map[string]any, generation, scopeKey, compatibilityFloor string) (Document, error) {
	if strings.TrimSpace(strings.ToLower(scopeKey)) != ScopeKey {
		return Document{}, validationError("scope_mismatch")
	}
	if strings.TrimSpace(compatibilityFloor) != "v1" {
		return Document{}, validationError("artifact_compatibility_floor")
	}
	document, err := Decode(content)
	if err != nil {
		return Document{}, err
	}
	if document.Generation != strings.TrimSpace(generation) {
		return Document{}, validationError("generation_envelope_mismatch")
	}
	return document, nil
}

func Validate(document Document) error {
	switch {
	case document.ContractKind != ContractKindMembership:
		return validationError("contract_kind")
	case document.ContractVersion != ContractVersionV1:
		return validationError("contract_version")
	case !validGeneration(document.Generation):
		return validationError("generation")
	case !validGeneration(document.BaseGeneration):
		return validationError("base_generation")
	case !validGeneration(document.PreviousGeneration):
		return validationError("previous_generation")
	case !validGeneration(document.RollbackGeneration):
		return validationError("rollback_generation")
	case document.Compatibility.ControllerFloor != ControllerCapabilityV1:
		return validationError("controller_floor")
	case !digestPattern.MatchString(document.Compatibility.WorkloadContractDigest):
		return validationError("workload_contract_digest")
	case document.Desired.ActiveMode != ActiveModeShadow:
		return validationError("active_mode")
	case document.Desired.PreservedMode != PreservedModeIptables:
		return validationError("preserved_mode")
	case document.Enforcement.Mode != EnforcementReportOnly && document.Enforcement.Mode != EnforcementEnabled:
		return validationError("enforcement_mode")
	}
	if document.Desired.ActiveShadow == nil || document.Desired.PreservedIptables == nil {
		return validationError("cohort_missing")
	}
	if len(document.Desired.ActiveShadow) == 0 || len(document.Desired.PreservedIptables) == 0 {
		return validationError("cohort_empty")
	}
	if err := validateNodeList(document.Desired.ActiveShadow, "active_shadow"); err != nil {
		return err
	}
	if err := validateNodeList(document.Desired.PreservedIptables, "preserved_iptables"); err != nil {
		return err
	}
	activeNames, activeIDs, activeUIDs := nodeNameSet(document.Desired.ActiveShadow), nodeIDSet(document.Desired.ActiveShadow), nodeUIDSet(document.Desired.ActiveShadow)
	for _, node := range document.Desired.PreservedIptables {
		if _, ok := activeNames[node.KubernetesName]; ok {
			return validationError("desired_overlap")
		}
		if _, ok := activeIDs[node.FugueNodeID]; ok {
			return validationError("desired_id_overlap")
		}
		if _, ok := activeUIDs[node.ExpectedKubernetesUID]; ok {
			return validationError("desired_uid_overlap")
		}
	}
	switch document.Transition.Type {
	case TransitionNoop, TransitionWorkloadUpdate, TransitionEnforcementUpdate:
		if document.Transition.Node != nil {
			return validationError("non_membership_node_present")
		}
	case TransitionAddOneShadow:
		if document.Transition.Node == nil {
			return validationError("transition_node_missing")
		}
		if err := validateNode(*document.Transition.Node); err != nil {
			return validationError("transition_node_invalid")
		}
	default:
		return validationError("transition_type")
	}
	if document.Audit.ActorType != strings.TrimSpace(document.Audit.ActorType) || document.Audit.ActorID != strings.TrimSpace(document.Audit.ActorID) || document.Audit.ChangeID != strings.TrimSpace(document.Audit.ChangeID) || !stableIDPattern.MatchString(document.Audit.ActorType) || !stableIDPattern.MatchString(document.Audit.ActorID) || !stableIDPattern.MatchString(document.Audit.ChangeID) {
		return validationError("audit_identity")
	}
	if reason := strings.TrimSpace(document.Audit.Reason); reason != document.Audit.Reason || len(reason) < 8 || len(reason) > 512 {
		return validationError("audit_reason")
	}
	return nil
}

func ValidStableID(value string) bool {
	return value == strings.TrimSpace(value) && stableIDPattern.MatchString(value)
}

func IsBootstrapNoop(document Document) bool {
	return document.Transition.Type == TransitionNoop && document.Enforcement.Mode == EnforcementReportOnly && document.BaseGeneration == document.Generation && document.PreviousGeneration == document.Generation && document.RollbackGeneration == document.Generation
}

func ClassifyTransition(base, target Document) TransitionDecision {
	reasons := make([]string, 0)
	if err := Validate(base); err != nil {
		reasons = append(reasons, "base_invalid")
	}
	if err := Validate(target); err != nil {
		reasons = append(reasons, "target_invalid")
	}
	if len(reasons) != 0 {
		return transitionDecision(false, "rejected", nil, nil, reasons)
	}
	if target.Generation == base.Generation {
		reasons = append(reasons, "generation_not_advanced")
	}
	if target.BaseGeneration != base.Generation || target.PreviousGeneration != base.Generation {
		reasons = append(reasons, "base_generation_mismatch")
	}
	compatibilityChanged := target.Compatibility != base.Compatibility
	enforcementChanged := target.Enforcement != base.Enforcement
	if target.Desired.ActiveMode != base.Desired.ActiveMode || target.Desired.PreservedMode != base.Desired.PreservedMode {
		reasons = append(reasons, "mode_changed")
	}
	if !equalNodeLists(base.Desired.PreservedIptables, target.Desired.PreservedIptables) {
		reasons = append(reasons, "preserved_membership_changed")
	}
	added, removed := nodeListDelta(base.Desired.ActiveShadow, target.Desired.ActiveShadow)
	wantedType := TransitionNoop
	switch {
	case len(added) == 1 && len(removed) == 0:
		wantedType = TransitionAddOneShadow
		if compatibilityChanged || enforcementChanged {
			reasons = append(reasons, "membership_transition_mixed")
		}
	case len(added) == 0 && len(removed) == 0 && compatibilityChanged && !enforcementChanged:
		wantedType = TransitionWorkloadUpdate
	case len(added) == 0 && len(removed) == 0 && !compatibilityChanged && enforcementChanged:
		wantedType = TransitionEnforcementUpdate
	case len(added) == 0 && len(removed) == 0 && !compatibilityChanged && !enforcementChanged:
		wantedType = TransitionNoop
	default:
		reasons = append(reasons, "transition_dimensions_mixed")
	}
	if target.Transition.Type != wantedType {
		reasons = append(reasons, "transition_classification_mismatch")
	}
	if wantedType == TransitionAddOneShadow && (target.Transition.Node == nil || !equalNode(*target.Transition.Node, added[0])) {
		reasons = append(reasons, "transition_node_mismatch")
	}
	if wantedType != TransitionAddOneShadow && target.Transition.Node != nil {
		reasons = append(reasons, "non_membership_node_present")
	}
	if enforcementChanged && (wantedType != TransitionEnforcementUpdate || base.Enforcement.Mode != EnforcementReportOnly || target.Enforcement.Mode != EnforcementEnabled) {
		reasons = append(reasons, "enforcement_transition_invalid")
	}
	return transitionDecision(len(reasons) == 0, wantedType, added, removed, reasons)
}

func ValidateTransition(base *Document, target Document, lkgGeneration string) error {
	if err := Validate(target); err != nil {
		return err
	}
	if base == nil {
		if !IsBootstrapNoop(target) {
			return validationError("bootstrap_not_noop")
		}
		return nil
	}
	if decision := ClassifyTransition(*base, target); !decision.Allowed {
		return validationError("transition_rejected")
	}
	if strings.TrimSpace(lkgGeneration) == "" || target.RollbackGeneration != strings.TrimSpace(lkgGeneration) {
		return validationError("rollback_generation_mismatch")
	}
	return nil
}

func ValidateBaseTargetLive(base, target Document, liveGeneration, recordedGeneration string) error {
	if strings.TrimSpace(liveGeneration) != base.Generation || strings.TrimSpace(recordedGeneration) != base.Generation {
		return validationError("base_live_record_mismatch")
	}
	if decision := ClassifyTransition(base, target); !decision.Allowed {
		return validationError("transition_rejected")
	}
	return nil
}

func CanonicalJSON(document Document) ([]byte, error) {
	if err := Validate(document); err != nil {
		return nil, err
	}
	return json.Marshal(document)
}

func Digest(document Document) (string, error) {
	data, err := CanonicalJSON(document)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validationError(code string) error { return ValidationError{Code: code} }

func validGeneration(value string) bool {
	return generationPattern.MatchString(value)
}

func validateNodeList(nodes []NodeIdentity, code string) error {
	for index, node := range nodes {
		if err := validateNode(node); err != nil {
			return validationError(code + "_node")
		}
		if index > 0 && nodes[index-1].KubernetesName >= node.KubernetesName {
			return validationError(code + "_order")
		}
	}
	ids, uids := map[string]struct{}{}, map[string]struct{}{}
	for _, node := range nodes {
		if _, exists := ids[node.FugueNodeID]; exists {
			return validationError(code + "_duplicate_id")
		}
		if _, exists := uids[node.ExpectedKubernetesUID]; exists {
			return validationError(code + "_duplicate_uid")
		}
		ids[node.FugueNodeID] = struct{}{}
		uids[node.ExpectedKubernetesUID] = struct{}{}
	}
	return nil
}

func validateNode(node NodeIdentity) error {
	if !stableIDPattern.MatchString(node.FugueNodeID) || !validNodeName(node.KubernetesName) || !nodeUIDPattern.MatchString(node.ExpectedKubernetesUID) {
		return validationError("node_identity")
	}
	return nil
}

func validNodeName(name string) bool {
	if len(name) < 1 || len(name) > 253 || !nodeNamePattern.MatchString(name) {
		return false
	}
	for _, segment := range strings.Split(name, ".") {
		if len(segment) < 1 || len(segment) > 63 {
			return false
		}
	}
	return true
}

func nodeNameSet(nodes []NodeIdentity) map[string]struct{} {
	set := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		set[node.KubernetesName] = struct{}{}
	}
	return set
}

func nodeIDSet(nodes []NodeIdentity) map[string]struct{} {
	set := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		set[node.FugueNodeID] = struct{}{}
	}
	return set
}

func nodeUIDSet(nodes []NodeIdentity) map[string]struct{} {
	set := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		set[node.ExpectedKubernetesUID] = struct{}{}
	}
	return set
}

func nodeListDelta(base, target []NodeIdentity) ([]NodeIdentity, []NodeIdentity) {
	baseByName, targetByName := map[string]NodeIdentity{}, map[string]NodeIdentity{}
	for _, node := range base {
		baseByName[node.KubernetesName] = node
	}
	for _, node := range target {
		targetByName[node.KubernetesName] = node
	}
	added, removed := []NodeIdentity{}, []NodeIdentity{}
	for name, node := range targetByName {
		if baseNode, found := baseByName[name]; !found || !equalNode(baseNode, node) {
			added = append(added, node)
		}
	}
	for name, node := range baseByName {
		if targetNode, found := targetByName[name]; !found || !equalNode(targetNode, node) {
			removed = append(removed, node)
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].KubernetesName < added[j].KubernetesName })
	sort.Slice(removed, func(i, j int) bool { return removed[i].KubernetesName < removed[j].KubernetesName })
	return added, removed
}

func equalNode(left, right NodeIdentity) bool { return left == right }

func equalNodeLists(left, right []NodeIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !equalNode(left[index], right[index]) {
			return false
		}
	}
	return true
}

func transitionDecision(allowed bool, transitionType string, added, removed []NodeIdentity, reasons []string) TransitionDecision {
	return TransitionDecision{Allowed: allowed, Type: transitionType, Added: append([]NodeIdentity(nil), added...), Removed: append([]NodeIdentity(nil), removed...), ReasonCodes: canonicalStrings(reasons)}
}

func canonicalStrings(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			set[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
