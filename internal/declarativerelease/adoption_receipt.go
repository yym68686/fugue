package declarativerelease

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	OwnershipAdoptionReceiptAPIVersion = "release.fugue.dev/v2"
	OwnershipAdoptionReceiptKind       = "OwnershipAdoptionReceipt"
)

// OwnershipAdoptionReceipt is the durable boundary between the one-time
// adopting adapter and the ordinary independent component lane.
type OwnershipAdoptionReceipt struct {
	APIVersion            string                    `json:"apiVersion"`
	Kind                  string                    `json:"kind"`
	Component             string                    `json:"component"`
	GroupID               string                    `json:"groupId"`
	RunID                 int64                     `json:"runId"`
	RunAttempt            int                       `json:"runAttempt"`
	TerminalReceiptDigest string                    `json:"terminalReceiptDigest"`
	Final                 Observation               `json:"final"`
	Ownership             []OwnershipAdoptionScope  `json:"ownership,omitempty"`
	TerminalHandoff       *OwnershipTerminalHandoff `json:"terminalHandoff,omitempty"`
	ReceiptDigest         string                    `json:"receiptDigest"`
}

// OwnershipTerminalHandoff binds one reviewed, failed-no-write independent
// forward to the exact legacy leaves that may be removed before retrying it.
// It is an authorization witness only; the Kubernetes adapter must still
// prove the live typed conflict set and exclusive post-takeover ownership.
type OwnershipTerminalHandoff struct {
	RunID                 int64                              `json:"runId"`
	RunAttempt            int                                `json:"runAttempt"`
	FailedConfigSHA       string                             `json:"failedConfigSha"`
	ForwardImageRef       string                             `json:"forwardImageRef"`
	ArtifactReceiptDigest string                             `json:"artifactReceiptDigest"`
	Conflicts             []OwnershipTerminalHandoffConflict `json:"conflicts"`
	Scaffolds             []string                           `json:"scaffolds,omitempty"`
}

type OwnershipTerminalHandoffConflict struct {
	Pointer       string `json:"pointer"`
	LegacyManager string `json:"legacyManager"`
}

func BuildOwnershipAdoptionReceipt(result ExecutionResult, component Component, groupID string, runID int64, runAttempt int) (OwnershipAdoptionReceipt, error) {
	if result.Status != "verified" || result.Reason != "forward-verified" || result.Component != component.ID || result.ConfigSHA != result.Final.ConfigSHA ||
		!digestPattern.MatchString(result.ReceiptDigest) {
		return OwnershipAdoptionReceipt{}, errors.New("terminal execution result did not verify ownership adoption")
	}
	receipt := OwnershipAdoptionReceipt{
		APIVersion: OwnershipAdoptionReceiptAPIVersion, Kind: OwnershipAdoptionReceiptKind,
		Component: component.ID, GroupID: groupID, RunID: runID, RunAttempt: runAttempt,
		TerminalReceiptDigest: result.ReceiptDigest, Final: result.Final,
	}
	digest, err := receipt.digest()
	if err != nil {
		return OwnershipAdoptionReceipt{}, err
	}
	receipt.ReceiptDigest = digest
	if err := receipt.Validate(component, groupID); err != nil {
		return OwnershipAdoptionReceipt{}, err
	}
	return receipt, nil
}

func DecodeOwnershipAdoptionReceipt(reader io.Reader) (OwnershipAdoptionReceipt, error) {
	var receipt OwnershipAdoptionReceipt
	if err := decodeStrict(reader, &receipt); err != nil {
		return OwnershipAdoptionReceipt{}, fmt.Errorf("decode ownership adoption receipt: %w", err)
	}
	return receipt, nil
}

func (receipt OwnershipAdoptionReceipt) Validate(component Component, groupID string) error {
	if receipt.APIVersion != OwnershipAdoptionReceiptAPIVersion || receipt.Kind != OwnershipAdoptionReceiptKind ||
		receipt.Component != component.ID || receipt.GroupID != groupID || !edgeGroupIDPattern.MatchString(groupID) ||
		receipt.RunID <= 0 || receipt.RunAttempt != 1 || !digestPattern.MatchString(receipt.TerminalReceiptDigest) ||
		!digestPattern.MatchString(receipt.ReceiptDigest) {
		return errors.New("ownership adoption receipt identity is invalid")
	}
	final := receipt.Final
	wantPrimary := ResourceIdentity{APIVersion: component.Workload.APIVersion, Kind: component.Workload.Kind, Namespace: component.Workload.Namespace, Name: component.Workload.Name}
	activeDesired := final.Desired - int32(component.Workload.PreservedUnavailable)
	if !final.Present || final.Primary != wantPrimary || final.UID == "" || final.ResourceVersion == "" || final.Generation <= 0 ||
		final.ObservedGeneration != final.Generation || activeDesired <= 0 || final.Updated != activeDesired || final.Ready != activeDesired ||
		final.Available != activeDesired || final.Unavailable != int32(component.Workload.PreservedUnavailable) || !shaPattern.MatchString(final.ConfigSHA) || final.ManifestSHA != final.ConfigSHA ||
		final.OCIRevision != final.ConfigSHA || !strings.HasPrefix(final.ImageRef, component.Artifact.Repository+"@sha256:") ||
		!digestPattern.MatchString(final.ImageID) || !strings.HasSuffix(final.ImageRef, final.ImageID) || !digestPattern.MatchString(final.HealthDigest) ||
		!receiptContainsString(final.FieldManagers, component.Workload.FieldManager) {
		return errors.New("ownership adoption receipt workload is not independently healthy")
	}
	if len(final.Resources) == 0 {
		return errors.New("ownership adoption receipt has no resource ownership evidence")
	}
	for _, resource := range final.Resources {
		if !resource.Present || resource.UID == "" || resource.ResourceVersion == "" || !digestPattern.MatchString(resource.ObjectDigest) ||
			!receiptContainsString(resource.FieldManagers, component.Workload.FieldManager) {
			return fmt.Errorf("ownership adoption receipt resource %s/%s is unverified", resource.Identity.Kind, resource.Identity.Name)
		}
	}
	if component.Transition != nil && component.Transition.Type == "edge-group-ab" && component.Transition.EdgeGroupAB != nil {
		resources := make(map[ResourceIdentity]ResourceObservation, len(final.Resources))
		for _, resource := range final.Resources {
			resources[resource.Identity] = resource
		}
		for _, target := range component.ArtifactTargets {
			identity := ResourceIdentity{APIVersion: target.APIVersion, Kind: target.Kind, Namespace: target.Namespace, Name: target.Name}
			resource, exists := resources[identity]
			if !exists || !resource.ReviewedOwnershipApplied || !resource.ReviewedOwnershipExclusive {
				return fmt.Errorf("ownership adoption receipt resource %s/%s is not pointer-exclusive", identity.Kind, identity.Name)
			}
		}
	}
	if len(receipt.Ownership) > 0 {
		adoption := OwnershipAdoption{LegacyFieldManager: "receipt-proof", Resources: receipt.Ownership}
		if err := adoption.validate(component); err != nil {
			return fmt.Errorf("ownership adoption receipt scope: %w", err)
		}
		observed := make(map[ResourceIdentity]struct{}, len(final.Resources))
		for _, resource := range final.Resources {
			observed[resource.Identity] = struct{}{}
		}
		for _, scope := range receipt.Ownership {
			if _, exists := observed[scope.Identity]; !exists {
				return fmt.Errorf("ownership adoption receipt scope %s/%s has no resource witness", scope.Identity.Kind, scope.Identity.Name)
			}
		}
	}
	if receipt.TerminalHandoff != nil {
		handoff := receipt.TerminalHandoff
		if handoff.RunID <= 0 || handoff.RunAttempt != 1 || !shaPattern.MatchString(handoff.FailedConfigSHA) ||
			!strings.HasPrefix(handoff.ForwardImageRef, component.Artifact.Repository+"@sha256:") ||
			!digestPattern.MatchString(strings.TrimPrefix(handoff.ForwardImageRef, component.Artifact.Repository+"@")) ||
			!digestPattern.MatchString(handoff.ArtifactReceiptDigest) || len(handoff.Conflicts) == 0 {
			return errors.New("ownership terminal handoff identity is invalid")
		}
		seen := make(map[string]struct{}, len(handoff.Conflicts))
		for _, conflict := range handoff.Conflicts {
			if conflict.LegacyManager == "" {
				return errors.New("ownership terminal handoff legacy manager is empty")
			}
			if _, err := parseAdoptionPointer(conflict.Pointer); err != nil {
				return fmt.Errorf("ownership terminal handoff pointer: %w", err)
			}
			key := conflict.LegacyManager + "\x00" + conflict.Pointer
			if _, exists := seen[key]; exists {
				return errors.New("ownership terminal handoff conflict is duplicated")
			}
			seen[key] = struct{}{}
		}
		for _, pointer := range handoff.Scaffolds {
			if _, err := parseAdoptionPointer(pointer); err != nil {
				return fmt.Errorf("ownership terminal handoff scaffold: %w", err)
			}
			key := "scaffold\x00" + pointer
			if _, exists := seen[key]; exists {
				return errors.New("ownership terminal handoff scaffold is duplicated")
			}
			seen[key] = struct{}{}
		}
	}
	wantDigest, err := receipt.digest()
	if err != nil || receipt.ReceiptDigest != wantDigest {
		return fmt.Errorf("ownership adoption receipt digest is invalid: got %s want %s", receipt.ReceiptDigest, wantDigest)
	}
	return nil
}

func (receipt OwnershipAdoptionReceipt) digest() (string, error) {
	receipt.ReceiptDigest = ""
	raw, err := json.Marshal(receipt)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", digest), nil
}

func receiptContainsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
