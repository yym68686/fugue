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
	APIVersion            string      `json:"apiVersion"`
	Kind                  string      `json:"kind"`
	Component             string      `json:"component"`
	GroupID               string      `json:"groupId"`
	RunID                 int64       `json:"runId"`
	RunAttempt            int         `json:"runAttempt"`
	TerminalReceiptDigest string      `json:"terminalReceiptDigest"`
	Final                 Observation `json:"final"`
	ReceiptDigest         string      `json:"receiptDigest"`
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
	wantDigest, err := receipt.digest()
	if err != nil || receipt.ReceiptDigest != wantDigest {
		return errors.New("ownership adoption receipt digest is invalid")
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
