package secretdryrunrequest

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

const (
	ReceiptAPIVersion = "backup-materializer-secret-dry-run.fugue.dev/v1"
	ReceiptKind       = "BackupObserverSecretDryRunResult"
	ReceiptPolicy     = "single-secret-create-or-resource-version-cas-dry-run-v1"
)

var ErrReceipt = errors.New("invalid backup materializer Secret dry-run receipt")

// Receipt is the secret-free acknowledgement that an API server accepted one
// exact request through authorization, admission, validation, and conflict
// checks without storage. Kubernetes-generated response fields are omitted
// because dry-run values are not stable evidence of a future persisted object.
type Receipt struct {
	APIVersion                string           `json:"apiVersion"`
	Kind                      string           `json:"kind"`
	Policy                    string           `json:"policy"`
	Namespace                 string           `json:"namespace"`
	SecretName                string           `json:"secretName"`
	CellKey                   string           `json:"cellKey"`
	CellID                    string           `json:"cellId"`
	Action                    reconcile.Action `json:"action"`
	PlanDigest                string           `json:"planDigest"`
	DecisionDigest            string           `json:"decisionDigest"`
	RequestDigest             string           `json:"requestDigest"`
	IdempotencyKey            string           `json:"idempotencyKey"`
	ValidatedAt               time.Time        `json:"validatedAt"`
	Accepted                  bool             `json:"accepted"`
	ServerSideDryRun          bool             `json:"serverSideDryRun"`
	Persisted                 bool             `json:"persisted"`
	DeleteAllowed             bool             `json:"deleteAllowed"`
	ExecutionAllowed          bool             `json:"executionAllowed"`
	ProductionMutationAllowed bool             `json:"productionMutationAllowed"`
	Digest                    string           `json:"digest"`
}

func ValidateReceipt(receipt Receipt) error {
	identity, err := materialization.SecretIdentityForCell(receipt.CellKey)
	if err != nil || receipt.APIVersion != ReceiptAPIVersion || receipt.Kind != ReceiptKind ||
		receipt.Policy != ReceiptPolicy || receipt.Namespace != identity.Namespace ||
		receipt.SecretName != identity.SecretName || receipt.CellID != identity.CellID ||
		(receipt.Action != reconcile.ActionCreateIfAbsent && receipt.Action != reconcile.ActionReplaceResourceVersionCAS) ||
		!validDigest(receipt.PlanDigest) || !validDigest(receipt.DecisionDigest) || !validDigest(receipt.RequestDigest) ||
		receipt.IdempotencyKey != IdempotencyKey(receipt.CellID, receipt.DecisionDigest) ||
		!canonicalTime(receipt.ValidatedAt) || !receipt.Accepted || !receipt.ServerSideDryRun || receipt.Persisted ||
		receipt.DeleteAllowed || receipt.ExecutionAllowed || receipt.ProductionMutationAllowed ||
		receipt.Digest != DigestReceipt(receipt) {
		return ErrReceipt
	}
	return nil
}

func DigestReceipt(receipt Receipt) string {
	receipt.Digest = ""
	document, err := json.Marshal(receipt)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

func (receipt Receipt) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretDryRunResult{cell=%q action=%q accepted=%t persisted=false executionAllowed=false digest=%q}",
		receipt.CellKey,
		receipt.Action,
		receipt.Accepted,
		receipt.Digest,
	)
}

func (receipt Receipt) GoString() string { return receipt.String() }
