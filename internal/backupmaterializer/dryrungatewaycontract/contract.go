// Package dryrungatewaycontract defines the explicit private wire envelope
// between one backup validator cell and a future cell-local Secret dry-run
// gateway. It owns no context, credential, filesystem, network, HTTP,
// Kubernetes client, datastore, process, retry, or live-mutation capability.
package dryrungatewaycontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/secretdryrunrequest"
)

const (
	APIVersion = "backup-materializer-dry-run-gateway.fugue.dev/v1"
	Kind       = "BackupObserverSecretDryRunGatewayRequest"
	Policy     = "loopback-cell-one-shot-secret-dry-run-v1"
	RoutePath  = "/v1/secret-dry-run"
	MediaType  = "application/json"

	MaximumRequestBytes = 192 << 10
	MaximumReceiptBytes = 16 << 10
)

var ErrContract = errors.New("invalid backup materializer dry-run gateway contract")

// wireRequest is deliberately private because SecretDocument contains the raw
// observer spec and bearer token. EncodeRequest is the only serialization
// egress and DecodeRequest is the only ingress.
type wireRequest struct {
	APIVersion                string                       `json:"apiVersion"`
	Kind                      string                       `json:"kind"`
	Policy                    string                       `json:"policy"`
	CellKey                   string                       `json:"cellKey"`
	CellID                    string                       `json:"cellId"`
	Request                   secretdryrunrequest.Evidence `json:"request"`
	SecretDocument            json.RawMessage              `json:"secretDocument"`
	IdempotencyKey            string                       `json:"idempotencyKey"`
	OneShot                   bool                         `json:"oneShot"`
	RetriesAllowed            bool                         `json:"retriesAllowed"`
	ProductionMutationAllowed bool                         `json:"productionMutationAllowed"`
	Digest                    string                       `json:"digest"`
}

// EncodeRequest opens one already validated request and emits its canonical,
// bounded private envelope. The returned bytes are secret-bearing and callers
// must never log, cache, persist, or place them in an error.
func EncodeRequest(request secretdryrunrequest.Request, expectedCellKey string, now time.Time) ([]byte, error) {
	evidence, secretDocument, err := request.Open(expectedCellKey, now)
	if err != nil {
		return nil, ErrContract
	}
	wire := wireRequest{
		APIVersion: APIVersion,
		Kind:       Kind,
		Policy:     Policy,
		CellKey:    evidence.CellKey,
		CellID:     evidence.CellID,
		Request:    evidence,
		SecretDocument: append(
			json.RawMessage(nil),
			secretDocument...,
		),
		IdempotencyKey:            evidence.IdempotencyKey,
		OneShot:                   true,
		RetriesAllowed:            false,
		ProductionMutationAllowed: false,
	}
	wire.Digest = digestWire(wire)
	document, err := json.Marshal(wire)
	if err != nil || len(document) == 0 || len(document) > MaximumRequestBytes {
		return nil, ErrContract
	}
	if _, err := DecodeRequest(document, expectedCellKey, now); err != nil {
		return nil, ErrContract
	}
	return append([]byte(nil), document...), nil
}

// DecodeRequest applies a strict canonical JSON boundary, validates every
// redundant cell/idempotency/safety field, and restores the immutable private
// request. It grants no transport or execution capability.
func DecodeRequest(document []byte, expectedCellKey string, now time.Time) (secretdryrunrequest.Request, error) {
	if len(document) == 0 || len(document) > MaximumRequestBytes {
		return secretdryrunrequest.Request{}, ErrContract
	}
	var wire wireRequest
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return secretdryrunrequest.Request{}, ErrContract
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return secretdryrunrequest.Request{}, ErrContract
	}
	canonicalDocument, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonicalDocument, document) {
		return secretdryrunrequest.Request{}, ErrContract
	}
	identity, err := materialization.SecretIdentityForCell(expectedCellKey)
	if err != nil || wire.APIVersion != APIVersion || wire.Kind != Kind || wire.Policy != Policy ||
		wire.CellKey != identity.CellKey || wire.CellID != identity.CellID ||
		wire.Request.CellKey != wire.CellKey || wire.Request.CellID != wire.CellID ||
		wire.IdempotencyKey != wire.Request.IdempotencyKey ||
		wire.IdempotencyKey != secretdryrunrequest.IdempotencyKey(wire.CellID, wire.Request.DecisionDigest) ||
		len(wire.SecretDocument) == 0 || !wire.OneShot || wire.RetriesAllowed || wire.ProductionMutationAllowed ||
		!validDigest(wire.Digest) || wire.Digest != digestWire(wire) {
		return secretdryrunrequest.Request{}, ErrContract
	}
	request, err := secretdryrunrequest.Restore(
		wire.Request,
		append([]byte(nil), wire.SecretDocument...),
		expectedCellKey,
		now,
	)
	if err != nil {
		return secretdryrunrequest.Request{}, ErrContract
	}
	return request, nil
}

// EncodeReceipt emits the canonical, secret-free acknowledgement for this
// exact request. A structurally valid receipt from another cell or reconcile
// attempt is rejected before it can cross the future gateway boundary.
func EncodeReceipt(
	receipt secretdryrunrequest.Receipt,
	request secretdryrunrequest.Request,
	expectedCellKey string,
	now time.Time,
) ([]byte, error) {
	if validateReceiptForRequest(receipt, request, expectedCellKey, now) != nil {
		return nil, ErrContract
	}
	document, err := json.Marshal(receipt)
	if err != nil || len(document) == 0 || len(document) > MaximumReceiptBytes {
		return nil, ErrContract
	}
	if _, err := DecodeReceipt(document, request, expectedCellKey, now); err != nil {
		return nil, ErrContract
	}
	return append([]byte(nil), document...), nil
}

// DecodeReceipt enforces one canonical JSON value and binds it to the exact
// sealed request that produced the gateway call. It never accepts a receipt
// merely because the receipt is independently well formed.
func DecodeReceipt(
	document []byte,
	request secretdryrunrequest.Request,
	expectedCellKey string,
	now time.Time,
) (secretdryrunrequest.Receipt, error) {
	if len(document) == 0 || len(document) > MaximumReceiptBytes {
		return secretdryrunrequest.Receipt{}, ErrContract
	}
	var receipt secretdryrunrequest.Receipt
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return secretdryrunrequest.Receipt{}, ErrContract
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return secretdryrunrequest.Receipt{}, ErrContract
	}
	canonicalDocument, err := json.Marshal(receipt)
	if err != nil || !bytes.Equal(canonicalDocument, document) ||
		validateReceiptForRequest(receipt, request, expectedCellKey, now) != nil {
		return secretdryrunrequest.Receipt{}, ErrContract
	}
	return receipt, nil
}

func validateReceiptForRequest(
	receipt secretdryrunrequest.Receipt,
	request secretdryrunrequest.Request,
	expectedCellKey string,
	now time.Time,
) error {
	evidence, _, err := request.Open(expectedCellKey, now)
	if err != nil || secretdryrunrequest.ValidateReceipt(receipt) != nil ||
		receipt.Namespace != evidence.Namespace || receipt.SecretName != evidence.SecretName ||
		receipt.CellKey != evidence.CellKey || receipt.CellID != evidence.CellID ||
		receipt.Action != evidence.Action || receipt.PlanDigest != evidence.PlanDigest ||
		receipt.DecisionDigest != evidence.DecisionDigest || receipt.RequestDigest != evidence.RequestDigest ||
		receipt.IdempotencyKey != evidence.IdempotencyKey || receipt.ValidatedAt.Before(evidence.PreparedAt) ||
		receipt.ValidatedAt.After(evidence.ExpiresAt) || receipt.ValidatedAt.After(now.UTC().Truncate(time.Second)) {
		return ErrContract
	}
	return nil
}

func digestWire(wire wireRequest) string {
	wire.Digest = ""
	document, err := json.Marshal(wire)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && strings.ToLower(value) == value
}
