// Package secretdryrunrequest seals one exact, short-lived Kubernetes Secret
// server-side-dry-run request and its secret-free acceptance receipt. It is a
// pure versioned handoff: it owns no credential, filesystem, network,
// Kubernetes client, process, retry, or live mutation capability.
package secretdryrunrequest

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

const (
	APIVersion = "backup-materializer-secret-dry-run-request.fugue.dev/v1"
	Kind       = "BackupObserverSecretDryRunRequest"
	Policy     = "single-cell-one-shot-create-or-resource-version-cas-request-v1"

	FieldManager          = "fugue-backup-materializer"
	DryRunRawQuery        = "dryRun=All&fieldManager=fugue-backup-materializer&fieldValidation=Strict"
	MaximumDecisionAge    = 5 * time.Second
	MaximumRequestBytes   = int64(128 << 10)
	SecretCollectionPath  = "/api/v1/namespaces/" + materialization.SecretNamespace + "/secrets"
	CreateMethod          = "POST"
	ReplaceMethod         = "PUT"
	CreateExpectedStatus  = 201
	ReplaceExpectedStatus = 200
)

var ErrRequest = errors.New("invalid backup materializer Secret dry-run request")

// Evidence is the public, secret-free projection of one sealed request. Its
// request digest binds the private document without making that document
// serializable through Request's default JSON or formatting surfaces.
type Evidence struct {
	APIVersion                string           `json:"apiVersion"`
	Kind                      string           `json:"kind"`
	Policy                    string           `json:"policy"`
	Namespace                 string           `json:"namespace"`
	SecretName                string           `json:"secretName"`
	CellKey                   string           `json:"cellKey"`
	CellID                    string           `json:"cellId"`
	RunID                     string           `json:"runId"`
	Action                    reconcile.Action `json:"action"`
	PlanDigest                string           `json:"planDigest"`
	ManifestDigest            string           `json:"manifestDigest"`
	DecisionDigest            string           `json:"decisionDigest"`
	Method                    string           `json:"method"`
	Path                      string           `json:"path"`
	RawQuery                  string           `json:"rawQuery"`
	ExpectedStatus            int              `json:"expectedStatus"`
	ExpectedUID               string           `json:"expectedUid,omitempty"`
	ExpectedResourceVersion   string           `json:"expectedResourceVersion,omitempty"`
	RequestDigest             string           `json:"requestDigest"`
	IdempotencyKey            string           `json:"idempotencyKey"`
	DecidedAt                 time.Time        `json:"decidedAt"`
	PreparedAt                time.Time        `json:"preparedAt"`
	ExpiresAt                 time.Time        `json:"expiresAt"`
	RequireAbsent             bool             `json:"requireAbsent"`
	RequireUIDMatch           bool             `json:"requireUidMatch"`
	RequireResourceVersionCAS bool             `json:"requireResourceVersionCas"`
	RetainExisting            bool             `json:"retainExisting"`
	OneShot                   bool             `json:"oneShot"`
	RetriesAllowed            bool             `json:"retriesAllowed"`
	ServerSideDryRun          bool             `json:"serverSideDryRun"`
	Persisted                 bool             `json:"persisted"`
	DeleteAllowed             bool             `json:"deleteAllowed"`
	ExecutionAllowed          bool             `json:"executionAllowed"`
	ProductionMutationAllowed bool             `json:"productionMutationAllowed"`
	Digest                    string           `json:"digest"`
}

// Request is immutable outside this package. Evidence and the private Secret
// document are copied at every ingress and egress boundary.
type Request struct {
	evidence Evidence
	document []byte
}

// Prepare validates one current plan/decision pair and seals its exact
// Kubernetes request. Its expiry remains anchored to the original decision;
// preparing or forwarding a request can never extend that authorization age.
func Prepare(expectedCellKey string, plan materialization.Plan, decision reconcile.Decision, now time.Time) (Request, error) {
	now = canonicalNow(now)
	identity, err := materialization.SecretIdentityForCell(expectedCellKey)
	if err != nil || !canonicalTime(now) || reconcile.ValidateDecision(decision) != nil ||
		materialization.Validate(plan, now) != nil || plan.CellKey != identity.CellKey ||
		plan.CellID != identity.CellID || plan.Namespace != identity.Namespace || plan.SecretName != identity.SecretName ||
		decision.Namespace != plan.Namespace || decision.SecretName != plan.SecretName ||
		decision.CellKey != plan.CellKey || decision.CellID != plan.CellID ||
		decision.DesiredPlanDigest != plan.Digest || !decision.MutationCandidate || decision.Blocked ||
		decision.ExecutionAllowed || decision.ProductionMutationAllowed || decision.DeleteAllowed ||
		decision.DecidedAt.After(now) || now.Sub(decision.DecidedAt) > MaximumDecisionAge ||
		(decision.Action != reconcile.ActionCreateIfAbsent && decision.Action != reconcile.ActionReplaceResourceVersionCAS) {
		return Request{}, ErrRequest
	}
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		return Request{}, ErrRequest
	}
	data, err := plan.Data(now)
	if err != nil {
		return Request{}, ErrRequest
	}
	immutable := false
	document := secretDocument{
		APIVersion: "v1",
		Kind:       "Secret",
		Metadata: secretMetadata{
			Name:        manifest.SecretName,
			Namespace:   manifest.Namespace,
			Labels:      cloneStringMap(manifest.Labels),
			Annotations: cloneStringMap(manifest.Annotations),
		},
		Immutable: &immutable,
		Type:      manifest.SecretType,
		Data: map[string]string{
			data.SpecKey:  base64.StdEncoding.EncodeToString(data.SpecDocument),
			data.TokenKey: base64.StdEncoding.EncodeToString(data.ObserverToken),
		},
	}
	evidence := Evidence{
		APIVersion: APIVersion, Kind: Kind, Policy: Policy,
		Namespace: identity.Namespace, SecretName: identity.SecretName,
		CellKey: identity.CellKey, CellID: identity.CellID, RunID: plan.RunID,
		Action: decision.Action, PlanDigest: plan.Digest, ManifestDigest: manifest.Digest,
		DecisionDigest: decision.Digest, RawQuery: DryRunRawQuery,
		DecidedAt: decision.DecidedAt, PreparedAt: now,
		ExpiresAt:     decision.DecidedAt.Add(MaximumDecisionAge),
		RequireAbsent: decision.RequireAbsent, RequireUIDMatch: decision.RequireUIDMatch,
		RequireResourceVersionCAS: decision.RequireResourceVersionCAS,
		RetainExisting:            decision.RetainExisting, OneShot: true, RetriesAllowed: false,
		ServerSideDryRun: true, Persisted: false, DeleteAllowed: false,
		ExecutionAllowed: false, ProductionMutationAllowed: false,
	}
	switch decision.Action {
	case reconcile.ActionCreateIfAbsent:
		evidence.Method = CreateMethod
		evidence.Path = SecretCollectionPath
		evidence.ExpectedStatus = CreateExpectedStatus
	case reconcile.ActionReplaceResourceVersionCAS:
		document.Metadata.UID = decision.ExpectedUID
		document.Metadata.ResourceVersion = decision.ExpectedResourceVersion
		evidence.Method = ReplaceMethod
		evidence.Path = SecretCollectionPath + "/" + identity.SecretName
		evidence.ExpectedStatus = ReplaceExpectedStatus
		evidence.ExpectedUID = decision.ExpectedUID
		evidence.ExpectedResourceVersion = decision.ExpectedResourceVersion
	}
	encoded, err := json.Marshal(document)
	if err != nil || len(encoded) == 0 || int64(len(encoded)) > MaximumRequestBytes {
		return Request{}, ErrRequest
	}
	evidence.RequestDigest = digestBytes(encoded)
	evidence.IdempotencyKey = IdempotencyKey(identity.CellID, decision.Digest)
	evidence.Digest = DigestEvidence(evidence)
	return Restore(evidence, encoded, expectedCellKey, now)
}

// Restore validates a public evidence/private-document handoff without
// granting transport capability. It is the sole future gateway wire ingress.
func Restore(evidence Evidence, document []byte, expectedCellKey string, now time.Time) (Request, error) {
	now = canonicalNow(now)
	if validateEvidence(evidence, expectedCellKey, now) != nil {
		return Request{}, ErrRequest
	}
	parsed, manifestDigest, err := validateTransportRequest(
		evidence.Method, evidence.Path, evidence.RawQuery, expectedCellKey, document,
	)
	if err != nil || evidence.RequestDigest != digestBytes(document) || evidence.ManifestDigest != manifestDigest ||
		parsed.Metadata.Annotations[reconcile.AnnotationPlanDigest] != evidence.PlanDigest ||
		parsed.Metadata.Annotations[reconcile.AnnotationCellKey] != evidence.CellKey ||
		parsed.Metadata.Annotations[reconcile.AnnotationRunID] != evidence.RunID ||
		parsed.Metadata.UID != evidence.ExpectedUID ||
		parsed.Metadata.ResourceVersion != evidence.ExpectedResourceVersion {
		return Request{}, ErrRequest
	}
	return Request{
		evidence: evidence,
		document: append([]byte(nil), document...),
	}, nil
}

// Validate rechecks the complete sealed request and current replay window.
func Validate(request Request, expectedCellKey string, now time.Time) error {
	_, err := Restore(request.evidence, request.document, expectedCellKey, now)
	return err
}

// Evidence returns a secret-free copy. It never opens the private document.
func (request Request) Evidence() Evidence { return request.evidence }

// Open explicitly returns the validated evidence and a fresh copy of the
// private document. Callers must cross their own separately reviewed network
// or credential boundary after this operation.
func (request Request) Open(expectedCellKey string, now time.Time) (Evidence, []byte, error) {
	if Validate(request, expectedCellKey, now) != nil {
		return Evidence{}, nil, ErrRequest
	}
	return request.evidence, append([]byte(nil), request.document...), nil
}

// ValidateTransportRequest proves the exact fixed-purpose Kubernetes request
// shape independently of a prepared evidence envelope.
func ValidateTransportRequest(method, path, rawQuery, expectedCellKey string, document []byte) error {
	_, _, err := validateTransportRequest(method, path, rawQuery, expectedCellKey, document)
	return err
}

func DigestEvidence(evidence Evidence) string {
	evidence.Digest = ""
	document, err := json.Marshal(evidence)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

func (request Request) MarshalJSON() ([]byte, error) { return json.Marshal(request.evidence) }

// UnmarshalJSON fails closed because public JSON contains no private document.
// Restore is the only supported ingress for a complete wire handoff.
func (*Request) UnmarshalJSON([]byte) error { return ErrRequest }

func (request Request) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretDryRunRequest{cell=%q action=%q request=%q expiresAt=%q document=[REDACTED] persisted=false executionAllowed=false digest=%q}",
		request.evidence.CellKey,
		request.evidence.Action,
		request.evidence.RequestDigest,
		request.evidence.ExpiresAt.Format(time.RFC3339),
		request.evidence.Digest,
	)
}

func (request Request) GoString() string { return request.String() }

type secretDocument struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   secretMetadata    `json:"metadata"`
	Immutable  *bool             `json:"immutable"`
	Type       string            `json:"type"`
	Data       map[string]string `json:"data"`
	StringData map[string]string `json:"stringData,omitempty"`
}

type secretMetadata struct {
	Name                       string            `json:"name"`
	GenerateName               string            `json:"generateName,omitempty"`
	Namespace                  string            `json:"namespace"`
	SelfLink                   string            `json:"selfLink,omitempty"`
	UID                        string            `json:"uid,omitempty"`
	ResourceVersion            string            `json:"resourceVersion,omitempty"`
	Generation                 int64             `json:"generation,omitempty"`
	CreationTimestamp          json.RawMessage   `json:"creationTimestamp,omitempty"`
	DeletionTimestamp          json.RawMessage   `json:"deletionTimestamp,omitempty"`
	DeletionGracePeriodSeconds *int64            `json:"deletionGracePeriodSeconds,omitempty"`
	Labels                     map[string]string `json:"labels"`
	Annotations                map[string]string `json:"annotations"`
	OwnerReferences            []json.RawMessage `json:"ownerReferences,omitempty"`
	Finalizers                 []string          `json:"finalizers,omitempty"`
	ManagedFields              []json.RawMessage `json:"managedFields,omitempty"`
}

func validateEvidence(evidence Evidence, expectedCellKey string, now time.Time) error {
	identity, err := materialization.SecretIdentityForCell(expectedCellKey)
	if err != nil || !canonicalTime(now) || evidence.APIVersion != APIVersion || evidence.Kind != Kind ||
		evidence.Policy != Policy || evidence.Namespace != identity.Namespace || evidence.SecretName != identity.SecretName ||
		evidence.CellKey != identity.CellKey || evidence.CellID != identity.CellID || evidence.RunID == "" ||
		!validDigest(evidence.PlanDigest) || !validDigest(evidence.ManifestDigest) || !validDigest(evidence.DecisionDigest) ||
		!validDigest(evidence.RequestDigest) || evidence.RawQuery != DryRunRawQuery ||
		evidence.IdempotencyKey != IdempotencyKey(evidence.CellID, evidence.DecisionDigest) ||
		!canonicalTime(evidence.DecidedAt) || !canonicalTime(evidence.PreparedAt) || !canonicalTime(evidence.ExpiresAt) ||
		evidence.DecidedAt.After(evidence.PreparedAt) || evidence.PreparedAt.Sub(evidence.DecidedAt) > MaximumDecisionAge ||
		evidence.ExpiresAt != evidence.DecidedAt.Add(MaximumDecisionAge) || evidence.ExpiresAt.Before(evidence.PreparedAt) ||
		now.Before(evidence.PreparedAt) || now.After(evidence.ExpiresAt) || !evidence.OneShot || evidence.RetriesAllowed ||
		!evidence.ServerSideDryRun || evidence.Persisted || evidence.DeleteAllowed || evidence.ExecutionAllowed ||
		evidence.ProductionMutationAllowed || evidence.Digest != DigestEvidence(evidence) {
		return ErrRequest
	}
	switch evidence.Action {
	case reconcile.ActionCreateIfAbsent:
		if evidence.Method != CreateMethod || evidence.Path != SecretCollectionPath ||
			evidence.ExpectedStatus != CreateExpectedStatus || evidence.ExpectedUID != "" ||
			evidence.ExpectedResourceVersion != "" || !evidence.RequireAbsent || evidence.RequireUIDMatch ||
			evidence.RequireResourceVersionCAS || evidence.RetainExisting {
			return ErrRequest
		}
	case reconcile.ActionReplaceResourceVersionCAS:
		if evidence.Method != ReplaceMethod || evidence.Path != SecretCollectionPath+"/"+identity.SecretName ||
			evidence.ExpectedStatus != ReplaceExpectedStatus || !validOptionalOpaque(evidence.ExpectedUID) || evidence.ExpectedUID == "" ||
			!validOptionalOpaque(evidence.ExpectedResourceVersion) || evidence.ExpectedResourceVersion == "" ||
			evidence.RequireAbsent || !evidence.RequireUIDMatch || !evidence.RequireResourceVersionCAS || !evidence.RetainExisting {
			return ErrRequest
		}
	default:
		return ErrRequest
	}
	return nil
}

func validateTransportRequest(method, path, rawQuery, expectedCellKey string, document []byte) (secretDocument, string, error) {
	identity, err := materialization.SecretIdentityForCell(expectedCellKey)
	if err != nil || rawQuery != DryRunRawQuery || len(document) == 0 || int64(len(document)) > MaximumRequestBytes {
		return secretDocument{}, "", ErrRequest
	}
	var request secretDocument
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return secretDocument{}, "", ErrRequest
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return secretDocument{}, "", ErrRequest
	}
	canonicalDocument, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonicalDocument, document) {
		return secretDocument{}, "", ErrRequest
	}
	metadata := request.Metadata
	if request.APIVersion != "v1" || request.Kind != "Secret" || request.Type != reconcile.SecretTypeOpaque ||
		request.Immutable == nil || *request.Immutable || len(request.StringData) != 0 ||
		metadata.Name != identity.SecretName || metadata.Namespace != identity.Namespace || metadata.GenerateName != "" ||
		metadata.SelfLink != "" || metadata.Generation != 0 || len(metadata.CreationTimestamp) != 0 ||
		len(metadata.DeletionTimestamp) != 0 || metadata.DeletionGracePeriodSeconds != nil ||
		len(metadata.OwnerReferences) != 0 || len(metadata.Finalizers) != 0 || len(metadata.ManagedFields) != 0 ||
		!exactOwnedLabels(metadata.Labels) || !exactOwnedAnnotations(metadata.Annotations) || len(request.Data) != 2 {
		return secretDocument{}, "", ErrRequest
	}
	uid := metadata.UID
	resourceVersion := metadata.ResourceVersion
	switch method {
	case CreateMethod:
		if path != SecretCollectionPath || uid != "" || resourceVersion != "" {
			return secretDocument{}, "", ErrRequest
		}
		uid = "transport-validation"
		resourceVersion = "1"
	case ReplaceMethod:
		if path != SecretCollectionPath+"/"+identity.SecretName || uid == "" || resourceVersion == "" {
			return secretDocument{}, "", ErrRequest
		}
	default:
		return secretDocument{}, "", ErrRequest
	}
	data := make(map[string][]byte, len(request.Data))
	for key, encoded := range request.Data {
		decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
			return secretDocument{}, "", ErrRequest
		}
		data[key] = decoded
	}
	observation, err := reconcile.ObserveExisting(expectedCellKey, reconcile.SecretEvidence{
		Namespace: identity.Namespace, SecretName: identity.SecretName, UID: uid, ResourceVersion: resourceVersion,
		SecretType: request.Type, Labels: request.Metadata.Labels, Annotations: request.Metadata.Annotations,
		Data: data, Immutable: false, DeletionPending: false, OwnerReferenceCount: 0,
	})
	if err != nil || observation.State != reconcile.StateManaged {
		return secretDocument{}, "", ErrRequest
	}
	manifest := reconcile.Manifest{
		APIVersion: reconcile.ContractAPIVersion, Kind: reconcile.ManifestKind, Policy: reconcile.ContractPolicy,
		Namespace: identity.Namespace, SecretName: identity.SecretName, CellKey: identity.CellKey, CellID: identity.CellID,
		SecretType: request.Type, Labels: cloneStringMap(request.Metadata.Labels),
		Annotations: cloneStringMap(request.Metadata.Annotations),
		DataDigests: map[string]string{
			materialization.SpecDataKey:  digestBytes(data[materialization.SpecDataKey]),
			materialization.TokenDataKey: digestBytes(data[materialization.TokenDataKey]),
		},
		PlanDigest: request.Metadata.Annotations[reconcile.AnnotationPlanDigest],
		Immutable:  false, OwnerReferencesAllowed: false,
	}
	manifest.Digest = reconcile.DigestManifest(manifest)
	return request, manifest.Digest, nil
}

func exactOwnedLabels(labels map[string]string) bool {
	if len(labels) != 4 {
		return false
	}
	for _, key := range []string{
		reconcile.LabelName, reconcile.LabelComponent, reconcile.LabelManagedBy, reconcile.LabelCellID,
	} {
		if labels[key] == "" {
			return false
		}
	}
	return true
}

func exactOwnedAnnotations(annotations map[string]string) bool {
	if len(annotations) != 14 {
		return false
	}
	for _, key := range []string{
		reconcile.AnnotationPlanAPIVersion, reconcile.AnnotationPlanPolicy, reconcile.AnnotationPlanDigest,
		reconcile.AnnotationCellKey, reconcile.AnnotationRunID, reconcile.AnnotationSpecDigest,
		reconcile.AnnotationBundleDigest, reconcile.AnnotationCredentialID, reconcile.AnnotationTokenID,
		reconcile.AnnotationSpecDocumentDigest, reconcile.AnnotationObserverTokenDigest,
		reconcile.AnnotationIssuedAt, reconcile.AnnotationRenewAfter, reconcile.AnnotationExpiresAt,
	} {
		if annotations[key] == "" {
			return false
		}
	}
	return true
}

// IdempotencyKey returns the exact cross-process key for one cell/decision.
// It contains no Secret value and grants no replay or execution capability.
func IdempotencyKey(cellID, decisionDigest string) string {
	if cellID == "" || !validDigest(decisionDigest) {
		return ""
	}
	return "backup-materializer-secret-dry-run/" + cellID + "/" + strings.TrimPrefix(decisionDigest, "sha256:")
}

func validOptionalOpaque(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e || character == '\\' || character == '"' {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && strings.ToLower(value) == value
}

func cloneStringMap(value map[string]string) map[string]string {
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func canonicalNow(value time.Time) time.Time { return value.UTC().Truncate(time.Second) }

func canonicalTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}
