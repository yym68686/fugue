// Package dryrunguard defines the fail-closed admission and authorization
// contract for a future, cell-local Kubernetes Secret dry-run gateway. It is
// pure policy: constructing a Guard creates no ServiceAccount, RBAC object,
// admission resource, credential, network client, or workload.
package dryrunguard

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"fugue/internal/backupmaterializer/materialization"
)

const (
	APIVersion = "backup-materializer-dry-run-guard.fugue.dev/v1"
	Kind       = "BackupObserverSecretDryRunGuard"
	Policy     = "cell-local-gateway-single-secret-dry-run-only-fail-closed-v1"

	MinimumKubernetesVersion = "1.30.0"
	AdmissionAPIVersion      = "admissionregistration.k8s.io/v1"
	AdmissionPolicyKind      = "ValidatingAdmissionPolicy"
	AdmissionBindingKind     = "ValidatingAdmissionPolicyBinding"
	MatchPolicy              = "Equivalent"
	FailurePolicy            = "Fail"
	ValidationAction         = "Deny"
	NamespacedScope          = "Namespaced"
	NamespaceSelectorKey     = "kubernetes.io/metadata.name"

	serviceAccountPrefix = "fugue-backup-dryrun-"
)

var (
	ErrGuard = errors.New("invalid backup materializer dry-run guard")

	canonicalName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// ResourceRule is the exact admission request surface protected by the guard.
// MatchPolicy=Equivalent means the API server first converts an equivalent
// request to this registered core/v1 Secret shape before evaluating CEL.
type ResourceRule struct {
	APIGroups   []string `json:"apiGroups"`
	APIVersions []string `json:"apiVersions"`
	Operations  []string `json:"operations"`
	Resources   []string `json:"resources"`
	Scope       string   `json:"scope"`
}

// AuthorizationRule is the maximum Role rule allowed for the future gateway.
// Kubernetes cannot constrain CREATE by resourceName, so admission must be
// ready before this authorization is installed. UPDATE remains name-scoped.
type AuthorizationRule struct {
	APIGroups     []string `json:"apiGroups"`
	Resources     []string `json:"resources"`
	ResourceNames []string `json:"resourceNames,omitempty"`
	Verbs         []string `json:"verbs"`
}

// CELRule is one immutable match condition or validation expected in the
// future ValidatingAdmissionPolicy. Name is local contract metadata, not a
// Kubernetes Validation field.
type CELRule struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
	Message    string `json:"message,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// Guard seals one dedicated gateway identity to one cell-owned Secret. The
// two false fields are deliberate negative capabilities and participate in
// the digest, so a consumer cannot silently turn validation into mutation.
type Guard struct {
	APIVersion                   string              `json:"apiVersion"`
	Kind                         string              `json:"kind"`
	Policy                       string              `json:"policy"`
	MinimumKubernetesVersion     string              `json:"minimumKubernetesVersion"`
	AdmissionAPIVersion          string              `json:"admissionApiVersion"`
	AdmissionPolicyKind          string              `json:"admissionPolicyKind"`
	AdmissionBindingKind         string              `json:"admissionBindingKind"`
	CellKey                      string              `json:"cellKey"`
	CellID                       string              `json:"cellId"`
	Namespace                    string              `json:"namespace"`
	SecretName                   string              `json:"secretName"`
	ServiceAccountName           string              `json:"serviceAccountName"`
	ServiceAccountUsername       string              `json:"serviceAccountUsername"`
	AdmissionPolicyName          string              `json:"admissionPolicyName"`
	AdmissionBindingName         string              `json:"admissionBindingName"`
	NamespaceSelectorKey         string              `json:"namespaceSelectorKey"`
	NamespaceSelectorValue       string              `json:"namespaceSelectorValue"`
	MatchPolicy                  string              `json:"matchPolicy"`
	FailurePolicy                string              `json:"failurePolicy"`
	ResourceRule                 ResourceRule        `json:"resourceRule"`
	AuthorizationRules           []AuthorizationRule `json:"authorizationRules"`
	MatchConditions              []CELRule           `json:"matchConditions"`
	Validations                  []CELRule           `json:"validations"`
	ValidationActions            []string            `json:"validationActions"`
	DedicatedServiceAccount      bool                `json:"dedicatedServiceAccount"`
	BoundProjectedTokenRequired  bool                `json:"boundProjectedTokenRequired"`
	AdmissionReadyBeforeRBAC     bool                `json:"admissionReadyBeforeRbac"`
	AutomountServiceAccountToken bool                `json:"automountServiceAccountToken"`
	ProductionMutationAllowed    bool                `json:"productionMutationAllowed"`
	Digest                       string              `json:"digest"`
}

// Request is the converted admission evidence used by Evaluate. Evaluate is
// a local policy oracle for tests and preflight; it does not replace API-server
// authorization or admission evaluation.
type Request struct {
	Username           string
	APIGroup           string
	APIVersion         string
	Resource           string
	Operation          string
	Namespace          string
	Name               string
	Subresource        string
	DryRun             bool
	ObjectName         string
	ObjectGenerateName string
	OldObjectPresent   bool
	OldObjectName      string
}

// Decision distinguishes an admission denial from a request outside this
// policy's scope. Outside-scope requests still require independent RBAC.
type Decision struct {
	Applies bool
	Allowed bool
	Reason  string
}

// Build returns the complete, deterministic guard for one canonical backup
// cell. It grants no capability and never renders or installs Kubernetes API
// objects.
func Build(cellKey string) (Guard, error) {
	return buildExpected(cellKey)
}

// Validate rebuilds every field from CellKey. Recomputing Digest after a
// mutation therefore cannot bless a widened identity, rule, or capability.
func Validate(guard Guard) error {
	expected, err := buildExpected(guard.CellKey)
	if err != nil || !reflect.DeepEqual(guard, expected) {
		return ErrGuard
	}
	return nil
}

func buildExpected(cellKey string) (Guard, error) {
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil {
		return Guard{}, ErrGuard
	}
	name := serviceAccountPrefix + identity.CellID
	if !canonicalName.MatchString(name) {
		return Guard{}, ErrGuard
	}
	username := "system:serviceaccount:" + identity.Namespace + ":" + name
	quotedUsername := strconv.Quote(username)
	quotedNamespace := strconv.Quote(identity.Namespace)
	quotedSecretName := strconv.Quote(identity.SecretName)
	guard := Guard{
		APIVersion: APIVersion, Kind: Kind, Policy: Policy,
		MinimumKubernetesVersion: MinimumKubernetesVersion,
		AdmissionAPIVersion:      AdmissionAPIVersion, AdmissionPolicyKind: AdmissionPolicyKind,
		AdmissionBindingKind: AdmissionBindingKind,
		CellKey:              identity.CellKey, CellID: identity.CellID, Namespace: identity.Namespace, SecretName: identity.SecretName,
		ServiceAccountName: name, ServiceAccountUsername: username,
		AdmissionPolicyName: name, AdmissionBindingName: name,
		NamespaceSelectorKey: NamespaceSelectorKey, NamespaceSelectorValue: identity.Namespace,
		MatchPolicy: MatchPolicy, FailurePolicy: FailurePolicy,
		ResourceRule: ResourceRule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Operations: []string{"CREATE", "UPDATE"}, Resources: []string{"secrets"}, Scope: NamespacedScope},
		AuthorizationRules: []AuthorizationRule{
			{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create"}},
			{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{identity.SecretName}, Verbs: []string{"update"}},
		},
		MatchConditions: []CELRule{{Name: "dedicated-gateway-identity", Expression: "request.userInfo.username == " + quotedUsername}},
		Validations: []CELRule{
			{Name: "server-side-dry-run-only", Expression: "request.dryRun == true", Message: "backup dry-run gateway requests must be server-side dry-run", Reason: "Forbidden"},
			{Name: "exact-cell-secret-only", Expression: "request.namespace == " + quotedNamespace + " && request.name == " + quotedSecretName + " && request.subResource == \"\" && object.metadata.name == " + quotedSecretName + " && object.metadata.generateName == \"\"", Message: "backup dry-run gateway requests must target the exact cell Secret", Reason: "Forbidden"},
			{Name: "create-update-evidence-only", Expression: "(request.operation == \"CREATE\" && oldObject == null) || (request.operation == \"UPDATE\" && oldObject != null && oldObject.metadata.name == " + quotedSecretName + ")", Message: "backup dry-run gateway requests must carry canonical create or update evidence", Reason: "Forbidden"},
		},
		ValidationActions: []string{ValidationAction}, DedicatedServiceAccount: true,
		BoundProjectedTokenRequired: true, AdmissionReadyBeforeRBAC: true,
		AutomountServiceAccountToken: false, ProductionMutationAllowed: false,
	}
	guard.Digest = DigestGuard(guard)
	return guard, nil
}

// DigestGuard returns the deterministic public contract digest. Guard contains
// only identities and policy; no Secret data or bearer credential is accepted.
func DigestGuard(guard Guard) string {
	guard.Digest = ""
	document, err := json.Marshal(guard)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// Evaluate applies the same fixed identity, target, dry-run, and lifecycle
// predicates represented by the CEL contract to converted admission evidence.
func Evaluate(guard Guard, request Request) (Decision, error) {
	if err := Validate(guard); err != nil {
		return Decision{}, err
	}
	if request.APIGroup != "" || request.APIVersion != "v1" || request.Resource != "secrets" ||
		(request.Operation != "CREATE" && request.Operation != "UPDATE") {
		return Decision{Allowed: true, Reason: "outside-resource-rule"}, nil
	}
	if request.Username != guard.ServiceAccountUsername {
		return Decision{Allowed: true, Reason: "outside-gateway-identity"}, nil
	}
	if !request.DryRun {
		return Decision{Applies: true, Reason: "server-side-dry-run-required"}, nil
	}
	if request.Namespace != guard.Namespace || request.Name != guard.SecretName || request.Subresource != "" ||
		request.ObjectName != guard.SecretName || request.ObjectGenerateName != "" {
		return Decision{Applies: true, Reason: "exact-cell-secret-required"}, nil
	}
	if request.Operation == "CREATE" && request.OldObjectPresent {
		return Decision{Applies: true, Reason: "canonical-create-evidence-required"}, nil
	}
	if request.Operation == "UPDATE" && (!request.OldObjectPresent || request.OldObjectName != guard.SecretName) {
		return Decision{Applies: true, Reason: "canonical-update-evidence-required"}, nil
	}
	return Decision{Applies: true, Allowed: true, Reason: "dry-run-accepted"}, nil
}

func (guard Guard) String() string {
	return fmt.Sprintf("BackupObserverSecretDryRunGuard{cell=%q namespace=%q secret=%q serviceAccount=%q productionMutationAllowed=false digest=%q}", guard.CellKey, guard.Namespace, guard.SecretName, guard.ServiceAccountName, guard.Digest)
}

func (guard Guard) GoString() string { return guard.String() }
