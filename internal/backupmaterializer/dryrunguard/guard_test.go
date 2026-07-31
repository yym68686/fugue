package dryrunguard

import (
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"

	"fugue/internal/backupmaterializer/materialization"
)

const testCellKey = "backup/app-database/0123456789abcdef"

func TestGuardSealsDedicatedIdentityExactSecretAndFailClosedPolicy(t *testing.T) {
	t.Parallel()
	guard, err := Build(testCellKey)
	if err != nil {
		t.Fatalf("build dry-run guard: %v", err)
	}
	identity, err := materialization.SecretIdentityForCell(testCellKey)
	if err != nil {
		t.Fatalf("derive materialization identity: %v", err)
	}
	wantName := "fugue-backup-dryrun-app-database-0123456789abcdef"
	if guard.APIVersion != APIVersion || guard.Kind != Kind || guard.Policy != Policy ||
		guard.MinimumKubernetesVersion != "1.30.0" || guard.AdmissionAPIVersion != "admissionregistration.k8s.io/v1" ||
		guard.AdmissionPolicyKind != "ValidatingAdmissionPolicy" || guard.AdmissionBindingKind != "ValidatingAdmissionPolicyBinding" ||
		guard.CellKey != testCellKey || guard.CellID != identity.CellID || guard.Namespace != identity.Namespace ||
		guard.SecretName != identity.SecretName || guard.ServiceAccountName != wantName ||
		guard.ServiceAccountUsername != "system:serviceaccount:fugue-system:"+wantName ||
		guard.AdmissionPolicyName != wantName || guard.AdmissionBindingName != wantName ||
		guard.NamespaceSelectorKey != "kubernetes.io/metadata.name" || guard.NamespaceSelectorValue != "fugue-system" ||
		guard.MatchPolicy != "Equivalent" || guard.FailurePolicy != "Fail" ||
		!guard.DedicatedServiceAccount || !guard.BoundProjectedTokenRequired || !guard.AdmissionReadyBeforeRBAC ||
		guard.AutomountServiceAccountToken || guard.ProductionMutationAllowed || guard.Digest == "" ||
		guard.Digest != DigestGuard(guard) {
		t.Fatalf("dry-run guard boundary drifted: %+v", guard)
	}
	wantResourceRule := ResourceRule{
		APIGroups: []string{""}, APIVersions: []string{"v1"}, Operations: []string{"CREATE", "UPDATE"},
		Resources: []string{"secrets"}, Scope: "Namespaced",
	}
	wantAuthorization := []AuthorizationRule{
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"create"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{identity.SecretName}, Verbs: []string{"update"}},
	}
	if !reflect.DeepEqual(guard.ResourceRule, wantResourceRule) || !reflect.DeepEqual(guard.AuthorizationRules, wantAuthorization) ||
		!reflect.DeepEqual(guard.ValidationActions, []string{"Deny"}) || len(guard.MatchConditions) != 1 || len(guard.Validations) != 3 {
		t.Fatalf("admission/RBAC surface widened: rule=%+v auth=%+v conditions=%+v validations=%+v actions=%v", guard.ResourceRule, guard.AuthorizationRules, guard.MatchConditions, guard.Validations, guard.ValidationActions)
	}
	for _, forbiddenVerb := range []string{"get", "list", "watch", "patch", "delete", "deletecollection", "impersonate", "bind", "escalate"} {
		if strings.Contains(strings.Join(flattenAuthorizationVerbs(guard.AuthorizationRules), ","), forbiddenVerb) {
			t.Fatalf("authorization unexpectedly contains %q: %+v", forbiddenVerb, guard.AuthorizationRules)
		}
	}
	if !strings.Contains(guard.MatchConditions[0].Expression, guard.ServiceAccountUsername) ||
		guard.Validations[0].Expression != "has(request.dryRun) && request.dryRun == true" ||
		!strings.Contains(guard.Validations[1].Expression, "!has(request.subResource)") ||
		!strings.Contains(guard.Validations[1].Expression, "!has(object.metadata.generateName)") ||
		!strings.Contains(guard.Validations[1].Expression, guard.SecretName) ||
		!strings.Contains(guard.Validations[2].Expression, "oldObject == null") ||
		!strings.Contains(guard.Validations[2].Expression, "oldObject != null") {
		t.Fatalf("CEL policy lost a required predicate: conditions=%+v validations=%+v", guard.MatchConditions, guard.Validations)
	}
	if err := Validate(guard); err != nil {
		t.Fatalf("validate guard: %v", err)
	}
}

func TestGuardPolicyOracleDeniesLiveCrossCellAndMalformedGatewayRequests(t *testing.T) {
	t.Parallel()
	guard := mustGuard(t)
	create := canonicalRequest(guard, "CREATE")
	decision, err := Evaluate(guard, create)
	if err != nil || !decision.Applies || !decision.Allowed || decision.Reason != "dry-run-accepted" {
		t.Fatalf("canonical CREATE decision=%+v err=%v", decision, err)
	}
	update := canonicalRequest(guard, "UPDATE")
	update.OldObjectPresent = true
	update.OldObjectName = guard.SecretName
	decision, err = Evaluate(guard, update)
	if err != nil || !decision.Applies || !decision.Allowed || decision.Reason != "dry-run-accepted" {
		t.Fatalf("canonical UPDATE decision=%+v err=%v", decision, err)
	}

	denied := map[string]func(*Request){
		"live request":            func(value *Request) { value.DryRun = false },
		"other request namespace": func(value *Request) { value.Namespace = "default" },
		"other request name":      func(value *Request) { value.Name = "other" },
		"subresource":             func(value *Request) { value.Subresource = "status" },
		"other object name":       func(value *Request) { value.ObjectName = "other" },
		"generated name":          func(value *Request) { value.ObjectGenerateName = "fugue-backup-" },
		"create with old object":  func(value *Request) { value.OldObjectPresent = true },
	}
	for name, mutate := range denied {
		t.Run("create/"+name, func(t *testing.T) {
			request := canonicalRequest(guard, "CREATE")
			mutate(&request)
			decision, err := Evaluate(guard, request)
			if err != nil || !decision.Applies || decision.Allowed || decision.Reason == "" {
				t.Fatalf("unsafe request decision=%+v err=%v request=%+v", decision, err, request)
			}
		})
	}
	for name, mutate := range map[string]func(*Request){
		"missing old object": func(value *Request) { value.OldObjectPresent = false },
		"other old name":     func(value *Request) { value.OldObjectName = "other" },
		"live update":        func(value *Request) { value.DryRun = false },
	} {
		t.Run("update/"+name, func(t *testing.T) {
			request := canonicalRequest(guard, "UPDATE")
			request.OldObjectPresent = true
			request.OldObjectName = guard.SecretName
			mutate(&request)
			decision, err := Evaluate(guard, request)
			if err != nil || !decision.Applies || decision.Allowed || decision.Reason == "" {
				t.Fatalf("unsafe update decision=%+v err=%v request=%+v", decision, err, request)
			}
		})
	}
}

func TestGuardPolicyOracleDoesNotInterceptOtherPrincipalsOrResources(t *testing.T) {
	t.Parallel()
	guard := mustGuard(t)
	tests := map[string]func(*Request){
		"other principal": func(value *Request) { value.Username = "system:serviceaccount:fugue-system:other" },
		"other group":     func(value *Request) { value.APIGroup = "apps" },
		"other version":   func(value *Request) { value.APIVersion = "v2" },
		"other resource":  func(value *Request) { value.Resource = "configmaps" },
		"delete":          func(value *Request) { value.Operation = "DELETE" },
		"connect":         func(value *Request) { value.Operation = "CONNECT" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request := canonicalRequest(guard, "CREATE")
			request.DryRun = false
			mutate(&request)
			decision, err := Evaluate(guard, request)
			if err != nil || decision.Applies || !decision.Allowed || decision.Reason == "" {
				t.Fatalf("outside-scope decision=%+v err=%v request=%+v", decision, err, request)
			}
		})
	}
}

func TestGuardRejectsEveryFieldMutationEvenWithRecomputedDigest(t *testing.T) {
	t.Parallel()
	base := mustGuard(t)
	mutations := map[string]func(*Guard){
		"api version":               func(value *Guard) { value.APIVersion = "v2" },
		"kind":                      func(value *Guard) { value.Kind = "Other" },
		"policy":                    func(value *Guard) { value.Policy = "other" },
		"minimum version":           func(value *Guard) { value.MinimumKubernetesVersion = "1.29.0" },
		"admission api":             func(value *Guard) { value.AdmissionAPIVersion = "v1beta1" },
		"policy kind":               func(value *Guard) { value.AdmissionPolicyKind = "Other" },
		"binding kind":              func(value *Guard) { value.AdmissionBindingKind = "Other" },
		"cell":                      func(value *Guard) { value.CellKey = "backup/registry/0123456789abcdef" },
		"cell id":                   func(value *Guard) { value.CellID = "other" },
		"namespace":                 func(value *Guard) { value.Namespace = "default" },
		"secret":                    func(value *Guard) { value.SecretName = "other" },
		"service account":           func(value *Guard) { value.ServiceAccountName = "other" },
		"username":                  func(value *Guard) { value.ServiceAccountUsername = "other" },
		"policy name":               func(value *Guard) { value.AdmissionPolicyName = "other" },
		"binding name":              func(value *Guard) { value.AdmissionBindingName = "other" },
		"selector key":              func(value *Guard) { value.NamespaceSelectorKey = "other" },
		"selector value":            func(value *Guard) { value.NamespaceSelectorValue = "default" },
		"match policy":              func(value *Guard) { value.MatchPolicy = "Exact" },
		"failure policy":            func(value *Guard) { value.FailurePolicy = "Ignore" },
		"api groups":                func(value *Guard) { value.ResourceRule.APIGroups = []string{"*"} },
		"api versions":              func(value *Guard) { value.ResourceRule.APIVersions = []string{"*"} },
		"operations":                func(value *Guard) { value.ResourceRule.Operations = append(value.ResourceRule.Operations, "DELETE") },
		"resources":                 func(value *Guard) { value.ResourceRule.Resources = []string{"*"} },
		"scope":                     func(value *Guard) { value.ResourceRule.Scope = "*" },
		"authorization group":       func(value *Guard) { value.AuthorizationRules[0].APIGroups = []string{"*"} },
		"authorization resource":    func(value *Guard) { value.AuthorizationRules[0].Resources = []string{"*"} },
		"authorization create name": func(value *Guard) { value.AuthorizationRules[0].ResourceNames = []string{value.SecretName} },
		"authorization verb": func(value *Guard) {
			value.AuthorizationRules[0].Verbs = append(value.AuthorizationRules[0].Verbs, "patch")
		},
		"authorization update name": func(value *Guard) { value.AuthorizationRules[1].ResourceNames = []string{"other"} },
		"extra authorization rule": func(value *Guard) {
			value.AuthorizationRules = append(value.AuthorizationRules, AuthorizationRule{Verbs: []string{"get"}})
		},
		"match condition": func(value *Guard) { value.MatchConditions[0].Expression = "true" },
		"extra match condition": func(value *Guard) {
			value.MatchConditions = append(value.MatchConditions, CELRule{Name: "other", Expression: "true"})
		},
		"dry-run validation":    func(value *Guard) { value.Validations[0].Expression = "true" },
		"target validation":     func(value *Guard) { value.Validations[1].Expression = "true" },
		"lifecycle validation":  func(value *Guard) { value.Validations[2].Expression = "true" },
		"validation message":    func(value *Guard) { value.Validations[0].Message = "other" },
		"validation reason":     func(value *Guard) { value.Validations[0].Reason = "Invalid" },
		"validation action":     func(value *Guard) { value.ValidationActions = []string{"Audit"} },
		"not dedicated":         func(value *Guard) { value.DedicatedServiceAccount = false },
		"unbound token":         func(value *Guard) { value.BoundProjectedTokenRequired = false },
		"rbac before admission": func(value *Guard) { value.AdmissionReadyBeforeRBAC = false },
		"automount token":       func(value *Guard) { value.AutomountServiceAccountToken = true },
		"production mutation":   func(value *Guard) { value.ProductionMutationAllowed = true },
		"digest":                func(value *Guard) { value.Digest = "sha256:" + strings.Repeat("0", 64) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			value := cloneGuard(t, base)
			mutate(&value)
			if name != "digest" {
				value.Digest = DigestGuard(value)
			}
			if err := Validate(value); !errors.Is(err, ErrGuard) {
				t.Fatalf("mutated guard validation = %v, want ErrGuard: %+v", err, value)
			}
		})
	}
	for _, invalid := range []string{"", "backup/all/0123456789abcdef", "backup/app-database/0123456789ABCDEf", " backup/app-database/0123456789abcdef"} {
		if _, err := Build(invalid); !errors.Is(err, ErrGuard) {
			t.Fatalf("Build(%q) error=%v, want ErrGuard", invalid, err)
		}
	}
}

func TestGuardNamesAreCellDistinctAndWithinServiceAccountLimit(t *testing.T) {
	t.Parallel()
	names := make(map[string]bool)
	for _, kind := range []string{"control-plane-db", "app-database", "persistent-storage", "data-workspace", "registry", "platform-component"} {
		cellKey := "backup/" + kind + "/0123456789abcdef"
		guard, err := Build(cellKey)
		if err != nil {
			t.Fatalf("build %s guard: %v", kind, err)
		}
		if len(guard.ServiceAccountName) > 63 || names[guard.ServiceAccountName] ||
			guard.AdmissionPolicyName != guard.ServiceAccountName || guard.AdmissionBindingName != guard.ServiceAccountName {
			t.Fatalf("invalid or colliding %s guard names: %+v", kind, guard)
		}
		names[guard.ServiceAccountName] = true
	}
}

func TestGuardFormattingAndJSONContainPolicyOnly(t *testing.T) {
	t.Parallel()
	guard := mustGuard(t)
	document, err := json.Marshal(guard)
	if err != nil {
		t.Fatalf("marshal guard: %v", err)
	}
	for _, rendered := range []string{string(document), guard.String(), guard.GoString()} {
		for _, forbidden := range []string{"spec.json", "observerToken", "bearer", "eyJ", "privateKey", "password"} {
			if strings.Contains(rendered, forbidden) {
				t.Fatalf("policy rendering contains private-data marker %q: %s", forbidden, rendered)
			}
		}
	}
}

func TestGuardProductionDependencyBoundaryIsPure(t *testing.T) {
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list guard dependencies: %v output=%s", err, output)
	}
	dependencies := strings.Fields(string(output))
	for _, forbidden := range []string{"database/sql", "net/http", "os/exec", "k8s.io/", "github.com/aws", "github.com/jackc", "github.com/google/go-containerregistry"} {
		for _, dependency := range dependencies {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden) {
				t.Fatalf("pure guard dependency widened to %q", dependency)
			}
		}
	}
	var local []string
	for _, dependency := range dependencies {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/dryrunguard",
		"fugue/internal/backupmaterializer/materialization",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("guard local dependency closure=%v, want %v", local, want)
	}
}

func canonicalRequest(guard Guard, operation string) Request {
	return Request{
		Username: guard.ServiceAccountUsername, APIGroup: "", APIVersion: "v1", Resource: "secrets",
		Operation: operation, Namespace: guard.Namespace, Name: guard.SecretName, DryRun: true,
		ObjectName: guard.SecretName,
	}
}

func mustGuard(t *testing.T) Guard {
	t.Helper()
	guard, err := Build(testCellKey)
	if err != nil {
		t.Fatalf("build fixture guard: %v", err)
	}
	return guard
}

func cloneGuard(t *testing.T, guard Guard) Guard {
	t.Helper()
	document, err := json.Marshal(guard)
	if err != nil {
		t.Fatalf("marshal guard clone: %v", err)
	}
	var clone Guard
	if err := json.Unmarshal(document, &clone); err != nil {
		t.Fatalf("unmarshal guard clone: %v", err)
	}
	return clone
}

func flattenAuthorizationVerbs(rules []AuthorizationRule) []string {
	var verbs []string
	for _, rule := range rules {
		verbs = append(verbs, rule.Verbs...)
	}
	return verbs
}
