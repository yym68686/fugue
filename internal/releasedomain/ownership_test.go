package releasedomain

import (
	"os"
	"path/filepath"
	"testing"
)

func testOwnership(t *testing.T) *OwnershipSpec {
	t.Helper()
	path := filepath.Join("..", "..", "deploy", "release-domains", "ownership-v1.yaml")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open ownership: %v", err)
	}
	defer file.Close()
	spec, err := LoadOwnership(file)
	if err != nil {
		t.Fatalf("load ownership: %v", err)
	}
	return spec
}

func testBindings() map[string]string {
	return map[string]string{
		"releaseName":                    "fugue",
		"releaseNamespace":               "fugue-system",
		"nodeLocalNamespace":             "kube-system",
		"nodeLocalName":                  "fugue-node-local-dns",
		"nodeLocalUpstreamServiceName":   "fugue-dns-upstream",
		"nodeLocalActiveName":            "fugue-node-local-dns-active",
		"dnsName":                        "fugue-dns",
		"apiName":                        "fugue-api",
		"controllerName":                 "fugue-controller",
		"telemetryAgentName":             "fugue-telemetry-agent",
		"serviceName":                    "fugue",
		"ingressName":                    "fugue",
		"imageCacheName":                 "fugue-image-cache",
		"controlPlanePostgresName":       "fugue-control-plane-postgres",
		"controlPlanePostgresSecretName": "fugue-control-plane-postgres-app",
		"controlPlaneRestoreDrillName":   "fugue-control-plane-restore-drill",
	}
}

func TestOwnershipV1ValidatesAllFiveDomains(t *testing.T) {
	spec := testOwnership(t)
	if !equalDomains(spec.Domains, KnownDomains()) {
		t.Fatalf("domains = %v", spec.Domains)
	}
	if len(spec.ObjectRules) < 20 {
		t.Fatalf("expected full object allowlist, got %d rules", len(spec.ObjectRules))
	}
	if err := spec.ValidateBindings(testBindings()); err != nil {
		t.Fatalf("bindings: %v", err)
	}
}

func TestEveryObjectRuleHasOneSyntheticMatch(t *testing.T) {
	spec := testOwnership(t)
	bindings := testBindings()
	for _, targetRule := range spec.ObjectRules {
		t.Run(targetRule.ID, func(t *testing.T) {
			nameTemplate := targetRule.Name
			nameSuffix := ""
			if nameTemplate == "" {
				nameTemplate = targetRule.NamePrefix
				nameSuffix = "group-a"
			}
			name, err := expandBindings(nameTemplate, bindings)
			if err != nil {
				t.Fatal(err)
			}
			name += nameSuffix
			namespace := ""
			if targetRule.Scope == ScopeNamespaced {
				namespace, err = expandBindings(targetRule.Namespace, bindings)
				if err != nil {
					t.Fatal(err)
				}
			}
			labels := map[string]string{}
			for key, value := range targetRule.RequiredLabels {
				labels[key], err = expandBindings(value, bindings)
				if err != nil {
					t.Fatal(err)
				}
			}
			if targetRule.NameSuffixLabel != nil {
				labels[targetRule.NameSuffixLabel.Key] = targetRule.NameSuffixLabel.ValuePrefix + nameSuffix
			}
			object := manifestObject{
				Identity: ObjectIdentity{
					APIGroup: targetRule.APIGroup, Version: targetRule.Version, Kind: targetRule.Kind,
					Namespace: namespace, Name: name,
				},
				Labels: labels,
			}
			matches := make([]string, 0)
			for _, candidate := range spec.ObjectRules {
				matched, err := candidate.matches(object, bindings["releaseNamespace"], bindings)
				if err != nil {
					t.Fatalf("match %s: %v", candidate.ID, err)
				}
				if matched {
					matches = append(matches, candidate.ID)
				}
			}
			if len(matches) != 1 || matches[0] != targetRule.ID {
				t.Fatalf("synthetic object matched %v, want only %s", matches, targetRule.ID)
			}
		})
	}
}

func TestCNPGBackupPointerOverridesControlPlane(t *testing.T) {
	spec := testOwnership(t)
	for _, rule := range spec.ObjectRules {
		if rule.ID != "control-plane-cnpg-cluster" {
			continue
		}
		if got := rule.domainForPointer("/spec/instances"); got != DomainControlPlane {
			t.Fatalf("instances domain = %s", got)
		}
		if got := rule.domainForPointer("/spec/backup/barmanObjectStore/destinationPath"); got != DomainBackup {
			t.Fatalf("backup domain = %s", got)
		}
		return
	}
	t.Fatal("CNPG cluster rule not found")
}

func TestValidateBindingsFailsClosed(t *testing.T) {
	for _, missing := range []string{"releaseName", "nodeLocalActiveName"} {
		t.Run(missing, func(t *testing.T) {
			spec := testOwnership(t)
			bindings := testBindings()
			delete(bindings, missing)
			if err := spec.ValidateBindings(bindings); err == nil {
				t.Fatalf("expected missing %s binding error", missing)
			}
		})
	}
}

func TestValidateRejectsDuplicateDomainDeclaration(t *testing.T) {
	spec := testOwnership(t)
	spec.Domains = append(spec.Domains, DomainNodeLocal)
	if err := spec.Validate(); err == nil {
		t.Fatal("expected duplicate domain declaration to fail")
	}
}
