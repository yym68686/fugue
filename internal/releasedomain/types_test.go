package releasedomain

import (
	"strings"
	"testing"
)

func TestKnownDomainsReturnsCanonicalCopy(t *testing.T) {
	domains := KnownDomains()
	if len(domains) != 5 || domains[0] != DomainNodeLocal || domains[4] != DomainBackup {
		t.Fatalf("unexpected domains: %v", domains)
	}
	domains[0] = DomainBackup
	if KnownDomains()[0] != DomainNodeLocal {
		t.Fatal("KnownDomains returned shared mutable storage")
	}
}

func TestObjectIdentityString(t *testing.T) {
	identity := ObjectIdentity{APIGroup: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "kube-system", Name: "dns"}
	if got, want := identity.String(), "apps/v1 DaemonSet kube-system/dns"; got != want {
		t.Fatalf("identity string = %q, want %q", got, want)
	}
}

func TestClassificationContextEvidenceIsCanonicalAndDetached(t *testing.T) {
	bindings := map[string]string{
		"nodeLocalName":    "fugue-node-local-dns",
		"releaseNamespace": "fugue-system",
		"apiName":          "fugue-api",
	}
	first, err := NewClassificationContextEvidence("fugue-system", bindings, true)
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	second, err := NewClassificationContextEvidence("fugue-system", map[string]string{
		"apiName":          "fugue-api",
		"releaseNamespace": "fugue-system",
		"nodeLocalName":    "fugue-node-local-dns",
	}, true)
	if err != nil {
		t.Fatalf("new reordered context: %v", err)
	}
	if first.Digest != second.Digest {
		t.Fatalf("context digest depends on map order: %s != %s", first.Digest, second.Digest)
	}
	if got := []string{first.Bindings[0].Name, first.Bindings[1].Name, first.Bindings[2].Name}; strings.Join(got, ",") != "apiName,nodeLocalName,releaseNamespace" {
		t.Fatalf("bindings are not canonical: %v", got)
	}

	bindings["apiName"] = "mutated-after-snapshot"
	copy := first.BindingMap()
	copy["nodeLocalName"] = "mutated-copy"
	if first.BindingMap()["apiName"] != "fugue-api" || first.BindingMap()["nodeLocalName"] != "fugue-node-local-dns" {
		t.Fatalf("persisted context observed external mutation: %#v", first)
	}
	if err := VerifyClassificationContextEvidence(first); err != nil {
		t.Fatalf("verify context: %v", err)
	}
}

func TestClassificationContextEvidenceRejectsMismatchAndMutation(t *testing.T) {
	if _, err := NewClassificationContextEvidence("fugue-system", map[string]string{
		"releaseNamespace": "other-system",
	}, false); err == nil {
		t.Fatal("expected namespace/binding mismatch to fail")
	}
	context, err := NewClassificationContextEvidence("fugue-system", map[string]string{
		"releaseNamespace": "fugue-system",
		"apiName":          "fugue-api",
	}, false)
	if err != nil {
		t.Fatalf("new context: %v", err)
	}
	context.Bindings[0].Value = "mutated"
	if err := VerifyClassificationContextEvidence(context); err == nil {
		t.Fatal("expected mutation to invalidate classification context digest")
	}
}

func TestClassificationContextEvidenceRejectsInvalidUTF8WithoutDigestCollision(t *testing.T) {
	invalidValues := []string{"bad-\xff", "bad-\xfe"}
	for _, value := range invalidValues {
		if _, err := NewClassificationContextEvidence("fugue-system", map[string]string{
			"releaseNamespace": "fugue-system",
			"extra":            value,
		}, false); err == nil {
			t.Fatalf("invalid binding value %q was accepted", value)
		}
		if _, err := NewClassificationContextEvidence(value, map[string]string{
			"releaseNamespace": value,
		}, false); err == nil {
			t.Fatalf("invalid namespace %q was accepted", value)
		}
	}

	context, err := NewClassificationContextEvidence("fugue-system", map[string]string{
		"releaseNamespace": "fugue-system",
	}, false)
	if err != nil {
		t.Fatalf("new valid context: %v", err)
	}
	context.Bindings = append(context.Bindings, ClassificationBinding{Name: "extra", Value: invalidValues[0]})
	context.Digest = classificationContextDigest(context)
	if err := VerifyClassificationContextEvidence(context); err == nil {
		t.Fatal("verification accepted invalid UTF-8 with a matching replacement-based digest")
	}

	context, err = NewClassificationContextEvidence("fugue-system", map[string]string{
		"releaseNamespace": "fugue-system",
	}, false)
	if err != nil {
		t.Fatalf("new valid context: %v", err)
	}
	context.Digest = "sha256:invalid-\xff"
	if err := VerifyClassificationContextEvidence(context); err == nil || !strings.Contains(err.Error(), "not valid UTF-8") {
		t.Fatalf("invalid context digest error = %v", err)
	}
}
