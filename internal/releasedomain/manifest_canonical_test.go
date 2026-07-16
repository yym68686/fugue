package releasedomain

import (
	"bytes"
	"strings"
	"testing"
)

func TestCanonicalizeRenderedManifestIsDeterministicAndClassifierEquivalent(t *testing.T) {
	spec := &OwnershipSpec{}
	first := []byte(`apiVersion: v1
kind: ConfigMap
metadata:
  name: second
  creationTimestamp: null
  labels:
    z: last
    a: first
status:
  ignored: true
spec:
  count: 1
  ratio: 0.10
  exact: !!float 1/3
  big: 9007199254740993
  text: "1"
  ordered: [a, b]
---
kind: ConfigMap
apiVersion: v1
metadata: {name: first}
data: {value: stable}
`)
	second := []byte(`apiVersion: v1
data:
  value: stable
metadata:
  name: first
kind: ConfigMap
---
spec:
  ordered:
    - a
    - b
  text: '1'
  big: !!float 9007199254740993.0
  exact: !!float "1/3"
  ratio: 1e-1
  count: 1.0
metadata:
  labels: {a: first, z: last}
  name: second
apiVersion: v1
kind: ConfigMap
`)
	canonicalFirst, err := CanonicalizeRenderedManifest(first, spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	canonicalSecond, err := CanonicalizeRenderedManifest(second, spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonicalFirst, canonicalSecond) {
		t.Fatalf("equivalent manifests canonicalized differently:\n%s\n--- versus ---\n%s", canonicalFirst, canonicalSecond)
	}
	if !bytes.Contains(canonicalFirst, []byte("namespace: fugue-system")) {
		t.Fatalf("default namespace is absent:\n%s", canonicalFirst)
	}
	for _, forbidden := range []string{"creationTimestamp", "status:"} {
		if bytes.Contains(canonicalFirst, []byte(forbidden)) {
			t.Fatalf("canonical manifest retained %q:\n%s", forbidden, canonicalFirst)
		}
	}
	repeated, err := CanonicalizeRenderedManifest(canonicalFirst, spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonicalFirst, repeated) {
		t.Fatalf("canonicalization is not idempotent:\n%s\n%s", canonicalFirst, repeated)
	}
	classification := ClassifyRendered(first, canonicalFirst, spec, RenderedOptions{DefaultNamespace: "fugue-system"})
	if len(classification.Domains) != 0 || len(classification.Unknown) != 0 {
		t.Fatalf("canonical manifest changed classifier semantics: %#v", classification)
	}
}

func TestCanonicalizeRenderedManifestPreservesSignificantTypesAndOrder(t *testing.T) {
	spec := &OwnershipSpec{}
	manifest := func(value string) []byte {
		return []byte("apiVersion: v1\nkind: Secret\nmetadata:\n  name: typed\n  annotations:\n    helm.sh/hook: pre-upgrade\nspec:\n" + value + "\n")
	}
	for _, test := range []struct {
		name  string
		left  string
		right string
	}{
		{name: "number versus string", left: "  value: 1", right: "  value: '1'"},
		{name: "boolean versus string", left: "  value: true", right: "  value: 'true'"},
		{name: "array order", left: "  value: [a, b]", right: "  value: [b, a]"},
		{name: "secret value", left: "  value: secret-a", right: "  value: secret-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			left, err := CanonicalizeRenderedManifest(manifest(test.left), spec, "fugue-system")
			if err != nil {
				t.Fatal(err)
			}
			right, err := CanonicalizeRenderedManifest(manifest(test.right), spec, "fugue-system")
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(left, right) {
				t.Fatalf("significant change was canonicalized away:\n%s", left)
			}
		})
	}
}

func TestCanonicalizeRenderedManifestExpandsLists(t *testing.T) {
	spec := &OwnershipSpec{}
	list := []byte(`apiVersion: v1
kind: List
items:
  - apiVersion: v1
    kind: ConfigMap
    metadata: {name: second}
  - apiVersion: v1
    kind: ConfigMap
    metadata: {name: first}
`)
	documents := []byte(`apiVersion: v1
kind: ConfigMap
metadata: {name: first}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: second}
`)
	canonicalList, err := CanonicalizeRenderedManifest(list, spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	canonicalDocuments, err := CanonicalizeRenderedManifest(documents, spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonicalList, canonicalDocuments) {
		t.Fatalf("List expansion drifted:\n%s\n%s", canonicalList, canonicalDocuments)
	}
}

func TestCanonicalizeRenderedManifestRejectsAmbiguousInput(t *testing.T) {
	spec := &OwnershipSpec{}
	for _, input := range []string{
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: duplicate}\ndata: {key: one, key: two}\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: &meta {name: anchored}\ncopy: *meta\n",
		"null\n",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: same}\n",
	} {
		if output, err := CanonicalizeRenderedManifest([]byte(input), spec, "fugue-system"); err == nil {
			t.Fatalf("ambiguous input unexpectedly succeeded:\n%s", output)
		}
	}
}

func TestRenderedManifestDepthBudgetFailsClosed(t *testing.T) {
	var nested strings.Builder
	nested.WriteString("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: deep}\nspec:\n")
	for depth := 0; depth < maxRenderedManifestDepth+2; depth++ {
		nested.WriteString(strings.Repeat("  ", depth+1) + "nested:\n")
	}
	nested.WriteString(strings.Repeat("  ", maxRenderedManifestDepth+3) + "value: leaf\n")
	if output, err := CanonicalizeRenderedManifest([]byte(nested.String()), &OwnershipSpec{}, "fugue-system"); err == nil {
		t.Fatalf("over-depth manifest unexpectedly succeeded:\n%s", output)
	}
}

func TestRenderedManifestNumberExpansionBudgetFailsClosed(t *testing.T) {
	for _, number := range []string{
		"1e4097",
		"1e-4097",
		"0x1p4097",
		"0x1p-4097",
		strings.Repeat("9", maxRenderedNumberBytes+1),
	} {
		manifest := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: numeric-budget}\ndata:\n  value: !!float " + number + "\n")
		if output, err := CanonicalizeRenderedManifest(manifest, &OwnershipSpec{}, "fugue-system"); err == nil {
			t.Fatalf("expansive number %q unexpectedly succeeded:\n%s", number, output)
		}
	}
}

func TestRenderedManifestLexicalBudgetsFailBeforeYAMLDecode(t *testing.T) {
	tooManyLines := bytes.Repeat([]byte("\n"), maxRenderedManifestLines)
	if err := validateManifestLexicalBudget(tooManyLines); err == nil {
		t.Fatal("over-limit line count unexpectedly succeeded")
	}
	tooMuchFlowSyntax := bytes.Repeat([]byte(","), maxRenderedManifestSyntax+1)
	if err := validateManifestLexicalBudget(tooMuchFlowSyntax); err == nil {
		t.Fatal("over-limit flow syntax unexpectedly succeeded")
	}
}

func TestManifestDiagnosticPathIsBounded(t *testing.T) {
	path := "$"
	for index := 0; index < maxRenderedManifestDepth; index++ {
		path = boundedManifestDiagnosticPath(path, strings.Repeat("key", 1024))
	}
	if path != "$/<path-limit>" {
		t.Fatalf("bounded diagnostic path = %q", path)
	}
	if len(path) > maxManifestDiagnosticPath {
		t.Fatalf("bounded diagnostic path length = %d", len(path))
	}
}
