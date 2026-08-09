package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testIntent() retirementIntent {
	return retirementIntent{
		APIVersion: "release.fugue.dev/v1",
		Kind:       "HelmLedgerRetirementIntent",
		Release:    "fugue",
		Namespace:  "fugue-system",
		RetiredResources: []retiredResource{
			{resourceIdentity: resourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "legacy-edge"}},
		},
	}
}

func TestFilterRetiresOnlyTheAllowlistedIdentity(t *testing.T) {
	current := []byte(`---
# Source: fugue/templates/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: fugue-api
spec:
  ports:
    - port: 80
---
# Source: fugue/templates/edge.yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: legacy-edge
spec:
  selector:
    matchLabels:
      app: edge
`)
	target := []byte(`---
apiVersion: v1
kind: Service
metadata:
  name: fugue-api
spec:
  ports:
    - port: 81
`)
	got, err := filter(current, target, testIntent())
	if err != nil {
		t.Fatal(err)
	}
	text := string(got)
	if strings.Contains(text, "legacy-edge") || !strings.Contains(text, "name: fugue-api") || !strings.Contains(text, "port: 80") || strings.Contains(text, "port: 81") {
		t.Fatalf("filter did not freeze the current manifest while retiring one identity:\n%s", text)
	}
}

func TestFilterFailsClosedOnIdentityDrift(t *testing.T) {
	current := []byte(`apiVersion: apps/v1
kind: DaemonSet
metadata: {name: legacy-edge}
---
apiVersion: v1
kind: Service
metadata: {name: fugue-api}
`)
	for name, target := range map[string]string{
		"retired object remains": `apiVersion: apps/v1
kind: DaemonSet
metadata: {name: legacy-edge}
---
apiVersion: v1
kind: Service
metadata: {name: fugue-api}
`,
		"surviving object disappears": ``,
		"new object appears": `apiVersion: v1
kind: Service
metadata: {name: fugue-api}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: unexpected}
`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := filter(current, []byte(target), testIntent()); err == nil {
				t.Fatal("identity drift was accepted")
			}
		})
	}
}

func TestDecodeIntentRejectsDuplicateRetirementIdentity(t *testing.T) {
	intent := testIntent()
	intent.RetiredResources = append(intent.RetiredResources, intent.RetiredResources[0])
	raw, err := json.Marshal(intent)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "intent.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeIntent(path); err == nil {
		t.Fatal("duplicate retirement identity was accepted")
	}
}
