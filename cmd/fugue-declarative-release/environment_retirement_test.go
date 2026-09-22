package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
)

func retirementFixture(t *testing.T) (declarativerelease.PlanRelease, map[string]any, map[string]any, *kubectlCluster) {
	t.Helper()
	r := declarativerelease.PlanRelease{ComponentID: "dns-client", ExpectedPreviousPresent: true, Workload: declarativerelease.Workload{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "system", Name: "dns-agent", Container: "dns", FieldManager: "dns-declarative"}}
	var old map[string]any
	if err := json.Unmarshal([]byte(`{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"dns-agent","namespace":"system"},"spec":{"template":{"spec":{"containers":[{"name":"dns","image":"old","env":[{"name":"KEEP","value":"yes"},{"name":"RETIRED_A","value":"old-a"},{"name":"RETIRED_B","value":"old-b"}]}]}}}}`), &old); err != nil {
		t.Fatal(err)
	}
	next := deepCopyJSONMap(t, old)
	c, _, _ := retirementContainer(next, "containers", "dns")
	c["image"] = "new"
	c["env"] = []any{map[string]any{"name": "KEEP", "value": "yes"}, map[string]any{"name": "NEW", "value": "new"}}
	set := func(item map[string]any) []byte {
		return mustJSON(t, map[string]any{"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{item}})
	}
	cluster := &kubectlCluster{}
	if err := cluster.BindManifestTransition(r, set(next), set(old)); err != nil {
		t.Fatal(err)
	}
	return r, old, next, cluster
}
func retirementLive(t *testing.T, r declarativerelease.PlanRelease, old map[string]any) map[string]any {
	live := deepCopyJSONMap(t, old)
	m := mapField(live, "metadata")
	m["uid"], m["resourceVersion"], m["generation"] = "uid-one", "41", json.Number("7")
	entries := map[string]any{}
	c, _, _ := retirementContainer(old, "containers", "dns")
	for _, raw := range anySlice(c["env"]) {
		name := stringValue(mapFieldValue(raw, "name"))
		entries[`k:{"name":"`+name+`"}`] = map[string]any{".": map[string]any{}, "f:name": map[string]any{}, "f:value": map[string]any{}}
	}
	fields := map[string]any{"f:spec": map[string]any{"f:template": map[string]any{"f:spec": map[string]any{"f:containers": map[string]any{`k:{"name":"dns"}`: map[string]any{"f:env": entries}}}}}}
	m["managedFields"] = []any{map[string]any{"manager": r.Workload.FieldManager, "operation": "Apply", "fieldsV1": fields}, map[string]any{"manager": "helm", "operation": "Update", "fieldsV1": fields}}
	return live
}
func bindRetirementCAS(t *testing.T, next, live map[string]any) map[string]any {
	desired := deepCopyJSONMap(t, next)
	d, m := mapField(desired, "metadata"), mapField(live, "metadata")
	d["uid"], d["resourceVersion"] = m["uid"], m["resourceVersion"]
	return desired
}

func TestEnvironmentRetirementPatchAndRollbackAreManifestBound(t *testing.T) {
	r, old, next, cluster := retirementFixture(t)
	live := retirementLive(t, r, old)
	desired := bindRetirementCAS(t, next, live)
	retired := cluster.envRetirements[retirementResourceKey(desired)]
	if len(retired) != 2 {
		t.Fatal("forward deletion authority", retired)
	}
	patch, expected, err := retirementPatch(desired, live, r.Workload.FieldManager, retired)
	if err != nil {
		t.Fatal(err)
	}
	p, err := jsonpatch.DecodePatch(mustJSON(t, patch))
	if err != nil {
		t.Fatal(err)
	}
	actual, err := p.Apply(mustJSON(t, live))
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeJSONObject(actual)
	if err != nil || digestJSON(got) != digestJSON(expected) {
		t.Fatal("patch changed more than retired entries", err)
	}
	c, _, _ := retirementContainer(got, "containers", "dns")
	if c["image"] != "old" || len(anySlice(c["env"])) != 1 {
		t.Fatal("retirement applied target fields early")
	}
	for _, field := range []string{"uid", "resourceVersion"} {
		changed := deepCopyJSONMap(t, live)
		mapField(changed, "metadata")[field] = "999"
		if _, err := p.Apply(mustJSON(t, changed)); err == nil {
			t.Fatal("CAS mutation accepted", field)
		}
	}
	for _, n := range []string{"RETIRED_A", "RETIRED_B"} {
		changed := deepCopyJSONMap(t, live)
		c, _, _ := retirementContainer(changed, "containers", "dns")
		for _, raw := range anySlice(c["env"]) {
			e := raw.(map[string]any)
			if e["name"] == n {
				e["value"] = "raced"
			}
		}
		if _, err := p.Apply(mustJSON(t, changed)); err == nil {
			t.Fatal("changed old value accepted")
		}
	}
	reverse := cluster.envRetirements[retirementResourceKey(old)]
	if len(reverse) != 1 || reverse[0].Entry["name"] != "NEW" {
		t.Fatal("rollback lacks exact inverse authority", reverse)
	}
	changed := deepCopyJSONMap(t, next)
	c, _, _ = retirementContainer(changed, "containers", "dns")
	c["image"] = "unreviewed"
	if len(cluster.envRetirements[retirementResourceKey(changed)]) != 0 {
		t.Fatal("altered target inherited retirement authority")
	}
}

func TestEnvironmentRetirementRejectsDriftAndForeignOwnership(t *testing.T) {
	r, old, next, cluster := retirementFixture(t)
	base := retirementLive(t, r, old)
	desired := bindRetirementCAS(t, next, base)
	retired := cluster.envRetirements[retirementResourceKey(desired)]
	for _, scenario := range []string{"uid", "rv", "value", "unknown-owner", "lost-owner", "duplicate", "value-from"} {
		t.Run(scenario, func(t *testing.T) {
			live := deepCopyJSONMap(t, base)
			m := mapField(live, "metadata")
			c, _, _ := retirementContainer(live, "containers", "dns")
			env := anySlice(c["env"])
			entry := env[1].(map[string]any)
			switch scenario {
			case "uid":
				m["uid"] = "other"
			case "rv":
				m["resourceVersion"] = "42"
			case "value":
				entry["value"] = "raced"
			case "unknown-owner":
				m["managedFields"].([]any)[1].(map[string]any)["manager"] = "unknown"
			case "lost-owner":
				m["managedFields"] = m["managedFields"].([]any)[1:]
			case "duplicate":
				c["env"] = append(env, entry)
			case "value-from":
				delete(entry, "value")
				entry["valueFrom"] = map[string]any{"secretKeyRef": map[string]any{"name": "private", "key": "value"}}
			}
			if _, _, err := retirementPatch(desired, live, r.Workload.FieldManager, retired); err == nil {
				t.Fatal("unreviewed deletion accepted")
			}
		})
	}
}

func TestEnvironmentRetirementDryRunDoesNotWriteAndApplyRebindsOnlyReviewedChange(t *testing.T) {
	for _, scenario := range []string{"dry-run", "apply", "response-loss", "extra-change", "wrong-generation"} {
		t.Run(scenario, func(t *testing.T) {
			r, old, next, cluster := retirementFixture(t)
			live := retirementLive(t, r, old)
			desired := bindRetirementCAS(t, next, live)
			_, fresh, err := retirementPatch(desired, live, r.Workload.FieldManager, cluster.envRetirements[retirementResourceKey(desired)])
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "dry-run" {
				m := mapField(fresh, "metadata")
				m["resourceVersion"], m["generation"] = "42", json.Number("8")
			}
			if scenario == "extra-change" {
				c, _, _ := retirementContainer(fresh, "containers", "dns")
				c["image"] = "foreign"
			}
			if scenario == "wrong-generation" {
				mapField(fresh, "metadata")["generation"] = json.Number("9")
			}
			d := t.TempDir()
			script := filepath.Join(d, "kubectl")
			log := filepath.Join(d, "calls")
			t.Setenv("RETIRE_TEST_LIVE", string(mustJSON(t, live)))
			t.Setenv("RETIRE_TEST_FRESH", string(mustJSON(t, fresh)))
			t.Setenv("RETIRE_TEST_LOG", log)
			t.Setenv("RETIRE_TEST_MODE", scenario)
			program := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$RETIRE_TEST_LOG"
case "$1" in
 get) printf '%s' "$RETIRE_TEST_LIVE";;
 patch) if [ "$RETIRE_TEST_MODE" = response-loss ]; then exit 42; fi
        printf '%s' "$RETIRE_TEST_FRESH";;
 *) exit 43;;
esac
`
			if err := os.WriteFile(script, []byte(program), 0700); err != nil {
				t.Fatal(err)
			}
			cluster.kubectl, cluster.timeout = script, time.Second
			id := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "system", Name: "dns-agent"}
			output, _, _, err := cluster.retireEnvironment(context.Background(), r, id, desired, mustJSON(t, desired), scenario == "dry-run")
			if scenario == "response-loss" || scenario == "extra-change" || scenario == "wrong-generation" {
				if err == nil {
					t.Fatal("unproven patch accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rv := "42"
			if scenario == "dry-run" {
				rv = "41"
			}
			if mapField(output, "metadata")["resourceVersion"] != rv {
				t.Fatal("incorrect rebind")
			}
			calls, _ := os.ReadFile(log)
			if strings.Contains(string(calls), "--dry-run=server") != (scenario == "dry-run") {
				t.Fatal("dry-run mutated cluster")
			}
		})
	}
}

func TestEnvironmentRetirementFirstInstallHasNoDeletionAuthority(t *testing.T) {
	r, _, next, c := retirementFixture(t)
	r.ExpectedPreviousPresent = false
	if err := c.BindManifestTransition(r, mustJSON(t, next), []byte(`{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":null}`)); err != nil || len(c.envRetirements) != 0 {
		t.Fatal("bootstrap retained deletion authority", err)
	}
}

func TestEnvironmentRetirementLeavesReferenceChangesToOrdinarySSA(t *testing.T) {
	r, old, next, cluster := retirementFixture(t)
	c, _, _ := retirementContainer(next, "containers", "dns")
	c["env"] = append(anySlice(c["env"]), map[string]any{"name": "REFERENCE", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "credentials", "key": "token"}}})
	set := func(item map[string]any) []byte {
		return mustJSON(t, map[string]any{"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{item}})
	}
	if err := cluster.BindManifestTransition(r, set(next), set(old)); err != nil {
		t.Fatal("normal reference addition was blocked", err)
	}
	reverse := cluster.envRetirements[retirementResourceKey(old)]
	if len(reverse) != 1 || reverse[0].Entry["name"] != "NEW" {
		t.Fatal("reference deletion entered literal retirement bridge")
	}
}

func TestEnvironmentRetirementAcceptsOnlyKubernetesEmptyStringDefault(t *testing.T) {
	r, old, next, cluster := retirementFixture(t)
	live := retirementLive(t, r, old)
	desired := bindRetirementCAS(t, next, live)
	retired := cluster.envRetirements[retirementResourceKey(desired)]
	for i := range retired {
		retired[i].Entry["value"] = ""
	}
	c, _, _ := retirementContainer(live, "containers", "dns")
	for _, raw := range anySlice(c["env"]) {
		entry := raw.(map[string]any)
		if strings.HasPrefix(stringValue(entry["name"]), "RETIRED_") {
			delete(entry, "value")
		}
	}
	if _, _, err := retirementPatch(desired, live, r.Workload.FieldManager, retired); err != nil {
		t.Fatal("API omitted literal empty value rejected", err)
	}
	anySlice(c["env"])[1].(map[string]any)["valueFrom"] = map[string]any{"fieldRef": map[string]any{"fieldPath": "metadata.name"}}
	if _, _, err := retirementPatch(desired, live, r.Workload.FieldManager, retired); err == nil {
		t.Fatal("reference treated as empty literal")
	}
}

func TestEnvironmentRetirementApplyRetriesOnlyObservationCASUpdates(t *testing.T) {
	for _, scenario := range []string{"status-only", "spec-race", "uid-race", "generation-race", "exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			r, old, next, cluster := retirementFixture(t)
			live := retirementLive(t, r, old)
			desired := bindRetirementCAS(t, next, live)
			_, patched, err := retirementPatch(desired, live, r.Workload.FieldManager, cluster.envRetirements[retirementResourceKey(desired)])
			if err != nil {
				t.Fatal(err)
			}
			mapField(patched, "metadata")["resourceVersion"] = "42"
			mapField(patched, "metadata")["generation"] = json.Number("8")
			latest := deepCopyJSONMap(t, patched)
			m := mapField(latest, "metadata")
			m["resourceVersion"] = "43"
			latest["status"] = map[string]any{"numberReady": 0}
			switch scenario {
			case "spec-race":
				c, _, _ := retirementContainer(latest, "containers", "dns")
				c["image"] = "raced"
			case "uid-race":
				m["uid"] = "replacement"
			case "generation-race":
				m["generation"] = json.Number("9")
			}
			d := t.TempDir()
			script := filepath.Join(d, "kubectl")
			t.Setenv("RETIRE_APPLY_DIR", d)
			t.Setenv("RETIRE_APPLY_MODE", scenario)
			for n, v := range map[string]map[string]any{"live": live, "patched": patched, "latest": latest} {
				if err := os.WriteFile(filepath.Join(d, n), mustJSON(t, v), 0600); err != nil {
					t.Fatal(err)
				}
			}
			program := `#!/bin/sh
set -eu
case "$1" in
 get)
  if [ -f "$RETIRE_APPLY_DIR/patch-done" ]; then
   cat "$RETIRE_APPLY_DIR/latest"
  else cat "$RETIRE_APPLY_DIR/live"; fi;;
 patch) touch "$RETIRE_APPLY_DIR/patch-done";cat "$RETIRE_APPLY_DIR/patched";;
 apply)
  if [ -f "$RETIRE_APPLY_DIR/apply-count" ]; then n=$(cat "$RETIRE_APPLY_DIR/apply-count"); else n=0; fi
  n=$((n+1));printf '%s' "$n" > "$RETIRE_APPLY_DIR/apply-count"
  cat > "$RETIRE_APPLY_DIR/request-$n"
  if [ "$n" -eq 1 ] || [ "$RETIRE_APPLY_MODE" = exhausted ]; then
   printf 'Operation cannot be fulfilled: the object has been modified\n' >&2;exit 1
  fi
  cat "$RETIRE_APPLY_DIR/latest";;
 *) exit 43;;
esac
`
			if err := os.WriteFile(script, []byte(program), 0700); err != nil {
				t.Fatal(err)
			}
			cluster.kubectl, cluster.timeout = script, time.Second
			id := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "system", Name: "dns-agent"}
			err = cluster.applyResourceWithOwnershipConvergence(context.Background(), r, id, desired, mustJSON(t, desired), false)
			countRaw, _ := os.ReadFile(filepath.Join(d, "apply-count"))
			count := string(countRaw)
			if scenario == "status-only" {
				if err != nil || count != "2" {
					t.Fatal("safe status race not retried", count, err)
				}
				raw, _ := os.ReadFile(filepath.Join(d, "request-2"))
				retry, _ := decodeJSONObject(raw)
				if mapField(retry, "metadata")["resourceVersion"] != "43" {
					t.Fatal("retry CAS not refreshed")
				}
				c, _, _ := retirementContainer(retry, "containers", "dns")
				if c["image"] != "new" {
					t.Fatal("retry lost reviewed target")
				}
			} else {
				if err == nil {
					t.Fatal("unsafe race accepted")
				}
				if scenario != "exhausted" && count != "1" {
					t.Fatal("spec drift retried", count)
				}
				if scenario == "exhausted" && !strings.Contains(err.Error(), "object has been modified") {
					t.Fatal("original conflict lost", err)
				}
			}
		})
	}
}
