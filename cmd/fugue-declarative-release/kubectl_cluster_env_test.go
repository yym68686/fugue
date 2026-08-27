package main

import (
	"testing"

	"fugue/internal/declarativerelease"
)

func TestOmittedLegacyEnvironmentEntriesAreBoundToLegacyOwnership(t *testing.T) {
	desired := map[string]any{"spec": map[string]any{"template": map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/declarative-environment-tombstones": "OLD"}},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "env": []any{map[string]any{"name": "KEEP", "value": "1"}}}}},
	}}}

	legacyFields := map[string]any{"f:spec": map[string]any{"f:template": map[string]any{"f:spec": map[string]any{
		"f:containers": map[string]any{"k:{\"name\":\"api\"}": map[string]any{
			"f:env": map[string]any{"k:{\"name\":\"OLD\"}": map[string]any{
				".": map[string]any{}, "f:name": map[string]any{},
				"f:valueFrom": map[string]any{"f:secretKeyRef": map[string]any{"f:name": map[string]any{}}},
			}},
		}},
	}}}}
	live := map[string]any{
		"metadata": map[string]any{"managedFields": []any{map[string]any{"manager": "helm", "operation": "Update", "fieldsV1": legacyFields}}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "api", "env": []any{
				map[string]any{"name": "KEEP", "value": "1"},
				map[string]any{"name": "OLD", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "config"}}},
			}}},
		}}},
	}
	entries, err := omittedLegacyEnvironmentEntries(desired, live)
	if err != nil {
		t.Fatalf("find omitted legacy environment: %v", err)
	}
	if len(entries) != 1 || entries[0].containerIndex != 0 || entries[0].envIndex != 1 || entries[0].containerName != "api" || entries[0].envName != "OLD" {
		t.Fatalf("unexpected legacy environment entries: %+v", entries)
	}
}

func TestOmittedEnvironmentOwnedByDeclarativeManagerIsNotPruned(t *testing.T) {
	desired := map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
		"containers": []any{map[string]any{"name": "api"}},
	}}}}
	fields := map[string]any{"f:spec": map[string]any{"f:template": map[string]any{"f:spec": map[string]any{
		"f:containers": map[string]any{"k:{\"name\":\"api\"}": map[string]any{
			"f:env": map[string]any{"k:{\"name\":\"OLD\"}": map[string]any{".": map[string]any{}, "f:name": map[string]any{}}},
		}},
	}}}}
	live := map[string]any{
		"metadata": map[string]any{"managedFields": []any{map[string]any{"manager": "fugue-api-declarative", "operation": "Apply", "fieldsV1": fields}}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "api", "env": []any{map[string]any{"name": "OLD"}}}},
		}}},
	}
	entries, err := omittedLegacyEnvironmentEntries(desired, live)
	if err != nil {
		t.Fatalf("find omitted environment: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("declarative-owned environment must not be pruned: %+v", entries)
	}
}

func TestRetryLegacyEnvironmentNormalizationIgnoresOnlyOwnedTombstones(t *testing.T) {
	desired := map[string]any{"spec": map[string]any{"template": map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{retryLegacyEnvironmentTombstonesAnnotation: "OLD"}},
		"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "env": []any{map[string]any{"name": "KEEP", "value": "1"}}}}},
	}}}
	live := map[string]any{
		"metadata": map[string]any{"managedFields": []any{map[string]any{"manager": "helm", "operation": "Update", "fieldsV1": map[string]any{"f:spec": map[string]any{"f:template": map[string]any{"f:spec": map[string]any{
			"f:containers": map[string]any{"k:{\"name\":\"api\"}": map[string]any{"f:env": map[string]any{"k:{\"name\":\"OLD\"}": map[string]any{".": map[string]any{}, "f:name": map[string]any{}}}}},
		}}}}}}},
		"spec": map[string]any{"template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "api", "env": []any{
				map[string]any{"name": "KEEP", "value": "1"}, map[string]any{"name": "OLD", "value": "stale"},
			}}}},
		}},
	}
	normalizedDesired, normalizedLive, err := normalizeRetryLegacyEnvironment(desired, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(anySlice(mapField(mapField(mapField(normalizedLive, "spec"), "template"), "spec")["containers"])) != 1 {
		t.Fatal("normalized live container inventory changed")
	}
	container := anySlice(mapField(mapField(mapField(normalizedLive, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	if len(anySlice(container["env"])) != 1 || stringValue(mapFieldFromAny(anySlice(container["env"])[0])["name"]) != "KEEP" {
		t.Fatalf("owned tombstone was not removed from comparison copy: %+v", container["env"])
	}
	if mapField(mapField(mapField(normalizedDesired, "spec"), "template"), "metadata")["annotations"] == nil {
		t.Fatal("normalized desired metadata disappeared")
	}
	if !declarativerelease.ResourceDesiredSubset(normalizedDesired, normalizedLive) {
		t.Fatalf("normalized predecessor should match the live runtime shape: desired=%+v live=%+v", normalizedDesired, normalizedLive)
	}
}
