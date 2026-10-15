package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	"fugue/internal/declarativerelease"
)

func cpuPolicyFixture(t *testing.T) (declarativerelease.PlanRelease, declarativerelease.RenderedManifests, []byte) {
	t.Helper()
	release := declarativerelease.PlanRelease{ComponentID: "controller", Workload: declarativerelease.Workload{Kind: "Deployment", Name: "controller", Namespace: "platform", Container: "controller"}}
	raw := []byte(`{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"controller","namespace":"platform","annotations":{"code":"old"}},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"controller","image":"example/controller@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","env":[{"name":"FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_REQUEST_UPSCALE_ENABLED","value":"false"},{"name":"FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_LIMIT_UPSCALE_ENABLED","value":"false"},{"name":"OTHER_GATE","value":"false"}]}]}}}}]}`)
	config := []byte(`{"schema":"fugue.managed-postgres-cpu-policy/v1","namespace":"platform","deployment":"controller","container":"controller","expectedImage":"example/controller@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expectedConfigSha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","cpuRequestUpscale":true,"cpuLimitUpscale":true}`)
	return release, declarativerelease.RenderedManifests{LKG: raw, Forward: bytes.Replace(raw, []byte(`"code":"old"`), []byte(`"code":"new"`), 1)}, config
}

func TestIndependentCPUPolicySurvivesCodeForwardAndRollback(t *testing.T) {
	release, rendered, config := cpuPolicyFixture(t)
	original := append([]byte(nil), rendered.LKG...)
	result, err := bindPostgresCPUPolicyBytes(release, rendered, config)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, rendered.LKG) {
		t.Fatal("modified caller's immutable LKG bytes")
	}
	var policy map[string]any
	if err := json.Unmarshal(config, &policy); err != nil {
		t.Fatal(err)
	}
	canonical, _ := declarativerelease.CanonicalJSON(policy)
	for _, tc := range []struct {
		raw          []byte
		digest, code string
	}{{result.Forward, result.ForwardDigest, "new"}, {result.LKG, result.LKGDigest, "old"}} {
		var set declarativerelease.ResourceSet
		if err := json.Unmarshal(tc.raw, &set); err != nil {
			t.Fatal(err)
		}
		meta := mapField(set.Items[0], "metadata")
		annotations := mapField(meta, "annotations")
		if annotations["code"] != tc.code || annotations[postgresCPUPolicyAnnotation] != fmt.Sprintf("%x", sha256.Sum256(canonical)) {
			t.Fatal("code identity or policy receipt changed")
		}
		if tc.digest != fmt.Sprintf("sha256:%x", sha256.Sum256(tc.raw)) {
			t.Fatal("rendered digest not rebound")
		}
		c := anySlice(mapField(mapField(mapField(set.Items[0], "spec"), "template"), "spec")["containers"])[0].(map[string]any)
		for _, name := range []string{"FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_REQUEST_UPSCALE_ENABLED", "FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_LIMIT_UPSCALE_ENABLED"} {
			if v, _ := declaredEnvValue(c, name); v != "true" {
				t.Fatal("policy reverted by code target")
			}
		}
		if v, _ := declaredEnvValue(c, "OTHER_GATE"); v != "false" {
			t.Fatal("unrelated policy changed")
		}
		if c["image"] != "example/controller@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Fatal("code image changed")
		}
	}
}

func TestIndependentCPUPolicyRejectsAmbiguousOrUnreviewedBinding(t *testing.T) {
	for _, mode := range []string{"wrong workload", "unknown field", "nonboolean", "missing gate", "value source", "duplicate gate"} {
		t.Run(mode, func(t *testing.T) {
			release, rendered, config := cpuPolicyFixture(t)
			switch mode {
			case "wrong workload":
				release.Workload.Name = "other"
			case "unknown field":
				config = bytes.Replace(config, []byte(`"cpuLimitUpscale":true`), []byte(`"cpuLimitUpscale":true,"newImage":"unreviewed"`), 1)
			case "nonboolean":
				config = bytes.Replace(config, []byte(`"cpuLimitUpscale":true`), []byte(`"cpuLimitUpscale":"true"`), 1)
			case "missing gate":
				rendered.LKG = bytes.ReplaceAll(rendered.LKG, []byte("CPU_LIMIT_UPSCALE"), []byte("CPU_LIMIT_DOWNSCALE"))
			case "value source":
				rendered.LKG = bytes.Replace(rendered.LKG, []byte(`"value":"false"`), []byte(`"valueFrom":{"secretKeyRef":{"name":"secret","key":"key"}}`), 1)
			case "duplicate gate":
				rendered.LKG = bytes.Replace(rendered.LKG, []byte(`"name":"OTHER_GATE"`), []byte(`"name":"FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_LIMIT_UPSCALE_ENABLED"`), 1)
			}
			if _, err := bindPostgresCPUPolicyBytes(release, rendered, config); err == nil {
				t.Fatal("unsafe configuration binding accepted")
			}
		})
	}
}
