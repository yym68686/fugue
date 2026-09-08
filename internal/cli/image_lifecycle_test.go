package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestImageInventoryCannotClaimAnActiveProbe(t *testing.T) {
	now := time.Now().UTC()
	digest := "sha256:" + strings.Repeat("a", 64)
	calls := []string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		if r.URL.Path == "/v1/images/img_test" {
			json.NewEncoder(w).Encode(map[string]any{"image": model.Image{ID: "img_test", CanonicalDigest: digest}})
		} else {
			json.NewEncoder(w).Encode(map[string]any{"replicas": []model.ImageReplica{{ID: "rep_test", Digest: digest, Status: "present", LastVerifiedAt: &now}}})
		}
	}))
	defer srv.Close()
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", srv.URL, "--token", "test", "--json", "image", "verify", "img_test"}, &out, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	var result imageVerification
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Verified || !result.InventoryReady || result.EvidenceKind != "inventory" {
		t.Fatal(result)
	}
	for _, call := range calls {
		if !strings.HasPrefix(call, "GET ") {
			t.Fatalf("inventory check mutated state: %s", call)
		}
	}
}
func TestImageReplicaFreshnessRequiresMatchingDigestAndTime(t *testing.T) {
	now := time.Now().UTC()
	fresh := now.Add(-time.Minute)
	old := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	expired := now.Add(-time.Second)
	base := model.ImageReplica{Status: "present", Digest: "sha256:known", LastVerifiedAt: &fresh}
	if !imageReplicaFresh(base, base.Digest, now, 15*time.Minute) {
		t.Fatal("fresh evidence rejected")
	}
	for _, mutate := range []func(*model.ImageReplica){func(r *model.ImageReplica) { r.LastVerifiedAt = nil }, func(r *model.ImageReplica) { r.LastVerifiedAt = &old }, func(r *model.ImageReplica) { r.LastVerifiedAt = &future }, func(r *model.ImageReplica) { r.Digest = "other" }, func(r *model.ImageReplica) { r.LeaseExpiresAt = &expired }, func(r *model.ImageReplica) { r.Status = "missing" }} {
		r := base
		mutate(&r)
		if imageReplicaFresh(r, base.Digest, now, 15*time.Minute) {
			t.Fatalf("accepted invalid replica %+v", r)
		}
	}
}
func TestImagePinAndReplicateUseAuthorizedImmutableIdentity(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, `{"image":{"id":"img_test","app_id":"app_test","canonical_digest":"sha256:aaaa"}}`)
			return
		}
		calls++
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if strings.HasSuffix(r.URL.Path, "pins") {
			if request["reason"] != "user_pin" || request["app_id"] != "app_test" {
				t.Error(request)
			}
			fmt.Fprint(w, `{"pin":{"id":"pin_test"}}`)
		} else {
			if request["image_id"] != "img_test" || request["target_cluster_node_name"] != "worker-test" {
				t.Error(request)
			}
			fmt.Fprint(w, `{"task":{"id":"task_test","status":"pending"}}`)
		}
	}))
	defer srv.Close()
	for _, args := range [][]string{{"image", "pin", "img_test"}, {"image", "replicate", "img_test", "--node", "worker-test"}} {
		var out, stderr bytes.Buffer
		err := runWithStreams(append([]string{"--base-url", srv.URL, "--token", "test", "--json"}, args...), &out, &stderr)
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestActiveImageProbeRequiresRealTaskAndFreshMatchingNode(t *testing.T) {
	now := time.Now().UTC()
	verifiedAt := now.Add(time.Millisecond)
	digest := "sha256:" + strings.Repeat("b", 64)
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/images/img_probe":
			json.NewEncoder(w).Encode(map[string]any{"image": model.Image{ID: "img_probe", CanonicalDigest: digest}})
		case r.URL.Path == "/v1/node-update-tasks":
			task := model.NodeUpdateTask{ID: "nodetask_probe", NodeUpdaterID: "nodeupdater_probe", Type: model.NodeUpdateTaskTypeVerifyImageCache, Status: model.NodeUpdateTaskStatusCompleted, CreatedAt: now}
			if r.Method == "POST" {
				posts++
				var req nodeUpdateTaskCreateRequest
				json.NewDecoder(r.Body).Decode(&req)
				if req.Type != model.NodeUpdateTaskTypeVerifyImageCache || req.Payload["digest"] != digest || req.ClusterNodeName != "node-test" {
					t.Error(req)
				}
				json.NewEncoder(w).Encode(map[string]any{"task": task})
			} else {
				json.NewEncoder(w).Encode(map[string]any{"tasks": []model.NodeUpdateTask{task}})
			}
		case r.URL.Path == "/v1/images/img_probe/replicas":
			json.NewEncoder(w).Encode(map[string]any{"replicas": []model.ImageReplica{{ImageID: "img_probe", ClusterNodeName: "node-test", Status: "present", Digest: digest, LastVerifiedAt: &verifiedAt}}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	var out, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", server.URL, "--token", "test", "--json", "image", "verify", "img_probe", "--probe", "--node", "node-test"}, &out, &stderr)
	if err != nil {
		t.Fatal(err, out.String())
	}
	var result imageVerification
	if json.Unmarshal(out.Bytes(), &result) != nil || !result.Verified || posts != 1 {
		t.Fatal(out.String(), posts)
	}
}
