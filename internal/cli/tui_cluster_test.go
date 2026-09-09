package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/tui"
)

func TestTUINodePreviewAndPeakAttribution(t *testing.T) {
	now := time.Now()
	older := now.Add(-time.Minute)
	low, high := 12.0, 91.0
	capacity, used := int64(500<<30), int64(60<<30)
	a := model.ClusterNode{Name: "worker-a", ObservedAt: &now, CPU: &model.ClusterNodeCPUStats{UsagePercent: &high}, EphemeralStorage: &model.ClusterNodeStorageStats{UsagePercent: &low, CapacityBytes: &capacity, UsedBytes: &used}}
	b := model.ClusterNode{Name: "worker-b", ObservedAt: &older, CPU: &model.ClusterNodeCPUStats{UsagePercent: &low}, EphemeralStorage: &model.ClusterNodeStorageStats{UsagePercent: &high}}
	detail := tuiNodePreview(a)
	if *detail.Series[2].Points[0].Value != 12 || detail.Capacity[1].Value != "60.0 GiB / 500.0 GiB" {
		t.Fatalf("wrong node preview: %+v", detail)
	}
	if detail.Capacity[0].Value != "-- / --" {
		t.Fatal("missing capacity became zero")
	}
	s := tui.Snapshot{}
	appendTUIClusterSeries(&s, []model.ClusterNode{a, b})
	if s.Series[0].Subject != "worker-a" || !s.Series[0].Points[0].At.Equal(now) {
		t.Fatal("CPU peak lost its owning node/time")
	}
	if s.Series[2].Subject != "worker-b" || !s.Series[2].Points[0].At.Equal(older) {
		t.Fatal("disk peak lost its owning node/time")
	}
}

func TestTUIEntrypointsExposeSharedFlags(t *testing.T) {
	for _, path := range [][]string{{"app", "top"}, {"project", "top"}, {"admin", "cluster", "top"}, {"console"}} {
		root := newCLI(&bytes.Buffer{}, &bytes.Buffer{}).newRootCommand()
		cmd, _, err := root.Find(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"mouse", "theme", "graph", "window", "screen-mode", "interval", "once", "plain"} {
			if cmd.Flags().Lookup(name) == nil {
				t.Fatalf("%v lacks --%s", path, name)
			}
		}
	}
}
func TestAdminTUIAppearanceParsingAndSnapshotCompatibility(t *testing.T) {
	server := newAdminClusterTopServer(t)
	defer server.Close()
	out, stderr, err := runMonitorCommand(server.URL, "admin", "cluster", "top", "--mouse=false", "--theme", "carbon", "--window", "5m", "--once")
	if err != nil {
		t.Fatalf("example flags: %v %s", err, stderr)
	}
	if strings.Contains(out, "\x1b") || !strings.Contains(out, "Admin cluster top") {
		t.Fatal("snapshot contract changed")
	}
	_, _, err = runMonitorCommand(server.URL, "admin", "cluster", "top", "--theme", "invalid", "--once")
	if err == nil {
		t.Fatal("invalid theme accepted")
	}
}
func TestClusterComponentScopeAndPodDrilldown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/auth/context":
			json.NewEncoder(w).Encode(map[string]any{"principal": map[string]any{"platform_admin": true}})
		case "/v1/cluster/control-plane":
			w.Write([]byte(`{"control_plane":{"status":"ready","components":[{"component":"api","deployment_name":"control-api","ready_replicas":1,"desired_replicas":1,"observed_pods":[{"name":"api-pod-a","phase":"Running","ready":true,"node_name":"node-a","image_tag":"build-a"}]}],"deploy_workflow":{"workflow":"ci.yml","head_sha":"abcdef0123456789","status":"completed","conclusion":"success"}}}`))
		default:
			t.Errorf("scope fetched unrelated endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	p := &tuiProvider{client: &Client{baseURL: server.URL, token: "test", httpClient: server.Client()}}
	s, err := p.Load(context.Background(), tui.Request{Target: tui.Target{Kind: "component", ID: "control-api"}, Section: "overview"})
	if err != nil {
		t.Fatal(err)
	}
	var pod *tui.Target
	for _, table := range s.Tables {
		if table.ID == "component-pods" {
			pod = table.Rows[0].Target
		}
	}
	if pod == nil || pod.Kind != "component-pod" {
		t.Fatal("missing pod route")
	}
	detail, err := p.Load(context.Background(), tui.Request{Target: *pod, Section: "overview"})
	if err != nil || detail.Title != "api-pod-a" {
		t.Fatalf("pod detail: %+v %v", detail, err)
	}
}
