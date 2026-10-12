package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
)

func TestBusinessExplorationCLIIsOfflineAndRejectsUnknownFields(t *testing.T) {
	t.Setenv("FUGUE_CONFIG_DIR", t.TempDir())
	t.Setenv("FUGUE_CLI_UPDATE_CHECK", "off")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()
	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	policy := model.BusinessEdgeExplorationPolicy{Version: model.BusinessEdgeExplorationVersion, IntentID: "intent-a", Mode: "observe", BaselineEdgeID: "edge-a", CandidateEdgeIDs: []string{"edge-a", "edge-b"},
		InitialDNSPublications: map[string]model.BusinessDNSPublicationAnchor{"dns-a": {Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", FencingToken: 1}},
		StartsAt:               now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), ExplorationBasisPoints: 100, MaximumCandidateBasisPoints: 1000, MaximumStepBasisPoints: 50,
		BucketSeconds: 300, RequiredBuckets: 3, MinimumConnectionsPerBucket: 3, CooldownSeconds: 600, EvidenceMaxAgeSeconds: 600,
		MinimumCohortCoverageBasisPoints: 9000, RTTAdvantageRatio: .15, DeliveryAdvantageRatio: .2, MaximumRTTRegressionRatio: .1,
		MaximumDeliveryRegressionRatio: .1, MaximumRetransmissionIncrease: .01, StopRTTMS: 3000, StopRetransmissionRate: .2}
	input := edgequality.BusinessExplorationInput{Policy: policy, CapturedAt: now, Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "dynamic_api",
		Candidates: []edgequality.Candidate{{EdgeID: "edge-a"}, {EdgeID: "edge-b"}}}
	raw, _ := json.Marshal(input)
	path := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	args := []string{"--base-url", server.URL, "--token", "invalid", "admin", "edge", "business-exploration", "evaluate", path}
	if err := runWithStreams(args, &stdout, &stderr); err != nil {
		t.Fatal(err, stderr.String())
	}
	var receipt edgequality.BusinessExplorationReceipt
	if err := json.Unmarshal(stdout.Bytes(), &receipt); err != nil {
		t.Fatal(err, stdout.String())
	}
	if receipt.Decision.ServingAuthority || calls.Load() != 0 {
		t.Fatal("offline evaluation contacted a service or claimed authority")
	}
	if err := os.WriteFile(path, stdout.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	args[len(args)-2] = "replay"
	if err := runWithStreams(args, &stdout, &stderr); err != nil || calls.Load() != 0 || !bytes.Contains(stdout.Bytes(), []byte(`"matched": true`)) {
		t.Fatal(err, stdout.String())
	}
	if err := os.WriteFile(path, []byte(`{"unexpected":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runWithStreams(args, &stdout, &stderr); err == nil {
		t.Fatal("accepted unknown receipt fields")
	}
}
