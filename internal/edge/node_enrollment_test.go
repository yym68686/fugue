package edge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestPrivateEdgeEnrollmentCannotOverrideActivationOrRenewLease(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	activation := filepath.Join(root, "activation.json")
	intent := model.EdgeNodeEnrollment{ID: "enrollment-fixture", NodeID: "node-new", EdgeGroupID: "edge-group-fixture", AuthorityRecordDigest: "sha256:" + strings.Repeat("a", 64), Slot: "a", FenceSequence: 7, BundleVersion: "bundle-fixture.p9.r0", WorkerSourceSHA: strings.Repeat("b", 40), WorkerImageDigest: "sha256:" + strings.Repeat("c", 64), IssuedAt: now, ExpiresAt: now.Add(time.Minute)}
	s := &Service{Config: config.EdgeConfig{EdgeID: intent.NodeID, EdgeGroupID: intent.EdgeGroupID, EdgeSlot: "a"}, edgeEnrollment: &intent, RouteBundleSource: RouteBundleSourceConfig{URL: "http://edge-control-fixture.fugue-system.svc:8092/v1/edge/routes", CandidateURL: "http://edge-control-fixture.fugue-system.svc:8092/v1/edge/candidate-routes", TokenFile: filepath.Join(root, "token"), VerifierKeyringFile: filepath.Join(root, "verifier"), ActivationStateFile: activation}}
	selected, err := s.selectRouteBundleSource()
	if err != nil || selected.enrollment.ID == "" || selected.candidate || selected.expectedPublicationSequence != 9 {
		t.Fatalf("private preparation rejected: %+v %v", selected, err)
	}
	if _, err = os.Stat(activation); !os.IsNotExist(err) {
		t.Fatal("reading enrollment created activation")
	}
	if err = s.validateRouteBundleSourceSelection(selected); err != nil {
		t.Fatal(err)
	}
	second, err := s.selectRouteBundleSource()
	if err != nil || second != selected {
		t.Fatal("same enrollment yielded unstable route selection identity", err)
	}
	if err = os.WriteFile(activation, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.selectRouteBundleSource(); err == nil {
		t.Fatal("corrupt activation fell back to enrollment")
	}
	if err = s.validateRouteBundleSourceSelection(selected); err == nil {
		t.Fatal("activation race accepted")
	}
	if err = os.Remove(activation); err != nil {
		t.Fatal(err)
	}
	s.edgeEnrollment.ExpiresAt = now.Add(-time.Second)
	if _, err = s.selectRouteBundleSource(); err == nil {
		t.Fatal("expired lease accepted")
	}
	if s.edgeEnrollment.ExpiresAt.After(now) {
		t.Fatal("read renewed lease")
	}
	s.edgeEnrollment.ExpiresAt = now.Add(time.Minute)
	s.edgeEnrollment.NodeID = "node-other"
	if _, err = s.selectRouteBundleSource(); err == nil {
		t.Fatal("foreign node enrollment accepted")
	}
}
