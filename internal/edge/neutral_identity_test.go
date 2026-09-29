package edge

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestFencedCellWorkerNeverNeedsOrUsesLegacyCoreIdentity(t *testing.T) {
	coreRequests := 0
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		coreRequests++
		t.Errorf("fenced candidate accessed legacy Core: %s", r.URL.Path)
	}))
	defer core.Close()
	newService := func() *Service {
		root := t.TempDir()
		s := NewServiceWithEdgeSources(config.EdgeConfig{
			APIURL: core.URL, EdgeID: "node-a", EdgeGroupID: "cell-a", EdgeSlot: model.EdgeSlotA,
			EdgeInstanceUID: "pod-a", EdgeReleaseEpoch: "release-a", EdgeHeartbeatFenced: true,
			FaultDomainID: "host-a", EdgePoolID: "pool-public", HTTPTimeout: time.Second,
		}, RouteBundleSourceConfig{
			URL:       "http://control-a.fugue-system.svc:8092/v1/edge/routes",
			TokenFile: filepath.Join(root, "reader"), VerifierKeyringFile: filepath.Join(root, "verifier"),
		}, InventoryProducerConfig{
			URL:              "http://control-a.fugue-system.svc:8092/v1/authority/group-inventory-heartbeats",
			AuthorityService: "control-a", IdentityKeyringFile: filepath.Join(root, "inventory"),
			ActivationStateFile: filepath.Join(root, "activation.json"), Interval: 30 * time.Second,
		}, log.New(io.Discard, "", 0))
		s.PlatformTokenFile = filepath.Join(root, "pod-token")
		return s
	}
	s := newService()
	if err := s.validateConfig(); err != nil {
		t.Fatal(err)
	}
	if s.heartbeatEnabled() {
		t.Fatal("candidate enabled the legacy heartbeat")
	}
	if err := s.HeartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.RefreshDesiredState(context.Background()); err != nil {
		t.Fatal(err)
	}
	if coreRequests != 0 {
		t.Fatal("candidate contacted legacy Core")
	}
	for name, mutate := range map[string]func(*Service){
		"unfenced":              func(s *Service) { s.Config.EdgeHeartbeatFenced = false },
		"legacy authority":      func(s *Service) { s.Config.EdgeGroupID = "edge-group-a" },
		"missing authority":     func(s *Service) { s.Config.EdgeGroupID = "" },
		"no route source":       func(s *Service) { s.RouteBundleSource = RouteBundleSourceConfig{} },
		"partial route source":  func(s *Service) { s.RouteBundleSource.VerifierKeyringFile = "" },
		"no inventory":          func(s *Service) { s.InventoryProducer = InventoryProducerConfig{} },
		"partial inventory":     func(s *Service) { s.InventoryProducer.IdentityKeyringFile = "" },
		"missing node":          func(s *Service) { s.Config.EdgeID = "" },
		"no Pod identity":       func(s *Service) { s.PlatformTokenFile = "" },
		"relative Pod identity": func(s *Service) { s.PlatformTokenFile = "token" },
		"dynamic registration":  func(s *Service) { s.Config.WorkloadMode = model.EdgeWorkloadModeDynamic },
		"legacy desired state":  func(s *Service) { s.Config.EdgeDesiredStateURL = core.URL + "/desired-state" },
	} {
		t.Run(name, func(t *testing.T) {
			s := newService()
			mutate(s)
			if err := s.validateConfig(); err == nil {
				t.Fatal("incomplete independent identity accepted")
			}
		})
	}
}
