package dnsserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/trafficoverride"
	miekgdns "github.com/miekg/dns"
)

func TestTrafficOverrideConsumerAtomicallyCachesAndRetainsLKGOnPullFailure(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	override, err := trafficoverride.Sign(model.TrafficOverride{
		Hostname:           "app.example.com",
		Generation:         1,
		State:              model.TrafficOverrideStateStaged,
		Answers:            []string{"192.0.2.10"},
		RequiredHostRoutes: []string{"app.example.com"},
		RouteGeneration:    "route-generation-1",
		RouteDigest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ActivateAt:         now.Add(time.Minute),
		ExpiresAt:          now.Add(time.Hour),
		Reason:             "test atomic DNS overlay",
		Operator:           "test/operator",
		SignedAt:           now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}, base64.RawStdEncoding.EncodeToString(privateKey), "test-key-1")
	if err != nil {
		t.Fatal(err)
	}
	feed := trafficOverrideFeedResponse{Feed: trafficOverrideFeed{
		Schema:      trafficOverrideFeedSchema,
		Generation:  1,
		GeneratedAt: now,
		Overrides:   []model.TrafficOverride{override},
		SigningKey: model.TrafficOverrideSigningKeyStatus{
			Schema:           model.TrafficOverrideSigningSchemaV1,
			Generation:       1,
			CurrentKeyID:     "test-key-1",
			CurrentPublicKey: base64.RawStdEncoding.EncodeToString(publicKey),
		},
	}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != trafficOverrideFeedPath {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(feed)
	}))
	defer server.Close()

	service := NewService(config.DNSConfig{APIURL: server.URL, EdgeToken: "edge-token", Zone: "example.com"}, nil)
	service.override = trafficOverrideSettings{enabled: true, interval: 5 * time.Second, path: filepath.Join(t.TempDir(), "overlay.json")}
	if err := service.syncTrafficOverrides(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := service.overlayRecords("app.example.com", miekgdns.TypeA); len(got) != 0 {
		t.Fatalf("future prepared override activated before activate_at: %+v", got)
	}
	service.activatePrepared(now.Add(2 * time.Minute))
	if got := service.overlayRecords("app.example.com", miekgdns.TypeA); len(got) != 1 || got[0].Values[0] != "192.0.2.10" {
		t.Fatalf("expected cached overlay answer, got %+v", got)
	}
	if _, err := os.Stat(service.override.path); err != nil {
		t.Fatalf("expected atomically activated cache: %v", err)
	}
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	})
	if err := service.syncTrafficOverrides(context.Background()); err == nil {
		t.Fatal("expected failed feed pull")
	}
	if got := service.overlayRecords("app.example.com", miekgdns.TypeA); len(got) != 1 || got[0].Values[0] != "192.0.2.10" {
		t.Fatalf("failed pull discarded positive LKG overlay: %+v", got)
	}
}

func TestTrafficOverrideOverlayIsDisabledByDefault(t *testing.T) {
	t.Setenv("FUGUE_DNS_TRAFFIC_OVERRIDE_ENABLED", "")
	service := NewService(config.DNSConfig{Zone: "example.com"}, nil)
	service.overrideMu.Lock()
	service.overrides["app.example.com"] = model.TrafficOverride{Hostname: "app.example.com", Answers: []string{"192.0.2.10"}, ExpiresAt: time.Now().UTC().Add(time.Hour)}
	service.overrideMu.Unlock()
	if got := service.overlayRecords("app.example.com", miekgdns.TypeA); len(got) != 0 {
		t.Fatalf("disabled overlay changed DNS behavior: %+v", got)
	}
}
