package dnsserver

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/trafficoverride"
	miekgdns "github.com/miekg/dns"
)

func TestTrafficOverrideOverlayIsInertByDefault(t *testing.T) {
	service := NewService(config.DNSConfig{Zone: "example.com"}, nil)
	service.overrides["app.example.com"] = model.TrafficOverride{
		Hostname:  "app.example.com",
		Answers:   []string{"192.0.2.10"},
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	if got := service.overlayRecords("app.example.com", miekgdns.TypeA); len(got) != 0 {
		t.Fatalf("disabled overlay changed DNS behavior: %+v", got)
	}
}

func TestTrafficOverrideFeedVerificationRejectsTampering(t *testing.T) {
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
		ExpiresAt:          now.Add(time.Hour),
		Reason:             "test signed DNS overlay",
		Operator:           "api-key/admin",
		SignedAt:           now,
		CreatedAt:          now,
		UpdatedAt:          now,
	}, base64.RawStdEncoding.EncodeToString(privateKey), "traffic-override-key-1")
	if err != nil {
		t.Fatal(err)
	}
	feed := model.TrafficOverrideFeed{
		Schema:      model.TrafficOverrideFeedSchemaV1,
		Generation:  1,
		GeneratedAt: now,
		Overrides:   []model.TrafficOverride{override},
		SigningKey: model.TrafficOverrideSigningKeyStatus{
			Schema:           model.TrafficOverrideSigningSchemaV1,
			Generation:       1,
			CurrentKeyID:     "traffic-override-key-1",
			CurrentPublicKey: base64.RawStdEncoding.EncodeToString(publicKey),
		},
	}
	verified, err := validateTrafficOverrideFeed(feed, now, "example.com")
	if err != nil || verified["app.example.com"].Answers[0] != "192.0.2.10" {
		t.Fatalf("valid override feed was rejected: verified=%+v err=%v", verified, err)
	}
	feed.Overrides[0].Answers = []string{"192.0.2.11"}
	if _, err := validateTrafficOverrideFeed(feed, now, "example.com"); err == nil {
		t.Fatal("tampered traffic override feed was accepted")
	}
}
