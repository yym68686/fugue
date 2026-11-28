package api

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/hosteddns"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestFlattenCaptureBindsFreshQueryWithoutReusingLegacyCache(t *testing.T) {
	now := time.Now().UTC()
	result := platformIntentProjectionResponse{CapturedAt: now, Intent: platformconfig.PlatformIntent{Generation: "intent", DNS: []platformconfig.DNSIntent{{Hostname: "alias.example", TenantID: "tenant", Type: "ALIAS", Values: []string{"target.example"}, TTL: 60, Flatten: &platformconfig.DNSFlattenIntent{Mode: "always", Target: "target.example", IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "target", FallbackPolicy: "fail_closed"}}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy", MaxStaleSeconds: 300}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}, Issues: []platformProjectionIssue{{Code: "dns_flatten_observation_not_captured", Hostname: "alias.example"}}}
	intent := platformconfig.NormalizePlatformIntent(result.Intent)
	captureDNSFlattenFacts(context.Background(), &result, func(ctx context.Context, target string, record model.DNSRecord) hosteddns.Result {
		if target != "target.example" || record.FlattenTTLPolicy != "target" || record.FlattenFallbackPolicy != "fail_closed" {
			t.Fatal("resolver received wrong configuration")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("unbounded resolver")
		}
		return hosteddns.Result{A: []string{"93.184.216.34"}, TTL: 120}
	})
	if len(result.RuntimeSnapshot.DNSFlatten) != 1 || len(result.Issues) != 0 {
		t.Fatalf("capture failed: %+v", result)
	}
	fact := result.RuntimeSnapshot.DNSFlatten[0]
	digest, _ := platformconfig.DNSFlattenInputDigest(result.Intent.DNS[0])
	if fact.InputDigest != digest || fact.TargetTTL != 120 || fact.ObservedAt.Before(now) || !fact.ObservedAt.Equal(fact.CheckedAt) {
		t.Fatal("missing query provenance")
	}
	if !reflect.DeepEqual(intent, platformconfig.NormalizePlatformIntent(result.Intent)) {
		t.Fatal("query changed intent")
	}
	result.RuntimeSnapshot.DNSFlatten = nil
	captureDNSFlattenFacts(context.Background(), &result, func(context.Context, string, model.DNSRecord) hosteddns.Result {
		return hosteddns.Result{Err: errors.New("resolution failed")}
	})
	fact = result.RuntimeSnapshot.DNSFlatten[0]
	if !fact.ObservedAt.IsZero() || len(fact.A) != 0 || fact.Status != "error" || len(result.Issues) == 0 {
		t.Fatal("failed query fabricated positive evidence")
	}
}
