package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestReleaseOriginsRequireExactReadyTrafficTargets(t *testing.T) {
	for _, scenario := range []string{"canary", "candidate only", "stable only", "candidate unavailable fallback", "candidate proof absent", "stable proof absent", "stable unavailable", "runtime differs", "stale fact", "disabled intent", "blocked policy", "nonrelease origin"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			traffic := model.AppTrafficPolicy{ID: "traffic-a", AppID: "app-a", TenantID: "tenant-a", Mode: model.AppTrafficModeCanary, StableReleaseID: "stable", CandidateReleaseID: "candidate", StableWeight: 99, CandidateWeight: 1}
			switch scenario {
			case "candidate only":
				traffic.StableWeight, traffic.CandidateWeight = 0, 100
			case "stable only":
				traffic.Mode, traffic.StableWeight, traffic.CandidateWeight = model.AppTrafficModeSingle, 100, 0
			}
			policy, err := platformconfig.ProjectPolicySnapshot(platformconfig.PolicySnapshot{Scope: "global", MinimumHealthyEdges: 1, MaxStaleSeconds: 120}, nil, []model.AppTrafficPolicy{traffic}, "policy")
			if err != nil {
				t.Fatal(err)
			}
			route := platformconfig.RouteIntent{Hostname: "app.example.test", OriginRef: "origin", AppID: "app-a", TenantID: "tenant-a", Enabled: true, RuntimeID: model.DefaultManagedRuntimeID, UpstreamURL: "http://canonical.example:8080"}
			p := platformIntentProjectionResponse{Intent: platformconfig.PlatformIntent{Routes: []platformconfig.RouteIntent{route}}, Policy: policy, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, Origins: []platformconfig.OriginObservation{{Ref: "origin", ObservedAt: now, RuntimeID: route.RuntimeID, Status: model.EdgeRouteStatusUnavailable, StatusReason: "runtime invariant violation: current_image_mismatch"}}}}
			proofs := []releaseRuntimeReadiness{{ReleaseID: "stable", Ready: true}, {ReleaseID: "candidate", Ready: true}}
			for _, id := range []string{"stable", "candidate"} {
				p.RuntimeSnapshot.Releases = append(p.RuntimeSnapshot.Releases, platformconfig.ReleaseObservation{ID: id, AppID: route.AppID, TenantID: route.TenantID, RuntimeID: route.RuntimeID, ObservedAt: now, Status: model.EdgeRouteStatusActive, UpstreamURL: "http://" + id + ".example:8080", DeploymentGeneration: "sha256:" + strings.Repeat("a", 64)})
			}
			wantReady, wantErr := true, false
			switch scenario {
			case "candidate unavailable fallback":
				p.RuntimeSnapshot.Releases[1].Status = model.EdgeRouteStatusUnavailable
				proofs[1].Ready = false
			case "candidate proof absent":
				proofs = proofs[:1]
				wantReady = false
			case "stable proof absent":
				proofs = proofs[1:]
				wantReady = false
			case "stable unavailable":
				p.RuntimeSnapshot.Releases[0].Status = model.EdgeRouteStatusUnavailable
				proofs[0].Ready = false
				wantReady, wantErr = false, true
			case "runtime differs":
				p.RuntimeSnapshot.Releases[1].RuntimeID = "other"
				wantReady = false
			case "stale fact":
				p.RuntimeSnapshot.Releases[1].ObservedAt = now.Add(-time.Hour)
				wantReady, wantErr = false, true
			case "disabled intent":
				p.Intent.Routes[0].Enabled = false
				wantReady = false
			case "blocked policy":
				p.Intent.Routes[0].RoutePolicy = model.EdgeRoutePolicyRouteAOnly
				wantReady = false
			case "nonrelease origin":
				p.Intent.Routes[0].OriginRef = "other"
				wantReady = false
			}
			beforeIntent, _ := json.Marshal(p.Intent)
			beforePolicy, _ := json.Marshal(p.Policy)
			err = projectReadyReleaseOrigins(&p, proofs)
			if (err != nil) != wantErr {
				t.Fatalf("error=%v wantErr=%v", err, wantErr)
			}
			if got := p.RuntimeSnapshot.Origins[0].Status == model.EdgeRouteStatusActive; got != wantReady {
				t.Fatalf("ready=%v want=%v origin=%+v", got, wantReady, p.RuntimeSnapshot.Origins[0])
			}
			afterIntent, _ := json.Marshal(p.Intent)
			afterPolicy, _ := json.Marshal(p.Policy)
			if string(beforeIntent) != string(afterIntent) || string(beforePolicy) != string(afterPolicy) {
				t.Fatal("runtime capture changed configuration")
			}
		})
	}
}
