package dnsserver

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestQueryFailureWitnessReplaysMissingRecoveryWithoutAuthorizingServing(t *testing.T) {
	for _, mode := range []string{"no-history", "expired", "failed-path"} {
		t.Run(mode, func(t *testing.T) {
			view, plan, policy, facts, now := queryExecutionFixture()
			view.Records = view.Records[:1]
			view.Records[0].AnswerPolicy.Availability = availabilityTestPolicy()
			for i := range facts {
				at := facts[i].Proof.CheckedAt
				facts[i].ProbeHistory = []time.Time{at.Add(-10 * time.Second), at}
				switch mode {
				case "no-history":
					facts[i].ProbeHistory = nil
				case "expired":
					facts[i].Proof.ValidUntil = now.Add(-time.Second)
				case "failed-path":
					facts[i].Ready = false
					facts[i].Reason = "probe_unavailable"
				}
			}
			before, _ := json.Marshal(view)
			records, err := materializeDNSQueries(view, &plan, &policy, facts, now)
			if err != nil || len(records) != 1 || len(records[0].Candidates) != 0 {
				t.Fatal("invalid failure fixture", err)
			}
			_, cause := executeDNSQueryRecord(records[0], dnsGeoHint{}, now)
			if cause == nil {
				t.Fatal("missing readiness accepted")
			}
			s := &Service{Config: config.DNSConfig{CachePath: filepath.Join(t.TempDir(), "dns")}, Logger: log.New(io.Discard, "", 0)}
			s.retainDNSQueryFailure(dnsPlatformCandidate{Artifact: model.PlatformArtifact{ID: "artifact", ContentHash: "digest"}}, model.PlatformConsumerAssignment{ExpectedConsumerSetID: "expected", FencingToken: 4}, view, records[0], &plan, &policy, facts, now, dnsGeoHint{}, cause)
			raw, err := os.ReadFile(s.Config.CachePath + ".query-failure.json")
			if err != nil {
				t.Fatal(err)
			}
			if matched, err := ReplayDNSQueryFailure(raw); err != nil || !matched {
				t.Fatal("exact original failure did not replay", matched, err)
			}
			after, _ := json.Marshal(view)
			if string(before) != string(after) || s.platformServing.Load() != nil {
				t.Fatal("capture altered authorization or serving")
			}
			var changed dnsQueryFailureWitness
			json.Unmarshal(raw, &changed)
			changed.Facts[0].Ready = !changed.Facts[0].Ready
			raw, _ = json.Marshal(changed)
			if _, err := ReplayDNSQueryFailure(raw); err == nil {
				t.Fatal("modified witness accepted")
			}
			if _, err = os.Stat(s.Config.CachePath + ".platform-serving.json"); !os.IsNotExist(err) {
				t.Fatal("diagnostics created checkpoint")
			}
		})
	}
}
