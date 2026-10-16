package api

import (
	"fmt"
	"sync"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Preserve the prior computation as a differential oracle for the optimization.
func uncachedQualityContext(projection platformIntentProjectionResponse, job dynamicQualityJob) (string, error) {
	var record platformconfig.DNSIntent
	var owners []platformconfig.CompiledRoute
	for _, candidate := range projection.Intent.DNS {
		if candidate.Hostname != job.Route.Hostname || platformconfig.DNSPlacementOptions(candidate) == nil {
			continue
		}
		record = candidate
		var err error
		owners, _, err = placementRecordRoutes(projection, candidate)
		if err != nil {
			return "", err
		}
		break
	}
	var candidates []platformconfig.DNSSelectionCandidate
	for _, fact := range projection.RuntimeSnapshot.DNSSelections {
		if fact.NodeID == job.NodeID && fact.Hostname == job.Route.Hostname && fact.Type == "A" {
			candidates = fact.Candidates
		}
	}
	return platformconfig.Digest(struct {
		Record     platformconfig.DNSIntent
		Owners     []platformconfig.CompiledRoute
		Policy     model.PhysicalEdgeQualityPolicy
		Candidates []platformconfig.DNSSelectionCandidate
	}{record, owners, job.Route.Policy, candidates})
}

func qualityContextFixture() (platformIntentProjectionResponse, []dynamicQualityJob) {
	projection, _, _, _ := directQueryFixture()
	jobs := []dynamicQualityJob{}
	for i := 0; i < 4; i++ {
		node := fmt.Sprintf("dns-%d", i)
		jobs = append(jobs, dynamicQualityJob{NodeID: node, Key: node + "\x00app.example.test", Route: platformconfig.PhysicalQualityRoute{Hostname: "app.example.test", Policy: edgequality.DefaultNetworkPolicy()}})
		projection.RuntimeSnapshot.DNSSelections = append(projection.RuntimeSnapshot.DNSSelections, platformconfig.DNSSelectionObservation{NodeID: node, Hostname: "app.example.test", Type: "A", Candidates: []platformconfig.DNSSelectionCandidate{{EdgeID: fmt.Sprintf("edge-%d", i), IP: fmt.Sprintf("8.8.8.%d", i+1)}}})
	}
	return projection, jobs
}

func TestQualityContextReaderPreservesDigestAndRejections(t *testing.T) {
	for _, scenario := range []string{"ordinary", "missing record", "missing candidates", "duplicate declarations", "alias", "invalid ownership", "invalid policy", "invalid origin", "different job policy"} {
		t.Run(scenario, func(t *testing.T) {
			projection, jobs := qualityContextFixture()
			switch scenario {
			case "missing record":
				projection.Intent.DNS = nil
			case "missing candidates":
				projection.RuntimeSnapshot.DNSSelections = nil
			case "duplicate declarations":
				record := projection.Intent.DNS[0]
				record.TTL++
				projection.Intent.DNS = append(projection.Intent.DNS, record)
				fact := projection.RuntimeSnapshot.DNSSelections[0]
				fact.Candidates = nil
				projection.RuntimeSnapshot.DNSSelections = append(projection.RuntimeSnapshot.DNSSelections, fact)
				fact.Type = "AAAA"
				fact.Candidates = []platformconfig.DNSSelectionCandidate{{EdgeID: "ignored"}}
				projection.RuntimeSnapshot.DNSSelections = append(projection.RuntimeSnapshot.DNSSelections, fact)
			case "alias":
				projection.Intent.DNS[0].Hostname = "alias.example.test"
				for i := range jobs {
					jobs[i].Route.Hostname = "alias.example.test"
					projection.RuntimeSnapshot.DNSSelections[i].Hostname = "alias.example.test"
				}
			case "invalid ownership":
				projection.Intent.DNS[0].TenantID = "foreign-tenant"
			case "invalid policy":
				projection.Policy.SchemaVersion = "unsupported"
			case "invalid origin":
				projection.RuntimeSnapshot.Origins = nil
			case "different job policy":
				jobs[1].Route.Policy.EvidenceMaxAgeSeconds++
			}
			read := newDynamicQualityContextReader(projection, jobs)
			var wg sync.WaitGroup
			for _, job := range jobs {
				want, wantErr := uncachedQualityContext(projection, job)
				if (scenario == "invalid ownership" || scenario == "invalid policy" || scenario == "invalid origin") && wantErr == nil {
					t.Fatalf("fixture does not exercise rejection: %s", scenario)
				}
				for i := 0; i < 3; i++ {
					wg.Add(1)
					go func(job dynamicQualityJob, want string, wantErr error) {
						defer wg.Done()
						got, err := read(job)
						if got != want || fmt.Sprint(err) != fmt.Sprint(wantErr) {
							t.Errorf("context changed: %s %v; want %s %v", got, err, want, wantErr)
						}
					}(job, want, wantErr)
				}
			}
			wg.Wait()
		})
	}
}

func TestQualityContextReaderRebuildsAtProjectionBoundary(t *testing.T) {
	projection, jobs := qualityContextFixture()
	first, err := newDynamicQualityContextReader(projection, jobs)(jobs[0])
	if err != nil {
		t.Fatal(err)
	}
	projection.RuntimeSnapshot.DNSSelections[0].Candidates[0].EdgeID = "replacement-edge"
	second, err := newDynamicQualityContextReader(projection, jobs)(jobs[0])
	if err != nil || first == second {
		t.Fatal("changed candidate reused an earlier context", err)
	}
	projection.Intent.DNS[0].TTL++
	third, err := newDynamicQualityContextReader(projection, jobs)(jobs[0])
	if err != nil || second == third {
		t.Fatal("changed intent reused an earlier context", err)
	}
	projection.Policy.SchemaVersion = "unsupported"
	if _, err := newDynamicQualityContextReader(projection, jobs)(jobs[0]); err == nil {
		t.Fatal("invalid new policy reused a validated route")
	}
}

func TestQualityContextReaderDoesNotMutateProjection(t *testing.T) {
	projection, jobs := qualityContextFixture()
	before, err := platformconfig.Digest(projection)
	if err != nil {
		t.Fatal(err)
	}
	read := newDynamicQualityContextReader(projection, jobs)
	values := []string{}
	for _, job := range jobs {
		value, err := read(job)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	after, err := platformconfig.Digest(projection)
	if err != nil || before != after {
		t.Fatal("context read changed its input", err)
	}
	again := newDynamicQualityContextReader(projection, jobs)
	for i, job := range jobs {
		value, _ := again(job)
		if value != values[i] {
			t.Fatal("capture depends on prior reader")
		}
	}
}

func BenchmarkQualityContextConsumers(b *testing.B) {
	projection, jobs := qualityContextFixture()
	for i := 0; i < 64; i++ {
		projection.Policy.TrafficRolloutCohorts = append(projection.Policy.TrafficRolloutCohorts, platformconfig.TrafficRolloutCohort{ID: fmt.Sprintf("cohort-%d", i), EdgeGroupIDs: []string{"edge-group-a"}})
	}
	for _, count := range []int{1, 2, 4} {
		jobs := jobs[:count]
		for _, mode := range []string{"uncached", "capture_reader"} {
			b.Run(fmt.Sprintf("consumers_%d/%s", count, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					read := func(job dynamicQualityJob) (string, error) { return uncachedQualityContext(projection, job) }
					if mode == "capture_reader" {
						read = newDynamicQualityContextReader(projection, jobs)
					}
					for _, job := range jobs {
						if _, err := read(job); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
