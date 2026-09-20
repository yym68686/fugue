package livediagnostics

import (
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProbeReportCannotSubstituteIdentityOrHidePartialEvidence(t *testing.T) {
	now := time.Now()
	job := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "diagnostic-test", Labels: map[string]string{TargetTypeLabel: string(TargetNode)}, Annotations: map[string]string{ProbeRefAnnotation: "observer-test", ProbeDigestAnnotation: "probe-digest", CatalogDigestAnnotation: "catalog-digest", TargetNodeAnnotation: "node-test"}}}
	report := ProbeReport{Schema: "fugue.diagnostic.probe_report.v1", SessionID: job.Name, ProbeID: "observer-test", ProbeDigest: "probe-digest", CatalogDigest: "catalog-digest", Target: Target{Type: TargetNode, Node: "node-test"}, StartedAt: now, FinishedAt: now.Add(time.Second), Quality: EvidenceQuality{Status: "complete"}, Evidence: []Evidence{{Name: "sample", Status: "complete"}}}
	if err := ValidateProbeReport(report, job); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ProbeReport){func(r *ProbeReport) { r.ProbeDigest = "different" }, func(r *ProbeReport) { r.Target.Node = "different" }, func(r *ProbeReport) { r.Quality.Truncated = true }, func(r *ProbeReport) { r.Evidence = nil }, func(r *ProbeReport) { r.Quality.Gaps = []string{"samples missing"} }} {
		changed := report
		mutate(&changed)
		if err := ValidateProbeReport(changed, job); err == nil {
			t.Fatal("accepted substituted or incomplete evidence as complete")
		}
	}
	report.Quality.Status = "degraded"
	report.Quality.Gaps = []string{"source unavailable"}
	report.Evidence = []Evidence{{Name: "sample", Status: "unavailable"}}
	if err := ValidateProbeReport(report, job); err != nil {
		t.Fatal("truthful partial report was rejected", err)
	}
}
