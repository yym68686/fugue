package diagnosticprobe

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/livediagnostics"
)

func TestCPUProfileEvidenceDoesNotTreatLostOrUnresolvedSamplesAsComplete(t *testing.T) {
	for _, field := range []string{"lost_samples", "unresolved_user_samples", "unresolved_kernel_samples"} {
		v, err := profileEvidence([]byte(`{"schema":"fugue.diagnostic.cpu_profile.v1","samples":30,"raw_script":"duplicate","` + field + `":1}`))
		if err != nil {
			t.Fatal(err)
		}
		p, ok := v.(partialValue)
		if !ok || len(p.Gaps) == 0 {
			t.Fatalf("%s treated as complete: %+v", field, v)
		}
		if _, ok := p.Value.(map[string]any)["raw_script"]; ok {
			t.Fatal("unbounded duplicate export")
		}
	}
	v, err := profileEvidence([]byte(`{"schema":"fugue.diagnostic.cpu_profile.v1","samples":42,"target_pid":9007199254740993}`))
	if err != nil || v.(map[string]any)["target_pid"] != json.Number("9007199254740993") {
		t.Fatalf("invalid complete evidence: %v %v", v, err)
	}
	if _, err := profileEvidence([]byte(`{"schema":"other"}`)); err == nil {
		t.Fatal("accepted wrong schema")
	}
}

func TestCPUProfileRequiresBoundedSingleCaptureAndExactTarget(t *testing.T) {
	for _, req := range []livediagnostics.ProbeRequest{{DurationSeconds: 60}, {DurationSeconds: 10, Target: livediagnostics.Target{ProcessName: "k3s"}}} {
		if _, err := processCPUProfile(context.Background(), req, Collector{CaptureSeconds: 20, IntervalSeconds: 120}); err == nil {
			t.Fatal("accepted unsafe capture")
		}
	}
}

func TestCPUProfileReportsStructuredStackCoverageIndependently(t *testing.T) {
	for _, paths := range []string{
		`{"observed_samples":29,"truncated":false}`,
		`{"observed_samples":30,"omitted_samples":1,"truncated":true}`,
	} {
		v, err := profileEvidence([]byte(`{"schema":"fugue.diagnostic.cpu_profile.v1","samples":30,"stack_samples":30,"stack_paths":` + paths + `}`))
		if err != nil {
			t.Fatal(err)
		}
		p, ok := v.(partialValue)
		if !ok || !p.Truncated || len(p.Gaps) == 0 {
			t.Fatalf("incomplete path table reported complete: %+v", v)
		}
	}
}

func TestDiagnosticCommandBoundsOutputAndCancelsChildProcessGroup(t *testing.T) {
	raw, truncated, err := diagnosticCommand(context.Background(), 3, "sh", "-c", "printf abcdef")
	if string(raw) != "abc" || !truncated || err != nil {
		t.Fatalf("bad output bound: %q %v %v", raw, truncated, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err = diagnosticCommand(ctx, 100, "sh", "-c", "sleep 20 & wait")
	if err == nil || time.Since(start) > 3*time.Second {
		t.Fatal("descendant outlived capture cancellation")
	}
	_, stderr, _, err := diagnosticCommandEvidence(context.Background(), 100, "sh", "-c", "printf 'token=fixture-secret' >&2; exit 2")
	if err == nil || strings.Contains(stderr, "fixture-secret") || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatal("command failure lost its exit status or leaked credentials")
	}
}

func TestJournalEvidenceHasTimestampRedactionAndPartialCoverage(t *testing.T) {
	data := `{"__REALTIME_TIMESTAMP":"1789960000000000","_BOOT_ID":"boot-test","MESSAGE":"slow request token=fixture-secret"}` + "\n"
	value, err := journalEvidence([]byte(data), []string{"slow request"})
	if err != nil || value["entries"] != 1 || value["pattern_counts"].(map[string]int)["slow request"] != 1 {
		t.Fatalf("bad journal %v %v", value, err)
	}
	raw, _ := json.Marshal(value)
	if strings.Contains(string(raw), "fixture-secret") {
		t.Fatal("journal leaked token")
	}
	if _, err := journalEvidence([]byte(data+`{"MESSAGE":`), nil); err == nil {
		t.Fatal("partial journal reported complete")
	}
	if _, err := hostJournal(context.Background(), livediagnostics.ProbeRequest{}, Collector{Unit: "invalid;command", SinceSeconds: 900}); err == nil {
		t.Fatal("accepted unbounded host source")
	}
}

func TestJournalRetainsRarePatternsBehindFrequentMessages(t *testing.T) {
	line := `{"__REALTIME_TIMESTAMP":"1789960000000000","MESSAGE":"frequent warning"}` + "\n"
	rare := `{"__REALTIME_TIMESTAMP":"1789960000000001","MESSAGE":"slow storage token=fixture-secret"}` + "\n"
	v, err := journalEvidence([]byte(strings.Repeat(line, 80)+rare), []string{"warning", "slow storage"})
	if err != nil {
		t.Fatal(err)
	}
	groups := v["examples_by_pattern"].(map[string][]any)
	if len(groups["warning"]) != 8 || len(groups["slow storage"]) != 1 {
		t.Fatalf("rare evidence starved: %v", groups)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "fixture-secret") {
		t.Fatal("pattern-specific evidence bypassed redaction")
	}
}
