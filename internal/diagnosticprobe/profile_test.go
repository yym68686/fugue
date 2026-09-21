package diagnosticprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
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

func TestHostJournalSupportsBoundedParameterLookback(t *testing.T) {
	req := livediagnostics.ProbeRequest{Parameters: map[string]string{"since_seconds": "86400"}}
	got, err := configuredSinceSeconds(Collector{SinceSeconds: 900, SinceSecondsParam: "{{param.since_seconds}}"}, req)
	if err != nil || got != 86400 {
		t.Fatalf("lookback parameter: got=%d err=%v", got, err)
	}
	if _, err := configuredSinceSeconds(Collector{SinceSecondsParam: "{{param.since_seconds}}"}, livediagnostics.ProbeRequest{Parameters: map[string]string{"since_seconds": "nope"}}); err == nil {
		t.Fatal("accepted non-numeric lookback")
	}
}

func TestJournalFiltersAreLiteralAndWindowIsBounded(t *testing.T) {
	args, err := journalFilterArguments([]string{"timeout", "error|unsafe"})
	if err != nil || len(args) != 2 || args[0] != `--grep=timeout|error\|unsafe` || args[1] != "--case-sensitive=yes" {
		t.Fatalf("literal filter: %#v %v", args, err)
	}
	if _, err := journalFilterArguments([]string{"bad\nvalue"}); err == nil {
		t.Fatal("accepted newline in journal filter")
	}
	matcher := regexp.MustCompile(strings.TrimPrefix(args[0], "--grep="))
	for _, input := range []string{"Timeout", "unsafe", "error", "error|unsafe", "read timeout"} {
		want := strings.Contains(input, "timeout") || strings.Contains(input, "error|unsafe")
		if matcher.MatchString(input) != want {
			t.Fatalf("filter changed literal/case semantics for %q", input)
		}
	}
	now := time.Date(2026, 9, 21, 12, 0, 0, 123456789, time.UTC)
	c := Collector{SinceSeconds: 86400}
	since, until, err := journalWindow(c, livediagnostics.ProbeRequest{}, now)
	if err != nil || !until.Equal(now.Truncate(time.Microsecond)) || !since.Equal(time.Date(2026, 9, 20, 12, 0, 0, 123457000, time.UTC)) {
		t.Fatalf("window rounding: since=%s until=%s err=%v", since, until, err)
	}
	if _, _, err := journalWindow(Collector{SinceSeconds: 86401}, livediagnostics.ProbeRequest{}, now); err == nil {
		t.Fatal("accepted lookback beyond 24 hours")
	}
	for _, bounds := range [][2]string{
		{"2026-09-20T11:59:59Z", "2026-09-21T11:00:00Z"},
		{"2026-09-21T11:00:00Z", "2026-09-21T13:00:00Z"},
		{"2026-09-21T11:00:00Z", "2026-09-21T10:00:00Z"},
		{"invalid", "2026-09-21T11:00:00Z"},
	} {
		if _, _, err := journalWindow(Collector{SinceSeconds: 900, SinceTime: bounds[0], UntilTime: bounds[1]}, livediagnostics.ProbeRequest{}, now); err == nil {
			t.Fatalf("accepted invalid window %v", bounds)
		}
	}
	since, until, err = journalWindow(Collector{SinceSeconds: 900, SinceTime: "{{param.since}}", UntilTime: "{{param.until}}"}, livediagnostics.ProbeRequest{Parameters: map[string]string{"since": "2026-09-21T10:59:00Z", "until": "2026-09-21T11:01:00Z"}}, now)
	if err != nil || until.Sub(since) != 2*time.Minute || journalTimestamp(since) != "@1789988340.000000" {
		t.Fatalf("explicit incident window: %s %s %v", since, until, err)
	}
}

func TestJournalNoMatchesOnlyAcceptsCleanFilteredExit(t *testing.T) {
	err := exec.Command("sh", "-c", "exit 1").Run()
	if !journalNoMatches(fmt.Errorf("wrapped: %w", err), nil, "", true) {
		t.Fatal("did not recognize clean filtered no-match exit")
	}
	if journalNoMatches(err, nil, "journal failure", true) || journalNoMatches(err, []byte("partial"), "", true) || journalNoMatches(err, nil, "", false) || journalNoMatches(context.Canceled, nil, "", true) {
		t.Fatal("treated journal failure as no matches")
	}
	if journalNoMatches(exec.Command("sh", "-c", "exit 2").Run(), nil, "", true) {
		t.Fatal("treated unrelated failure as no matches")
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
