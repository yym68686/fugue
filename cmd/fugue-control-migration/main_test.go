package main

import (
	"strings"
	"testing"
)

func TestScanEnumeratesProductionActionsAndSchedule(t *testing.T) {
	files := []string{
		".github/workflows/ci.yml",
		"deploy/environments/production/dns/policy.json",
		"deploy/releases/components.json",
		"deploy/releases/dns-a/intent.json",
		"scripts/reconcile_dns.py",
		"scripts/test_reconcile_dns.py",
		"scripts/observe_dns.sh",
	}
	content := map[string][]byte{
		".github/workflows/ci.yml": []byte("name: ci\non:\n  schedule:\n    - cron: '*/5 * * * *'\njobs:\n  prepush:\n    runs-on: ubuntu-latest\n  production:\n    runs-on: self-hosted\n"),
	}
	items, err := scan(files, func(name string) ([]byte, error) { return content[name], nil })
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, item := range items {
		got = append(got, item.String())
	}
	for _, expected := range []string{
		"workflow-job\t.github/workflows/ci.yml#prepush",
		"workflow-job\t.github/workflows/ci.yml#production",
		"workflow-schedule\t.github/workflows/ci.yml#*/5 * * * *",
		"production-file\tdeploy/environments/production/dns/policy.json",
		"release-lane\tdns-a",
		"script\tscripts/observe_dns.sh",
		"script\tscripts/reconcile_dns.py",
	} {
		if !contains(got, expected) {
			t.Errorf("scan omitted %q from %v", expected, got)
		}
	}
	if contains(got, "script\tscripts/test_reconcile_dns.py") {
		t.Fatal("test script should not be classified as a production writer")
	}
}

func TestInventoryRequiresEveryDispositionAndRejectsStaleRows(t *testing.T) {
	items := []surface{{"script", "scripts/write.py"}, {"workflow-job", ".github/workflows/ci.yml#deploy"}}
	valid := strings.Join([]string{
		"script\tscripts/write.py\tmigrate\tnative intent API",
		"workflow-job\t.github/workflows/ci.yml#deploy\tretire\tremove after resource cutover",
	}, "\n")
	if migrate, retire, err := check(items, valid); err != nil || migrate != 1 || retire != 1 {
		t.Fatalf("valid inventory migrate=%d retire=%d err=%v", migrate, retire, err)
	}
	for _, invalid := range []string{
		"script\tscripts/write.py\tmigrate\tnative intent API",
		valid + "\nworkflow-job\told.yml#stale\tretire\tdeleted",
		valid + "\nscript\tscripts/write.py\tretire\tduplicate",
		valid + "\nworkflow-job\tbad.yml#job\tdone\tunknown",
	} {
		if _, _, err := check(items, invalid); err == nil {
			t.Errorf("accepted incomplete or invalid inventory %q", invalid)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
