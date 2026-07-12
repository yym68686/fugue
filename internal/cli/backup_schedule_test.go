package cli

import (
	"strings"
	"testing"
)

func TestBackupPolicyRequestMapValidatesSchedule(t *testing.T) {
	t.Parallel()

	req, err := backupPolicyRequestMap(backupPolicyOptions{Schedule: "0 */6 * * *"}, true)
	if err != nil {
		t.Fatalf("expected documented six-hour schedule to be accepted: %v", err)
	}
	if got := req["schedule"]; got != "0 */6 * * *" {
		t.Fatalf("expected schedule to be preserved, got %v", got)
	}

	if _, err := backupPolicyRequestMap(backupPolicyOptions{Schedule: "not cron"}, true); err == nil || !strings.Contains(err.Error(), "invalid backup schedule") {
		t.Fatalf("expected invalid schedule error, got %v", err)
	}
}

func TestBackupRunRequestMapOmitsDefaultTargetWhenPolicyIsSet(t *testing.T) {
	t.Parallel()

	req := backupRunRequestMap(backupRunOptions{
		PolicyID:   "tenant-policy",
		TargetType: "control-plane-db",
	})
	if _, ok := req["target"]; ok {
		t.Fatalf("policy-backed run must not send an ignored default target: %+v", req)
	}

	direct := backupRunRequestMap(backupRunOptions{TargetType: "app-database"})
	if _, ok := direct["target"]; !ok {
		t.Fatalf("direct run must send its target: %+v", direct)
	}
}
