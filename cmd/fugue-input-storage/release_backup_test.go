package main

import "testing"

func TestReleaseBackupFlagsRejectUnrelatedOperations(t *testing.T) {
	if !validReleaseBackupFlags(map[string]bool{"mode": true, "release-backup-contract": true}) {
		t.Fatal("exact read-only flags refused")
	}
	for _, key := range []string{"apply", "rehearse", "digest", "backup-contract", "source-verification-contract"} {
		if validReleaseBackupFlags(map[string]bool{"mode": true, "release-backup-contract": true, key: true}) {
			t.Fatal("unrelated operation accepted", key)
		}
	}
}
