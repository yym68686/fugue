package main

import "testing"

func TestRetentionInventoryModes(t *testing.T) {
	for _, mode := range []string{"archive-retention-inventory", "archive-retention-prepared-inventory", idleRetentionMode} {
		if !isRetentionInventoryMode(mode) {
			t.Fatal(mode)
		}
	}
	for _, mode := range []string{"archive-retire", "archive-copy", "archive-retention-idle-inventory-apply", ""} {
		if isRetentionInventoryMode(mode) {
			t.Fatal(mode)
		}
	}
}
