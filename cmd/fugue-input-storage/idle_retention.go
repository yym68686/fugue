package main

const idleRetentionMode = "archive-retention-idle-inventory"

func isRetentionInventoryMode(mode string) bool {
	return mode == "archive-retention-inventory" || mode == "archive-retention-prepared-inventory" || mode == idleRetentionMode
}
