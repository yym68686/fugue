//go:build !linux

package main

import "testing"

func TestIdleRetentionUnsupportedPlatform(t *testing.T) {
	if enterIdleRetention() == nil {
		t.Fatal("unsupported scheduling contract accepted")
	}
}
