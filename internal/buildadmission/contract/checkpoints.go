// Package contract defines the original v1 keys shared by the builder
// admission domain and its Store adapter.
package contract

import "strings"

const (
	memory  = "builder-memory/v1/"
	failure = "builder-permanent-failures/v1/"
	oom     = "builder-oom-history/v1/"
)

func Memory(tenantID, appID string) string { return memory + tenantID + "/" + appID }

func Failure(tenantID, appID string) string { return failure + tenantID + "/" + appID }

func OOM(tenantID, appID, fingerprint string) string {
	return oom + tenantID + "/" + appID + "/" + fingerprint
}

// OwnsCheckpoint recognizes the whole namespace, even for malformed keys.
// The general Store writers must not mutate it.
func OwnsCheckpoint(key string) bool {
	return strings.HasPrefix(key, memory) || strings.HasPrefix(key, failure) || strings.HasPrefix(key, oom)
}
