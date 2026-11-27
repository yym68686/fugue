package api

import (
	"testing"
	"time"
)

func TestDataGCIdentityRequiresUnchangedETagForTimestampPrecision(t *testing.T) {
	stamp := time.Date(2026, 10, 18, 5, 55, 53, 741000000, time.UTC)
	listed := dataObjectInfo{Size: 13, ETag: `"original"`, LastModified: stamp}
	for _, tc := range []struct {
		name    string
		current dataObjectInfo
		matches bool
	}{
		{"HTTP-date precision", dataObjectInfo{Size: 13, ETag: listed.ETag, LastModified: stamp.Truncate(time.Second)}, true},
		{"replacement in same second", dataObjectInfo{Size: 13, ETag: `"replacement"`, LastModified: stamp.Truncate(time.Second)}, false},
		{"missing ETag", dataObjectInfo{Size: 13, LastModified: stamp.Truncate(time.Second)}, false},
		{"changed size", dataObjectInfo{Size: 14, ETag: listed.ETag, LastModified: stamp.Truncate(time.Second)}, false},
		{"changed timestamp", dataObjectInfo{Size: 13, ETag: listed.ETag, LastModified: stamp.Add(time.Second).Truncate(time.Second)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dataGCObjectIdentityMatches(listed, tc.current); got != tc.matches {
				t.Fatalf("identity matches=%v want %v", got, tc.matches)
			}
		})
	}
	listed.ETag = ""
	if dataGCObjectIdentityMatches(listed, dataObjectInfo{Size: 13, LastModified: stamp.Truncate(time.Second)}) {
		t.Fatal("relaxed timestamp precision without ETag")
	}
}
