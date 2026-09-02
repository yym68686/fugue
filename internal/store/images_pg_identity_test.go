package store

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestImageReplicationTaskAdvisoryLockIdentityIsTextSafeAndUnambiguous(t *testing.T) {
	first := model.ImageReplicationTask{
		ID: "", ImageID: "image", SourceReplicaID: "a\x00b", TargetNodeID: "c",
		TargetRuntimeID: "runtime", TargetClusterNodeName: "node", Priority: "normal",
	}
	second := first
	second.SourceReplicaID = "a"
	second.TargetNodeID = "b\x00c"

	left, err := imageReplicationTaskAdvisoryLockIdentity(first)
	if err != nil {
		t.Fatalf("encode first identity: %v", err)
	}
	right, err := imageReplicationTaskAdvisoryLockIdentity(second)
	if err != nil {
		t.Fatalf("encode second identity: %v", err)
	}
	if strings.ContainsRune(left, '\x00') || strings.ContainsRune(right, '\x00') {
		t.Fatal("advisory lock identity must be valid PostgreSQL text without NUL bytes")
	}
	if left == right {
		t.Fatalf("distinct task identities collided: %q", left)
	}
	again, err := imageReplicationTaskAdvisoryLockIdentity(first)
	if err != nil || again != left {
		t.Fatalf("identity must be deterministic: %q vs %q (err=%v)", left, again, err)
	}
}
