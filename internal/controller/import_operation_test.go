package controller

import "testing"

func TestRewriteImportedImageRef(t *testing.T) {
	t.Parallel()

	got, err := rewriteImportedImageRef(
		"fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/demo:git-abc123",
		"fugue-fugue-registry.fugue-system.svc.cluster.local:5000",
		"10.128.0.2:30500",
	)
	if err != nil {
		t.Fatalf("rewrite imported image ref: %v", err)
	}
	want := "10.128.0.2:30500/fugue-apps/demo:git-abc123"
	if got != want {
		t.Fatalf("unexpected rewritten image ref: got %q want %q", got, want)
	}
}

func TestRewriteImportedImageRefRejectsUnexpectedPushBase(t *testing.T) {
	t.Parallel()

	if _, err := rewriteImportedImageRef(
		"registry.example.com/fugue-apps/demo:git-abc123",
		"fugue-fugue-registry.fugue-system.svc.cluster.local:5000",
		"10.128.0.2:30500",
	); err == nil {
		t.Fatalf("expected rewrite to fail for mismatched push base")
	}
}
