package model

import "testing"

func TestRecoveryRequiresExplicitIsolationAndBusinessProof(t *testing.T) {
	zero := int64(0)
	base := AppRecoveryStartRequest{AuthorizeOverlap: true, StorageStrategy: "fresh", RetentionSeconds: &zero, Probes: []AppReleaseProbe{{Path: "/transaction", ExpectedBodyContains: "accepted"}}}
	spec := AppSpec{Replicas: 1, PersistentStorage: &AppPersistentStorageSpec{Mounts: []AppPersistentStorageMount{{Kind: "directory", Path: "/data"}}}}
	for _, test := range []struct {
		name      string
		paths     []string
		wantError bool
	}{
		{"isolated", []string{"/tmp/spool"}, false}, {"root", []string{"/"}, true}, {"parent", []string{"/data"}, true}, {"child", []string{"/data/spool"}, true}, {"mask-system", []string{"/etc/spool"}, true}, {"duplicates", []string{"/tmp/spool", "/tmp/spool"}, true}, {"relative", []string{"tmp/spool"}, true}, {"traversal", []string{"/tmp/../data"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := base
			req.EphemeralPaths = test.paths
			if err := req.Validate(spec); (err != nil) != test.wantError {
				t.Fatalf("error=%v, wantError=%v", err, test.wantError)
			}
		})
	}
	for _, mutate := range []func(*AppRecoveryStartRequest){func(r *AppRecoveryStartRequest) { r.AuthorizeOverlap = false }, func(r *AppRecoveryStartRequest) { r.RetentionSeconds = nil }, func(r *AppRecoveryStartRequest) { r.Probes = nil }, func(r *AppRecoveryStartRequest) { r.StorageStrategy = "shared" }, func(r *AppRecoveryStartRequest) { r.Probes = []AppReleaseProbe{{Path: "/health"}} }} {
		req := base
		mutate(&req)
		if req.Validate(spec) == nil {
			t.Fatal("unsafe request accepted")
		}
	}
}

func TestRecoveryCanRepeatExistingSpoolIsolation(t *testing.T) {
	zero := int64(0)
	spec := AppSpec{Replicas: 1, EphemeralPaths: []string{"/tmp/spool"}}
	r := AppRecoveryStartRequest{AuthorizeOverlap: true, StorageStrategy: "fresh", RetentionSeconds: &zero, EphemeralPaths: []string{"/tmp/spool"}, Probes: []AppReleaseProbe{{Path: "/business", ExpectedBodyContains: "accepted"}}}
	if err := r.Validate(spec); err != nil {
		t.Fatal(err)
	}
	if got := RecoveryEphemeralPaths(spec, r.EphemeralPaths); len(got) != 1 {
		t.Fatal("recovery duplicated emptyDir mount", got)
	}
	r.EphemeralPaths = []string{"/tmp"}
	if r.Validate(spec) == nil {
		t.Fatal("overlapping existing mount accepted")
	}
}
