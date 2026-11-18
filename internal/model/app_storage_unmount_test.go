package model

import (
	"reflect"
	"testing"
)

func TestStorageUnmountOnlyRemovesExactMount(t *testing.T) {
	spec := AppSpec{Image: "example.invalid/app:v1", Replicas: 1, Ports: []int{8080}, RuntimeID: "managed",
		PersistentStorage: &AppPersistentStorageSpec{
			Mode: AppPersistentStorageModeMovableRWO, StorageClassName: AppStorageClassFugueLocalRWO, ClaimName: "stable-claim", StorageSize: "4Gi",
			Mounts: []AppPersistentStorageMount{
				{Kind: "file", Path: "/config", SeedContent: "fixed"},
				{Kind: "directory", Path: "/data", Mode: 0o750},
				{Kind: "file", Path: "/other", Secret: true, Mode: 0o440, SeedContent: "secret"},
			},
		},
	}
	want := spec.DeepCopy()
	want.PersistentStorage.Mounts = want.PersistentStorage.Mounts[1:]
	actual, paths, err := AppSpecWithoutPersistentMount(spec, "/config")
	if err != nil || !reflect.DeepEqual(paths, []string{"/data", "/other"}) || !reflect.DeepEqual(actual, want) {
		t.Fatalf("unexpected exact removal: %v, %v", paths, err)
	}
	actual.PersistentStorage.Mounts[0].Mode = 0
	if spec.PersistentStorage.Mounts[1].Mode != 0o750 || len(spec.PersistentStorage.Mounts) != 3 {
		t.Fatal("result aliased the original mount entries")
	}
	for _, input := range []string{"/config/", "/x/../config", "config", "//config", "/data/child", "/"} {
		if _, _, err := AppSpecWithoutPersistentMount(spec, input); err == nil {
			t.Fatalf("expected unsafe or non-exact path %q to be refused", input)
		}
	}
	for name, change := range map[string]func(*AppSpec){
		"last mount":    func(s *AppSpec) { s.PersistentStorage.Mounts = s.PersistentStorage.Mounts[:1] },
		"stopped app":   func(s *AppSpec) { s.Replicas = 0 },
		"reset":         func(s *AppSpec) { s.PersistentStorage.ResetToken = "prior-reset" },
		"unknown class": func(s *AppSpec) { s.PersistentStorage.StorageClassName = "other" },
		"shared":        func(s *AppSpec) { s.PersistentStorage.Mode = AppPersistentStorageModeSharedProjectRWX },
	} {
		t.Run(name, func(t *testing.T) {
			other := spec.DeepCopy()
			change(&other)
			if _, _, err := AppSpecWithoutPersistentMount(other, "/config"); err == nil {
				t.Fatal("unexpected allowance")
			}
		})
	}
}

func TestStorageUnmountProbeConfinesRequest(t *testing.T) {
	for _, u := range []string{"http://elsewhere/probe", "//elsewhere/probe", "/%2fel%s", "/%2fother", "/a/../b", "/a//b", "/test?q=secret", "/test#fragment", "/test\r\nFoo: bar"} {
		if err := (AppStorageUnmountProbe{Path: u, ExpectedBodyContains: "ok"}).Validate(); err == nil {
			t.Errorf("unsafe probe path accepted: %q", u)
		}
	}
	if err := (AppStorageUnmountProbe{Path: "/business", Method: "POST", Body: `{"test":true}`, BearerEnv: "PROBE_TOKEN", ExpectedBodyContains: "ok"}).Validate(); err != nil {
		t.Fatal(err)
	}
}
