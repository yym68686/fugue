package pgimage

import "testing"

func TestCatalogRequiresExactVerifiedImmutableTarget(t *testing.T) {
	c, err := ApprovedCatalog()
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Target(""); err != nil || got != c.DefaultImage {
		t.Fatalf("default target: %s %v", got, err)
	}
	for _, image := range []string{"postgres:latest", "registry.invalid/pg@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", c.PostgresBase} {
		if _, err := Target(image); err == nil {
			t.Fatalf("unverified image accepted: %s", image)
		}
	}
}
