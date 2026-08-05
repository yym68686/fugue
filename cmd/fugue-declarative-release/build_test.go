package main

import (
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestBuildArgumentsAreSingleComponentImmutablePush(t *testing.T) {
	release := declarativerelease.PlanRelease{Artifact: declarativerelease.Artifact{
		Repository: "ghcr.io/example/fugue-api", Dockerfile: "Dockerfile.api", Context: ".", BuildPackage: "./cmd/fugue-api",
	}}
	sha := strings.Repeat("a", 40)
	arguments := buildArguments(release, sha, "/tmp/metadata.json")
	got := strings.Join(arguments, " ")
	for _, required := range []string{
		"buildx build", "--platform linux/amd64", "--file Dockerfile.api",
		"--label org.opencontainers.image.revision=" + sha,
		"--tag ghcr.io/example/fugue-api:" + sha,
		"--metadata-file /tmp/metadata.json", "--push .",
	} {
		if !strings.Contains(got, required) {
			t.Fatalf("build command is missing %q: %s", required, got)
		}
	}
	for _, forbidden := range []string{"--build-arg", "--load", "--output", "--cache-to"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("build command contains forbidden option %q: %s", forbidden, got)
		}
	}
}

func TestImmutableTagPreflightIsAbsentOrFullyVerified(t *testing.T) {
	image := "ghcr.io/example/fugue-api:" + strings.Repeat("a", 40)
	if existing, err := decodeTagPreflight([]byte(`{"exists":false,"image":"`+image+`"}`), image); err != nil || existing != nil {
		t.Fatalf("valid missing-tag receipt: existing=%+v err=%v", existing, err)
	}
	digest := "sha256:" + strings.Repeat("b", 64)
	verified := `{"blob_count":4,"config_digest":"sha256:` + strings.Repeat("c", 64) + `","image":"ghcr.io/example/fugue-api@` + digest + `","index_digest":"` + digest + `","layer_get_probe_count":3,"manifest_digest":"sha256:` + strings.Repeat("d", 64) + `","oci_revision":"` + strings.Repeat("a", 40) + `","platform":"linux/amd64","request_count":12,"total_layer_bytes":4096,"verification":"registry_manifest_config_and_layer_get"}`
	if existing, err := decodeTagPreflight([]byte(verified), image); err != nil || existing == nil || existing.Image != "ghcr.io/example/fugue-api@"+digest {
		t.Fatalf("valid existing-tag receipt: existing=%+v err=%v", existing, err)
	}
	for _, invalid := range []string{
		`{"exists":true,"image":"` + image + `"}`,
		`{"exists":false,"image":"other"}`,
		`{"exists":false,"image":"` + image + `","extra":true}`,
		strings.Replace(verified, "registry_manifest_config_and_layer_get", "registry_manifest_config_get", 1),
		strings.Replace(verified, `"layer_get_probe_count":3`, `"layer_get_probe_count":0`, 1),
	} {
		if _, err := decodeTagPreflight([]byte(invalid), image); err == nil {
			t.Fatalf("invalid tag receipt was accepted: %s", invalid)
		}
	}
}

func TestBuildMetadataRequiresCanonicalDigest(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	got, err := topDigestFromBuildMetadata([]byte(`{"containerimage.digest":"` + digest + `"}`))
	if err != nil || got != digest {
		t.Fatalf("parse build metadata: got=%q err=%v", got, err)
	}
	for _, invalid := range []string{
		`{}`,
		`{"containerimage.digest":"sha256:short"}`,
		`{"containerimage.digest":"sha256:` + strings.Repeat("B", 64) + `"}`,
	} {
		if _, err := topDigestFromBuildMetadata([]byte(invalid)); err == nil {
			t.Fatalf("invalid build metadata was accepted: %s", invalid)
		}
	}
}
