package model

import (
	"fmt"
	"strings"
)

const managedPostgresReservedUser = "postgres"

func DefaultManagedPostgresUser(appName string) string {
	slug := strings.ReplaceAll(Slugify(appName), "-", "_")
	slug = strings.Trim(slug, "_")
	if slug == "" {
		slug = "app"
	}
	if slug[0] >= '0' && slug[0] <= '9' {
		slug = "app_" + slug
	}
	if len(slug) > 63 {
		slug = strings.TrimRight(slug[:63], "_")
	}
	if slug == "" {
		return "app"
	}
	return slug
}

func NormalizeManagedPostgresImage(image string) string {
	image = strings.TrimSpace(image)
	if image == "" {
		return ""
	}
	if isOfficialPostgresImage(image) {
		return ""
	}
	return image
}

func ValidateManagedPostgresUser(appName string, spec AppPostgresSpec) error {
	if !strings.EqualFold(strings.TrimSpace(spec.User), managedPostgresReservedUser) {
		return nil
	}

	return fmt.Errorf(
		"managed CNPG postgres user %q is reserved; use an app-scoped user such as %q",
		managedPostgresReservedUser,
		DefaultManagedPostgresUser(appName),
	)
}

func CloneResourceSpec(spec *ResourceSpec) *ResourceSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	return &out
}

func CloneAppPostgresSpec(spec *AppPostgresSpec) *AppPostgresSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	out.Resources = CloneResourceSpec(spec.Resources)
	out.RuntimeResources = CloneResourceSpec(spec.RuntimeResources)
	return &out
}

// ManagedPostgresRuntimeResources returns the in-place runtime target when it
// exists and otherwise falls back to the CNPG bootstrap template. The returned
// value is detached from the input so callers cannot mutate persisted state.
func ManagedPostgresRuntimeResources(spec AppPostgresSpec) ResourceSpec {
	if spec.RuntimeResources != nil {
		return *spec.RuntimeResources
	}
	if spec.Resources != nil {
		return *spec.Resources
	}
	return DefaultManagedPostgresResources()
}

func isOfficialPostgresImage(image string) bool {
	repository := postgresImageRepository(image)
	return repository == "postgres" || repository == "library/postgres"
}

func postgresImageRepository(image string) string {
	image = strings.ToLower(strings.TrimSpace(image))
	if image == "" {
		return ""
	}
	if withoutDigest, _, found := strings.Cut(image, "@"); found {
		image = withoutDigest
	}
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon > lastSlash {
		image = image[:lastColon]
	}
	switch {
	case strings.HasPrefix(image, "docker.io/"):
		image = strings.TrimPrefix(image, "docker.io/")
	case strings.HasPrefix(image, "index.docker.io/"):
		image = strings.TrimPrefix(image, "index.docker.io/")
	case strings.HasPrefix(image, "registry-1.docker.io/"):
		image = strings.TrimPrefix(image, "registry-1.docker.io/")
	}
	return image
}
