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
