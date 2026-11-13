package store

import (
	"context"
	"regexp"
)

// Retained only to validate already-retired stores during the reader compatibility window.
// Creating new retirement plans and retiring relations are no longer supported.
var compilerInputRetirementPlanHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

func compilerInputArchiveRetirementGuardsInstalled(ctx context.Context, db inputQueryer) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled IN ('O','A') AND tgfoid=to_regprocedure('fugue_platform_input_archive_retirement_guard()') AND tgname IN (
		'fugue_platform_input_archive_control_retired_row_guard','fugue_platform_input_archive_control_retired_truncate_guard',
		'fugue_platform_input_archives_retired_row_guard','fugue_platform_input_archives_retired_truncate_guard',
		'fugue_platform_input_archive_segments_retired_row_guard','fugue_platform_input_archive_segments_retired_truncate_guard',
		'fugue_platform_input_archive_leases_retired_row_guard','fugue_platform_input_archive_leases_retired_truncate_guard'
	)`).Scan(&count)
	return count == 8, err
}
