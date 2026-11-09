package main

import (
	"context"
	"io"
	"os"

	"fugue/internal/store"
)

func validReleaseBackupFlags(selected map[string]bool) bool {
	return len(selected) == 2 && selected["mode"] && selected["release-backup-contract"]
}

func runReleaseBackup(ctx context.Context, path string, out io.Writer) (store.PlatformReleaseBackupReport, error) {
	var c store.PlatformReleaseBackupContract
	f, err := preparationJSONReader(path)
	if err != nil {
		return store.PlatformReleaseBackupReport{}, store.ErrInvalidInput
	}
	defer f.Close()
	if decodePreparationJSON(f, &c, 16<<10) != nil || c.Validate() != nil {
		return store.PlatformReleaseBackupReport{}, store.ErrInvalidInput
	}
	return store.RunPlatformReleaseBackup(ctx, os.Getenv("FUGUE_DATABASE_URL"), c, out)
}
