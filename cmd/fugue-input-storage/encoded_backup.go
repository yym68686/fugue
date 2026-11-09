package main

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"fugue/internal/store"
)

func validEncodedBackupFlags(mode string, selected map[string]bool) bool {
	allowed := map[string]bool{"mode": true, "backup-contract": true}
	if mode == "decode-backup" {
		allowed["backup-input"], allowed["backup-output"] = true, true
	} else if mode != "encoded-backup" {
		return false
	}
	for key := range allowed {
		if !selected[key] {
			return false
		}
	}
	for key := range selected {
		if !allowed[key] {
			return false
		}
	}
	return true
}

func readEncodedBackupContract(path string) (store.CompilerInputBackupContract, error) {
	var c store.CompilerInputBackupContract
	f, err := preparationJSONReader(path)
	if err != nil {
		return c, store.ErrInvalidInput
	}
	defer f.Close()
	if err = decodePreparationJSON(f, &c, 16<<10); err != nil || c.Validate() != nil {
		return c, store.ErrInvalidInput
	}
	return c, nil
}

func runEncodedBackup(ctx context.Context, mode, contract, input, output string, stream io.Writer) (store.CompilerInputBackupReport, error) {
	c, err := readEncodedBackupContract(contract)
	if err != nil {
		return store.CompilerInputBackupReport{}, err
	}
	if mode == "encoded-backup" {
		return store.RunCompilerInputBackup(ctx, os.Getenv("FUGUE_DATABASE_URL"), c, stream)
	}
	return decodeEncodedBackupFile(ctx, c, input, output)
}

// A failed or truncated decode never leaves a usable target. The final hard
// link is atomic and refuses to replace any existing file. Both the temporary
// and final bytes stay in an explicitly private directory with mode 0600.
func decodeEncodedBackupFile(ctx context.Context, c store.CompilerInputBackupContract, input, output string) (store.CompilerInputBackupReport, error) {
	var report store.CompilerInputBackupReport
	if input == "" || output == "" || input == "-" || output == "-" {
		return report, store.ErrInvalidInput
	}
	info, err := os.Lstat(input)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return report, store.ErrInvalidInput
	}
	in, err := os.Open(input)
	if err != nil {
		return report, err
	}
	defer in.Close()
	actual, err := in.Stat()
	if err != nil || !os.SameFile(info, actual) || actual.Mode().Perm()&0077 != 0 {
		return report, store.ErrInvalidInput
	}
	dir := filepath.Dir(output)
	dirInfo, err := os.Stat(dir)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode().Perm()&0077 != 0 {
		return report, store.ErrInvalidInput
	}
	if _, err = os.Lstat(output); !os.IsNotExist(err) {
		return report, store.ErrInvalidInput
	}
	tmp, err := os.CreateTemp(dir, ".compiler-input-decode-*")
	if err != nil {
		return report, err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	report, err = store.DecodeCompilerInputBackup(ctx, in, c, tmp)
	if err != nil {
		return report, err
	}
	if err = tmp.Sync(); err != nil {
		report.Complete = false
		return report, err
	}
	if err = tmp.Close(); err != nil {
		report.Complete = false
		return report, err
	}
	if err = os.Link(tmp.Name(), output); err != nil {
		report.Complete = false
		return report, err
	}
	return report, nil
}
