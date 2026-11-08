package main

import (
	"context"
	"io"
	"os"

	"fugue/internal/store"
)

func validSourceVerificationFlags(selected map[string]bool) bool {
	if !selected["mode"] || !selected["source-verification-contract"] {
		return false
	}
	for name := range selected {
		if name != "mode" && name != "source-verification-contract" {
			return false
		}
	}
	return true
}

func decodeSourceVerificationContract(r io.Reader) (store.CompilerInputSourceVerificationContract, error) {
	var c store.CompilerInputSourceVerificationContract
	if err := decodePreparationJSON(r, &c, 16<<10); err != nil || c.Validate() != nil {
		return c, store.ErrInvalidInput
	}
	return c, nil
}

func runSourceVerification(ctx context.Context, path string) (store.CompilerInputSourceVerificationReport, error) {
	f, err := preparationJSONReader(path)
	if err != nil {
		return store.CompilerInputSourceVerificationReport{}, store.ErrInvalidInput
	}
	defer f.Close()
	c, err := decodeSourceVerificationContract(f)
	if err != nil {
		return store.CompilerInputSourceVerificationReport{}, err
	}
	return store.RunCompilerInputSourceVerification(ctx, os.Getenv("FUGUE_DATABASE_URL"), taskPreparationKeyring(), c)
}
