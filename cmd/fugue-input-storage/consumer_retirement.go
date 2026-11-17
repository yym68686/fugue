package main

import (
	"context"
	"io"
	"os"

	"fugue/internal/store"
)

func decodeConsumerRetirementContract(r io.Reader) (store.PlatformConsumerRetirementContract, error) {
	var c store.PlatformConsumerRetirementContract
	if err := decodePreparationJSON(r, &c, 64<<10); err != nil || c.Validate() != nil {
		return c, store.ErrInvalidInput
	}
	return c, nil
}

func runConsumerRetirement(ctx context.Context, path string, apply bool, plan string) (store.PlatformConsumerRetirementReport, error) {
	f, err := preparationJSONReader(path)
	if err != nil {
		return store.PlatformConsumerRetirementReport{}, store.ErrInvalidInput
	}
	defer f.Close()
	c, err := decodeConsumerRetirementContract(f)
	if err != nil {
		return store.PlatformConsumerRetirementReport{}, err
	}
	return store.RunPlatformConsumerRetirement(ctx, os.Getenv("FUGUE_DATABASE_URL"), c, apply, plan)
}
