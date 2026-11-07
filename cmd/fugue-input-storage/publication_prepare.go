package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"time"

	"fugue/internal/store"
)

func decodePublicationPreparationContract(r io.Reader) (store.PublicationInputPreparationContract, error) {
	var c store.PublicationInputPreparationContract
	if err := decodePreparationJSON(r, &c, 128<<10); err != nil || c.Validate(false) != nil {
		return c, store.ErrInvalidInput
	}
	return c, nil
}

func decodePublicationPreparationPlan(r io.Reader, now time.Time) (store.PublicationInputPreparationPlan, error) {
	var p store.PublicationInputPreparationPlan
	if err := decodePreparationJSON(r, &p, 513<<10); err != nil || p.Ticket.Validate(p.VerificationKey, now) != nil {
		return p, store.ErrInvalidInput
	}
	return p, nil
}

func decodePreparationJSON(r io.Reader, value any, limit int64) error {
	raw, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil || int64(len(raw)) > limit {
		return store.ErrInvalidInput
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return store.ErrInvalidInput
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return store.ErrInvalidInput
	}
	return nil
}

func preparationJSONReader(path string) (io.ReadCloser, error) {
	if path == "-" {
		return io.NopCloser(os.Stdin), nil
	}
	return os.Open(path)
}

func runPublicationPreparation(ctx context.Context, mode, contractPath, planPath, backend string) (any, error) {
	path := planPath
	if mode == "publication-input-plan" {
		path = contractPath
	}
	file, err := preparationJSONReader(path)
	if err != nil {
		return nil, store.ErrInvalidInput
	}
	defer file.Close()
	databaseURL := os.Getenv("FUGUE_DATABASE_URL")
	if mode == "publication-input-plan" {
		c, err := decodePublicationPreparationContract(file)
		if err != nil {
			return nil, err
		}
		return store.RunPublicationInputPreparationPlan(ctx, databaseURL, taskPreparationKeyring(), c, backend)
	}
	p, err := decodePublicationPreparationPlan(file, time.Now())
	if err != nil {
		return nil, err
	}
	if mode == "publication-input-confirm" {
		return store.RunPublicationInputPreparationConfirmation(ctx, databaseURL, taskPreparationKeyring(), p.Ticket)
	}
	// Independently pinned by the immutable trusted runner intent. A public
	// key supplied solely by the ticket's author is not a trust anchor.
	key := os.Getenv("FUGUE_PUBLICATION_INPUT_PREPARATION_VERIFICATION_KEY")
	if mode != "publication-input-restore" || key == "" || key != p.VerificationKey {
		return nil, store.ErrConflict
	}
	reader, err := store.NodeTaskPreparationReaderFromEnv()
	if err != nil {
		return nil, err
	}
	return store.RunPublicationInputRestore(ctx, databaseURL, p.Ticket, key, reader)
}
