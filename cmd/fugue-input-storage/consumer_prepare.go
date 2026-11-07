package main

import (
	"context"
	"io"
	"os"
	"time"

	"fugue/internal/store"
)

func decodeConsumerPreparationContract(r io.Reader) (store.ConsumerInputPreparationContract, error) {
	var c store.ConsumerInputPreparationContract
	if err := decodePreparationJSON(r, &c, 128<<10); err != nil || c.Validate(false) != nil {
		return c, store.ErrInvalidInput
	}
	return c, nil
}

func decodeConsumerPreparationPlan(r io.Reader, now time.Time) (store.ConsumerInputPreparationPlan, error) {
	var p store.ConsumerInputPreparationPlan
	if err := decodePreparationJSON(r, &p, 513<<10); err != nil || p.Ticket.Validate(p.VerificationKey, now) != nil {
		return p, store.ErrInvalidInput
	}
	return p, nil
}

func runConsumerPreparation(ctx context.Context, mode, contractPath, planPath, backend string) (any, error) {
	path := planPath
	if mode == "consumer-input-plan" {
		path = contractPath
	}
	file, err := preparationJSONReader(path)
	if err != nil {
		return nil, store.ErrInvalidInput
	}
	defer file.Close()
	url := os.Getenv("FUGUE_DATABASE_URL")
	if mode == "consumer-input-plan" {
		c, err := decodeConsumerPreparationContract(file)
		if err != nil {
			return nil, store.ErrInvalidInput
		}
		return store.RunConsumerInputPreparationPlan(ctx, url, taskPreparationKeyring(), c, backend)
	}
	p, err := decodeConsumerPreparationPlan(file, time.Now())
	if err != nil {
		return nil, store.ErrInvalidInput
	}
	if mode == "consumer-input-confirm" {
		return store.RunConsumerInputPreparationConfirmation(ctx, url, taskPreparationKeyring(), p.Ticket)
	}
	key := os.Getenv("FUGUE_CONSUMER_INPUT_PREPARATION_VERIFICATION_KEY")
	if mode != "consumer-input-restore" || key == "" || key != p.VerificationKey {
		return nil, store.ErrConflict
	}
	reader, err := store.NodeTaskPreparationReaderFromEnv()
	if err != nil {
		return nil, err
	}
	return store.RunConsumerInputRestore(ctx, url, p.Ticket, key, reader)
}
