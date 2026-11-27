package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"fugue/internal/productioncontrol"
)

func signExecutionWitness(args []string, out io.Writer, now func() time.Time) error {
	if len(args) != 4 {
		return errors.New(usage)
	}
	raw, err := readFile(args[0], productioncontrol.MaxWitnessBytes, false)
	if err != nil {
		return err
	}
	if fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) != args[1] {
		return errors.New("witness draft differs from the reviewed file digest")
	}
	var witness productioncontrol.ExecutionWitness
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&witness); err != nil {
		return fmt.Errorf("read execution witness draft: %w", err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) || len(witness.Signature) != 0 || witness.ExpiresAt != 0 {
		return errors.New("witness draft must contain a single unsigned observation with no expiry")
	}
	ttl, err := time.ParseDuration(args[3])
	if err != nil || ttl < time.Second || ttl > productioncontrol.MaxWitnessAge || ttl%time.Second != 0 {
		return errors.New("invalid execution witness lifetime in whole seconds")
	}
	at := now().Unix()
	if witness.ObservedAt <= 0 || witness.ObservedAt > at || witness.ObservedAt <= at-int64(ttl/time.Second) {
		return errors.New("witness observation falls outside the signing window")
	}
	witness.ExpiresAt = witness.ObservedAt + int64(ttl/time.Second)
	signingInput, err := witness.SigningInput()
	if err != nil {
		return err
	}
	key, err := readSigningKey(args[2])
	if err != nil {
		return err
	}
	defer clear(key)
	witness.Signature = ed25519.Sign(key, signingInput)
	envelope, _, err := productioncontrol.EncodeExecutionWitness(witness)
	if err != nil {
		return err
	}
	n, err := out.Write(envelope)
	if err == nil && n != len(envelope) {
		return io.ErrShortWrite
	}
	return err
}
