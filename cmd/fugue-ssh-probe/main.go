package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
)

func main() {
	if err := run(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "SSH identity probe:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, input io.Reader, output io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(input, 8193))
	if err != nil || len(raw) > 8192 {
		return errors.New("probe request exceeds its byte bound or cannot be read")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var request struct {
		Target      string `json:"target"`
		PublicKey   string `json:"public_key"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := decoder.Decode(&request); err != nil {
		return errors.New("bounded target, public key, and fingerprint JSON required")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("exactly one bounded probe request required")
	}
	key, _, _, rest, err := ssh.ParseAuthorizedKey([]byte(request.PublicKey))
	if err != nil || len(rest) != 0 || ssh.FingerprintSHA256(key) != request.Fingerprint {
		return errors.New("public identity does not match the expected fingerprint")
	}
	observation, probeErr := sshidentity.Probe(ctx, request.Target, key, 5*time.Second)
	if err := json.NewEncoder(output).Encode(observation); err != nil {
		return err
	}
	return probeErr
}
