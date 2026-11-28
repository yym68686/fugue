package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"github.com/google/go-containerregistry/pkg/authn"
)

func verifyReleaseRecoveryMirror(ctx context.Context, path string, out io.Writer) error {
	raw, err := readFile(path, 16<<10, false)
	if err != nil {
		return err
	}
	type artifact struct {
		ReceiptFile   string `json:"receiptFile"`
		ReceiptDigest string `json:"receiptDigest"`
		Repository    string `json:"destinationRepository"`
	}
	var cfg struct {
		Registry string   `json:"registry"`
		AuthFile string   `json:"registryAuthFile,omitempty"`
		Forward  artifact `json:"forward"`
		Rollback artifact `json:"rollback"`
	}
	if decodeRecoveryMirrorJSON(raw, &cfg) != nil {
		return errors.New("invalid release recovery mirror configuration")
	}
	var auth authn.Authenticator
	if cfg.AuthFile != "" {
		if !filepath.IsAbs(cfg.AuthFile) {
			return errors.New("recovery registry credential file must have an absolute path")
		}
		raw, err := readFile(cfg.AuthFile, 8<<10, true)
		if err != nil {
			return err
		}
		defer clear(raw)
		var secret struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if decodeRecoveryMirrorJSON(raw, &secret) != nil || secret.Username == "" || secret.Password == "" {
			return errors.New("invalid independent recovery registry credential")
		}
		auth = authn.FromConfig(authn.AuthConfig{Username: secret.Username, Password: secret.Password})
	}
	registry, err := releaseguardian.NewIndependentRegistry(cfg.Registry, auth, nil)
	if err != nil {
		return err
	}
	readArtifact := func(input artifact) (releaseguardian.RegistryRecoveryArtifact, error) {
		if !filepath.IsAbs(input.ReceiptFile) {
			return releaseguardian.RegistryRecoveryArtifact{}, errors.New("recovery artifact receipt file must have an absolute path")
		}
		raw, err := readFile(input.ReceiptFile, 32<<10, false)
		if err != nil {
			return releaseguardian.RegistryRecoveryArtifact{}, err
		}
		receipt, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(raw))
		return releaseguardian.RegistryRecoveryArtifact{Source: receipt, SourceReceiptDigest: input.ReceiptDigest,
			DestinationRepository: input.Repository}, err
	}
	forward, err := readArtifact(cfg.Forward)
	if err != nil {
		return err
	}
	rollback, err := readArtifact(cfg.Rollback)
	if err != nil {
		return err
	}
	report, err := registry.VerifyRecoveryMirror(ctx, forward, rollback)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(report)
}

func decodeRecoveryMirrorJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("expected one recovery mirror configuration object")
	}
	return nil
}
