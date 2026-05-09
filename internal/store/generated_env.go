package store

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
)

func applyGeneratedEnvSpec(spec *model.AppSpec, previous *model.AppSpec) error {
	if spec == nil {
		return nil
	}
	generated := model.NormalizeAppGeneratedEnvSpecs(spec.GeneratedEnv)
	if len(generated) == 0 {
		spec.GeneratedEnv = nil
		return nil
	}
	spec.GeneratedEnv = generated
	if spec.Env == nil {
		spec.Env = make(map[string]string, len(generated))
	}

	keys := make([]string, 0, len(generated))
	for key := range generated {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if !validGeneratedEnvName(key) {
			return fmt.Errorf("%w: generated_env key %q is not a valid environment variable name", ErrInvalidInput, key)
		}
		if model.IsFugueInjectedAppEnvName(key) {
			return fmt.Errorf("%w: generated_env key %q is reserved for Fugue platform injection", ErrInvalidInput, key)
		}
		envSpec := generated[key]
		if envSpec.Generate != model.AppGeneratedEnvGenerateRandom {
			return fmt.Errorf("%w: generated_env %s has unsupported generator %q", ErrInvalidInput, key, envSpec.Generate)
		}
		switch envSpec.Encoding {
		case model.AppGeneratedEnvEncodingBase64URL, model.AppGeneratedEnvEncodingBase64, model.AppGeneratedEnvEncodingHex:
		default:
			return fmt.Errorf("%w: generated_env %s has unsupported encoding %q", ErrInvalidInput, key, envSpec.Encoding)
		}
		if spec.Env[key] != "" {
			continue
		}
		if previous != nil && previous.Env != nil && previous.Env[key] != "" {
			spec.Env[key] = previous.Env[key]
			continue
		}
		value, err := generateAppEnvSecret(envSpec)
		if err != nil {
			return fmt.Errorf("generate env %s: %w", key, err)
		}
		spec.Env[key] = value
	}
	return nil
}

func generateAppEnvSecret(spec model.AppGeneratedEnvSpec) (string, error) {
	length := spec.Length
	if length <= 0 {
		length = model.DefaultAppGeneratedEnvBytes
	}
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	switch spec.Encoding {
	case model.AppGeneratedEnvEncodingHex:
		return hex.EncodeToString(buf), nil
	case model.AppGeneratedEnvEncodingBase64:
		return base64.StdEncoding.EncodeToString(buf), nil
	default:
		return base64.RawURLEncoding.EncodeToString(buf), nil
	}
}

func validGeneratedEnvName(key string) bool {
	if key == "" || strings.TrimSpace(key) != key {
		return false
	}
	for index := 0; index < len(key); index++ {
		ch := key[index]
		switch {
		case ch >= 'A' && ch <= 'Z':
		case ch >= 'a' && ch <= 'z':
		case ch == '_':
		case index > 0 && ch >= '0' && ch <= '9':
		default:
			return false
		}
	}
	return true
}
