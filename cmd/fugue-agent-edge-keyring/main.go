// fugue-agent-edge-keyring manages explicit out-of-band trust configuration.
// It never contacts an API or generates keys as a side effect of validation.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"fugue/internal/agentedge"
)

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("expected generate or verify")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	f.SetOutput(io.Discard)
	privatePath := f.String("private-file", "", "absolute path to private keyring")
	publicPath := f.String("public-file", "", "absolute path to independently declared public trust")
	id := f.String("key-id", "", "explicit new key identity")
	generation := f.Uint64("generation", 0, "explicit positive keyring generation")
	before := f.String("not-before", "", "absolute RFC3339 start")
	after := f.String("not-after", "", "absolute RFC3339 end")
	grantFile := f.String("grant-file", "", "absolute signed grant path")
	audience := f.String("audience", "", "expected runtime identity")
	origin := f.String("origin", "", "expected HTTPS origin")
	if err := f.Parse(args[1:]); err != nil || f.NArg() != 0 {
		return errors.New("invalid keyring command or private path")
	}
	if args[0] == "verify-grant" {
		if *privatePath != "" || *id != "" || *generation != 0 || *before != "" || *after != "" || !filepath.IsAbs(*publicPath) || !filepath.IsAbs(*grantFile) || *audience == "" || *origin == "" {
			return errors.New("grant verification requires only grant, public trust and expected audience/origin")
		}
		trust, err := agentedge.LoadTrustKeyring(*publicPath)
		if err != nil {
			return err
		}
		keys, err := trust.PublicKeys()
		if err != nil {
			return err
		}
		file, err := os.Open(*grantFile)
		if err != nil {
			return errors.New("signed grant file unavailable")
		}
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, agentedge.MaxGrantBytes+1))
		if err != nil {
			return errors.New("signed grant read failed")
		}
		verified, err := agentedge.Verify(raw, keys, *audience, *origin, nil, time.Now())
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(map[string]any{"verified": true, "trust_generation": trust.Generation, "digest": verified.Digest(), "grant": verified.View()})
	}
	if !filepath.IsAbs(*privatePath) || *grantFile != "" || *audience != "" || *origin != "" {
		return errors.New("invalid keyring command arguments")
	}
	switch args[0] {
	case "generate":
		start, e1 := time.Parse(time.RFC3339, *before)
		end, e2 := time.Parse(time.RFC3339, *after)
		if e1 != nil || e2 != nil || !end.After(start) || *id == "" || *generation == 0 || *publicPath != "" {
			return errors.New("generation requires explicit identity, generation and absolute validity")
		}
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return errors.New("key generation failed")
		}
		ring := agentedge.PrivateKeyring{Schema: agentedge.PrivateKeyringSchema, Generation: *generation, Keys: []agentedge.PrivateKeyConfig{{PublicKeyConfig: agentedge.PublicKeyConfig{KeyID: *id, PublicKey: base64.RawURLEncoding.EncodeToString(pub), NotBefore: start, NotAfter: end}, PrivateKey: base64.RawURLEncoding.EncodeToString(key)}}}
		if err = ring.Validate(); err != nil {
			return err
		}
		raw, err := json.Marshal(ring)
		if err != nil {
			return errors.New("keyring encoding failed")
		}
		file, err := os.OpenFile(*privatePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return errors.New("private output must not already exist and must have a writable parent")
		}
		if _, err = file.Write(append(raw, '\n')); err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return errors.New("private keyring persistence failed; inspect the output before retrying")
		}
		return json.NewEncoder(out).Encode(ring.Public())
	case "verify":
		if *id != "" || *generation != 0 || *before != "" || *after != "" || !filepath.IsAbs(*publicPath) {
			return errors.New("verification requires only private and public file paths")
		}
		private, err := agentedge.LoadPrivateKeyring(*privatePath)
		if err != nil {
			return err
		}
		public, err := agentedge.LoadTrustKeyring(*publicPath)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(private.Public(), public) {
			return errors.New("private keyring does not match independently declared public trust")
		}
		return json.NewEncoder(out).Encode(map[string]any{"verified": true, "generation": public.Generation, "keys": len(public.Keys)})
	default:
		return errors.New("expected generate or verify")
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
