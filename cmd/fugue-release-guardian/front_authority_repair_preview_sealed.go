package main

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/crypto/nacl/box"
)

const (
	frontRepairActionsPreviewJob    = "edge_front_repair_preview"
	frontRepairPreviewRequestSchema = "edge-front-repair-preview-request/v1"
	frontRepairPreviewSealSchema    = "edge-front-repair-preview-sealed/v1"
	frontRepairPreviewLimit         = 512 << 10
)

var frontRepairPreviewNonce = regexp.MustCompile("^[0-9a-f]{64}$")

type frontRepairPreviewRequest struct {
	Schema string `json:"schema"`
	Nonce  string `json:"nonce"`
}

type frontRepairPreviewSealScope struct {
	Repository  string `json:"repository"`
	Declaration string `json:"declarationCommit"`
	Executor    string `json:"executorCommit"`
	Nonce       string `json:"nonce"`
	RunID       uint64 `json:"runId"`
	Attempt     uint32 `json:"attempt"`
}

func (s frontRepairPreviewSealScope) valid() bool {
	return frontRepairGitHubRepositoryName.MatchString(s.Repository) && exactSourceSHA(s.Declaration) &&
		exactSourceSHA(s.Executor) && s.Executor != s.Declaration && frontRepairPreviewNonce.MatchString(s.Nonce) &&
		s.RunID != 0 && s.Attempt != 0
}

type frontRepairSealedPreview struct {
	Schema     string                      `json:"schema"`
	Scope      frontRepairPreviewSealScope `json:"scope"`
	Ciphertext []byte                      `json:"ciphertext"`
}

type frontRepairPreviewPayload struct {
	Scope   frontRepairPreviewSealScope `json:"scope"`
	Preview frontRepairPreview          `json:"preview"`
}

func frontRepairRecipient(raw string) (*ecdh.PublicKey, error) {
	block, rest := pem.Decode([]byte(raw))
	if len(raw) > 1024 || block == nil || block.Type != "PUBLIC KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("Front preview requires one protected X25519 public key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	pub, ok := parsed.(*ecdh.PublicKey)
	if err != nil || !ok || pub.Curve() != ecdh.X25519() {
		return nil, errors.New("Front preview recipient is not a supported X25519 key")
	}
	// Reject low-order public points before using the sealed-box library.
	probe, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if _, err := probe.ECDH(pub); err != nil {
		return nil, errors.New("Front preview recipient public point is invalid")
	}
	return pub, nil
}

func sealFrontRepairPreview(scope frontRepairPreviewSealScope, preview frontRepairPreview, recipient *ecdh.PublicKey) (frontRepairSealedPreview, error) {
	digest, err := preview.validatedDigest()
	if err != nil || digest != preview.Digest || !scope.valid() || recipient == nil || recipient.Curve() != ecdh.X25519() {
		return frontRepairSealedPreview{}, errors.New("Front preview could not bind exact scope and runtime evidence")
	}
	raw, err := json.Marshal(frontRepairPreviewPayload{Scope: scope, Preview: preview})
	if err != nil || len(raw) > frontRepairPreviewLimit {
		return frontRepairSealedPreview{}, errors.New("Front preview plaintext is not within its fixed byte budget")
	}
	var key [32]byte
	copy(key[:], recipient.Bytes())
	sealed, err := box.SealAnonymous(nil, raw, &key, rand.Reader)
	if err != nil {
		return frontRepairSealedPreview{}, errors.New("Front preview encryption failed")
	}
	return frontRepairSealedPreview{Schema: frontRepairPreviewSealSchema, Scope: scope, Ciphertext: sealed}, nil
}

// Encryption hides the preview from public artifact consumers. Authentic
// sender provenance still comes from the exact protected Actions run.
func openFrontRepairPreview(sealed frontRepairSealedPreview, expected frontRepairPreviewSealScope, secret *ecdh.PrivateKey) (frontRepairPreview, error) {
	if !expected.valid() || sealed.Schema != frontRepairPreviewSealSchema || sealed.Scope != expected ||
		len(sealed.Ciphertext) <= box.AnonymousOverhead || len(sealed.Ciphertext) > frontRepairPreviewLimit+box.AnonymousOverhead ||
		secret == nil || secret.Curve() != ecdh.X25519() {
		return frontRepairPreview{}, errors.New("Front preview envelope is outside the expected run or key scope")
	}
	var priv, pub [32]byte
	copy(priv[:], secret.Bytes())
	copy(pub[:], secret.PublicKey().Bytes())
	raw, ok := box.OpenAnonymous(nil, sealed.Ciphertext, &pub, &priv)
	clear(priv[:])
	var payload frontRepairPreviewPayload
	if !ok || decodeStrictJSON(raw, &payload) != nil || payload.Scope != expected {
		return frontRepairPreview{}, errors.New("Front preview authentication or sealed run binding failed")
	}
	digest, err := payload.Preview.validatedDigest()
	if err != nil || digest != payload.Preview.Digest {
		return frontRepairPreview{}, errors.New("Front preview payload has no valid bound predecessor")
	}
	return payload.Preview, nil
}

func runFrontRepairPreviewSealed(args []string, stdout io.Writer) error {
	if len(args) != 0 || stdout == nil {
		return errors.New("protected Front preview accepts no interactive arguments")
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("Front preview build identity is unavailable")
	}
	launch, err := frontRepairActionsContext(os.Getenv, info, frontRepairActionsPreviewJob)
	if err != nil {
		return err
	}
	var request frontRepairPreviewRequest
	if err := readFrontRepairReleaseInput(filepath.Join(launch.inputs, "preview-request.json"), "preview request", 1024, &request); err != nil {
		return err
	}
	run, runErr := strconv.ParseUint(os.Getenv("GITHUB_RUN_ID"), 10, 64)
	attempt, attemptErr := strconv.ParseUint(os.Getenv("GITHUB_RUN_ATTEMPT"), 10, 32)
	scope := frontRepairPreviewSealScope{Repository: launch.policy.Repository, Declaration: launch.declaration,
		Executor: launch.policy.ExecutorCommit, Nonce: request.Nonce, RunID: run, Attempt: uint32(attempt)}
	if request.Schema != frontRepairPreviewRequestSchema || !scope.valid() || runErr != nil || attemptErr != nil {
		return errors.New("Front preview does not name an exact pinned Actions run and request nonce")
	}
	recipient, err := frontRepairRecipient(os.Getenv("FUGUE_EDGE_FRONT_REPAIR_PREVIEW_RECIPIENT"))
	if err != nil {
		return err
	}
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(root, 60*time.Second)
	defer cancel()
	if err := requireFrontRepairCurrentDeclaration(ctx, launch, readFrontRepairGitHubCLI); err != nil {
		return err
	}
	if err := requireFrontRepairEnvironment(ctx, launch, readFrontRepairGitHubCLI); err != nil {
		return err
	}
	preview, err := captureFrontRepairPreview(ctx, defaultFrontRepairRegistry, launch.policy.Component)
	if err != nil {
		// Existing observation errors can contain private Node or Pod identity.
		return errors.New("read-only Front preview could not prove the exact retained release and full roster")
	}
	if err := requireFrontRepairCurrentDeclaration(ctx, launch, readFrontRepairGitHubCLI); err != nil {
		return err
	}
	sealed, err := sealFrontRepairPreview(scope, preview, recipient)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(sealed)
}

func runFrontRepairPreviewOpen(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("front-repair-preview-open", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	file := flags.String("envelope", "", "sealed artifact JSON from the verified protected job")
	key := flags.String("key", "", "local 0600 X25519 PKCS#8 PEM")
	var scope frontRepairPreviewSealScope
	flags.StringVar(&scope.Repository, "repository", "", "exact repository")
	flags.StringVar(&scope.Declaration, "declaration", "", "protected declaration commit")
	flags.StringVar(&scope.Executor, "executor", "", "approved Guardian commit")
	flags.StringVar(&scope.Nonce, "nonce", "", "exact request nonce")
	flags.Uint64Var(&scope.RunID, "run-id", 0, "verified Actions run")
	attempt := flags.Uint64("attempt", 0, "verified Actions attempt")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || flags.NFlag() != 8 || *attempt > uint64(^uint32(0)) || stdout == nil {
		return errors.New("Front preview open requires the local key and exact verified artifact scope")
	}
	scope.Attempt = uint32(*attempt)
	var sealed frontRepairSealedPreview
	if err := readFrontRepairReleaseInput(*file, "sealed preview", 1<<20, &sealed); err != nil {
		return err
	}
	f, err := os.Open(*key)
	if err != nil {
		return errors.New("cannot open the offline Front preview key file")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() < 1 || info.Size() > 1024 {
		return errors.New("offline Front preview key must be a bounded 0600 file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 1025))
	if err != nil || int64(len(raw)) != info.Size() {
		return errors.New("offline Front preview key could not be read")
	}
	defer clear(raw)
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(bytes.TrimSpace(rest)) != 0 {
		return errors.New("offline Front preview key must be a single PKCS#8 PEM")
	}
	defer clear(block.Bytes)
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	secret, ok := parsed.(*ecdh.PrivateKey)
	if err != nil || !ok || secret.Curve() != ecdh.X25519() {
		return errors.New("offline Front preview key is not X25519")
	}
	preview, err := openFrontRepairPreview(sealed, scope, secret)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(preview)
}
