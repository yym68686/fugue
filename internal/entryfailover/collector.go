package entryfailover

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	sc "fugue/internal/staticedgecontract"
)

type ProbeEnvelope struct {
	Schema       string      `json:"schema"`
	PolicyDigest string      `json:"policy_digest"`
	Nonce        string      `json:"nonce"`
	VantageID    string      `json:"vantage_id"`
	Result       ProbeResult `json:"result"`
}

type ProbeRPCRequest struct {
	Schema       string       `json:"schema"`
	SignedPolicy SignedPolicy `json:"signed_policy"`
	TargetID     string       `json:"target_id"`
	VantageID    string       `json:"vantage_id"`
	Nonce        string       `json:"nonce"`
}

type SSHCollector struct {
	Local        Prober
	SignedPolicy SignedPolicy
}

func (c SSHCollector) Collect(ctx context.Context, policy Policy, target Target, vantage Vantage) (ProbeResult, error) {
	if c.SignedPolicy.Digest != policy.Digest() {
		return ProbeResult{}, errors.New("collector policy differs from signed policy")
	}
	if vantage.Transport == "local" {
		result := c.Local.Probe(ctx, policy, target)
		if ctx.Err() != nil {
			return ProbeResult{}, ctx.Err()
		}
		return result, nil
	}
	if vantage.Transport != "ssh" || !identifier.MatchString(vantage.SSHHost) || !identifier.MatchString(vantage.ID) {
		return ProbeResult{}, errors.New("invalid remote vantage")
	}
	nonceRaw := make([]byte, 16)
	if _, err := rand.Read(nonceRaw); err != nil {
		return ProbeResult{}, err
	}
	nonce := hex.EncodeToString(nonceRaw)
	policyBytes, err := json.Marshal(ProbeRPCRequest{Schema: PolicySchema, SignedPolicy: c.SignedPolicy,
		TargetID: target.ID, VantageID: vantage.ID, Nonce: nonce})
	if err != nil {
		return ProbeResult{}, err
	}
	remoteCtx, cancel := context.WithTimeout(ctx, min(policy.Interval(), 30*time.Second))
	defer cancel()
	args := []string{"-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes", "-o", "ForwardAgent=no", "-o", "ClearAllForwardings=yes", "-o", "ConnectTimeout=5", "-T", "--", vantage.SSHHost}
	cmd := exec.CommandContext(remoteCtx, "ssh", args...)
	cmd.Stdin = bytes.NewReader(policyBytes)
	var stdout boundedOutput
	cmd.Stdout = &stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return ProbeResult{}, fmt.Errorf("remote vantage %s unavailable: %w", vantage.ID, err)
	}
	var envelope ProbeEnvelope
	if err := sc.StrictJSON(stdout.Bytes(), &envelope); err != nil {
		return ProbeResult{}, err
	}
	if envelope.Schema != PolicySchema || envelope.PolicyDigest != policy.Digest() || envelope.Nonce != nonce || envelope.VantageID != vantage.ID || envelope.Result.TargetID != target.ID {
		return ProbeResult{}, errors.New("remote probe identity, nonce or policy mismatch")
	}
	return envelope.Result, nil
}

type boundedOutput struct{ bytes.Buffer }

func (b *boundedOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 64<<10 {
		return 0, errors.New("remote probe output exceeds 64 KiB")
	}
	return b.Buffer.Write(p)
}

// ProbeFromSignedPolicy is the SSH-only, read-only operation on a remote
// vantage. It receives no Cloudflare credential or DNS write capability.
func ProbeFromSignedPolicy(ctx context.Context, raw []byte, verificationKey []byte, targetID, vantageID, nonce string) (ProbeEnvelope, error) {
	if !identifier.MatchString(targetID) || !identifier.MatchString(vantageID) || len(nonce) != 32 || strings.Trim(nonce, "0123456789abcdef") != "" {
		return ProbeEnvelope{}, errors.New("invalid probe target, vantage or nonce")
	}
	key, err := sc.ParsePublicKeyPEM(verificationKey)
	if err != nil {
		return ProbeEnvelope{}, err
	}
	var decoded SignedPolicy
	if err = sc.StrictJSON(raw, &decoded); err != nil {
		return ProbeEnvelope{}, err
	}
	signed, err := ParseSignedPolicy(raw, map[string]ed25519.PublicKey{decoded.KeyID: key})
	if err != nil {
		return ProbeEnvelope{}, err
	}
	var target Target
	found := false
	for _, t := range signed.Policy.Targets {
		if t.ID == targetID {
			target, found = t, true
			break
		}
	}
	if !found {
		return ProbeEnvelope{}, errors.New("probe target outside signed policy")
	}
	validVantage := false
	for _, v := range signed.Policy.Vantages {
		if v.ID == vantageID && v.Transport == "ssh" {
			validVantage = true
			break
		}
	}
	if !validVantage {
		return ProbeEnvelope{}, errors.New("vantage outside signed policy")
	}
	probeCtx, cancel := context.WithTimeout(ctx, min(signed.Policy.Interval(), 30*time.Second))
	defer cancel()
	result := (Prober{}).Probe(probeCtx, signed.Policy, target)
	if probeCtx.Err() != nil {
		return ProbeEnvelope{}, errors.New("probe deadline exceeded; result is unknown")
	}
	return ProbeEnvelope{Schema: PolicySchema, PolicyDigest: signed.Digest, Nonce: nonce, VantageID: vantageID, Result: result}, nil
}

// ProbeRPC accepts one bounded signed request. It is suitable for a fixed SSH
// authorized_keys command and has no shell, DNS write or Fugue API capability.
func ProbeRPC(ctx context.Context, raw, verificationKey []byte) (ProbeEnvelope, error) {
	if len(raw) > 64<<10 {
		return ProbeEnvelope{}, errors.New("probe request exceeds size bound")
	}
	var request ProbeRPCRequest
	if err := sc.StrictJSON(raw, &request); err != nil {
		return ProbeEnvelope{}, err
	}
	if request.Schema != PolicySchema {
		return ProbeEnvelope{}, errors.New("probe request schema mismatch")
	}
	signedRaw, err := json.Marshal(request.SignedPolicy)
	if err != nil {
		return ProbeEnvelope{}, err
	}
	return ProbeFromSignedPolicy(ctx, signedRaw, verificationKey, request.TargetID, request.VantageID, request.Nonce)
}
