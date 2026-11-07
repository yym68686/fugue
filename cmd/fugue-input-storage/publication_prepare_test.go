package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func publicationTestContract() store.PublicationInputPreparationContract {
	return store.PublicationInputPreparationContract{Version: 1, Mode: "release", ArtifactID: "neutral-artifact", OperationKey: "neutral-publication",
		Request:                        model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"},
		CompilerInputPreparationBudget: store.CompilerInputPreparationBudget{MaxInputs: 2, MaxRawBytes: 2048, MaxStoredBytes: 2048, MaxGetBytes: 65536, MaxGetRequests: 2, MaxSeconds: 30}}
}

func TestPublicationPreparationContractStrictIdentityAndEnvelope(t *testing.T) {
	c := publicationTestContract()
	raw, _ := json.Marshal(c)
	if _, err := decodePublicationPreparationContract(bytes.NewReader(raw)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{string(raw) + `{}`, strings.TrimSuffix(string(raw), "}") + `,"guard":{}}`, strings.Repeat(" ", (128<<10)+1) + string(raw)} {
		if _, err := decodePublicationPreparationContract(strings.NewReader(bad)); err == nil {
			t.Fatal("unknown authority field or oversized input accepted")
		}
	}
	for _, change := range []func(*store.PublicationInputPreparationContract){
		func(c *store.PublicationInputPreparationContract) { c.Mode = "retire" },
		func(c *store.PublicationInputPreparationContract) { c.ReleaseID = "unrelated" },
		func(c *store.PublicationInputPreparationContract) { c.ExpectedPlanSHA256 = strings.Repeat("a", 64) },
		func(c *store.PublicationInputPreparationContract) { c.MaxSeconds = 541 },
		func(c *store.PublicationInputPreparationContract) { c.MaxInputs = 33 },
		func(c *store.PublicationInputPreparationContract) { c.MaxStoredBytes = 129 << 20 },
		func(c *store.PublicationInputPreparationContract) { c.MaxGetRequests = 32769 },
		func(c *store.PublicationInputPreparationContract) { c.Request.Reason = strings.Repeat("x", 65536) },
	} {
		bad := c
		change(&bad)
		raw, _ := json.Marshal(bad)
		if _, err := decodePublicationPreparationContract(bytes.NewReader(raw)); err == nil {
			t.Fatal("invalid preparation intent accepted")
		}
	}
	for _, mode := range []string{"rollback", "lkg"} {
		c := publicationTestContract()
		c.Mode, c.Request = mode, model.PlatformArtifactReleaseRequest{}
		if mode == "rollback" {
			c.RollbackGeneration = "neutral-generation"
		} else {
			c.ArtifactID, c.ReleaseID = "", "neutral-release"
		}
		if c.Validate(false) != nil {
			t.Fatal("valid exact purpose rejected", mode)
		}
		c.Request.ReleaseChannel = "full"
		if c.Validate(false) == nil {
			t.Fatal("ambiguous cross-purpose request accepted")
		}
	}
}

func TestPublicationPreparationProducerOriginalArgumentsStrictDecoder(t *testing.T) {
	for _, action := range []string{"shadow", "gray", "full", "rollback", "verify", "verify-recovery"} {
		t.Run(action, func(t *testing.T) {
			c := publicationTestContract()
			p := &store.ProducerInputPreparationIntent{Action: action, PolicyReleaseID: "neutral-policy"}
			c.Producer = p
			switch action {
			case "shadow":
				c.Request.Reason, c.Request.IdempotencyKey = "versioned platform configuration producer", "producer/neutral-policy/neutral-artifact"
			case "gray", "full":
				c.Request = model.PlatformArtifactReleaseRequest{ReleaseChannel: action, Reason: "versioned platform configuration serving producer", IdempotencyKey: "producer/neutral-policy/" + action + "/neutral-artifact"}
				p.PreviousFullReleaseID, p.BaselineArtifactID = "neutral-full", "neutral-baseline"
			case "rollback":
				c.Mode, c.RollbackGeneration, c.Request = "rollback", "neutral-generation", model.PlatformArtifactReleaseRequest{}
				p.PreviousFullReleaseID, p.BaselineArtifactID, p.FailedReleaseID = "neutral-full", "neutral-baseline", "neutral-failed"
				p.Rollback = &model.PlatformArtifactRollbackRequest{ReleaseChannel: "full", ToGeneration: c.RollbackGeneration, Reason: "neutral recovery"}
			default:
				c.Mode, c.ArtifactID, c.ReleaseID, c.Request = "lkg", "", "neutral-release", model.PlatformArtifactReleaseRequest{}
				p.BaselineArtifactID = "neutral-baseline"
				p.Verification = &model.PlatformArtifactVerifyLKGRequest{FencingToken: 7, Reason: "neutral verification"}
			}
			raw, _ := json.Marshal(c)
			if _, err := decodePublicationPreparationContract(bytes.NewReader(raw)); err != nil {
				t.Fatal("original producer intent rejected", err)
			}
			for _, mutation := range []string{"guard", "business-authority", "wrong-purpose"} {
				var bad map[string]any
				if err := json.Unmarshal(raw, &bad); err != nil {
					t.Fatal(err)
				}
				args := bad["producer"].(map[string]any)
				switch mutation {
				case "guard":
					args["guard"] = map[string]any{}
				case "business-authority":
					if action == "rollback" {
						args["rollback"].(map[string]any)["force_publish"] = true
					} else if c.Mode == "lkg" {
						args["verification"].(map[string]any)["allow_initial_lkg"] = true
					} else {
						bad["request"].(map[string]any)["idempotency_key"] = "unrelated"
					}
				case "wrong-purpose":
					args["action"] = "retire"
				}
				encoded, _ := json.Marshal(bad)
				if _, err := decodePublicationPreparationContract(bytes.NewReader(encoded)); err == nil {
					t.Fatal("raw authority or changed original workflow accepted", mutation)
				}
			}
		})
	}
}

func TestPublicationPreparationPublicPlanStrictDecoderAndPinnedKey(t *testing.T) {
	now := time.Now().UTC()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{9}, ed25519.SeedSize))
	key := base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
	c := publicationTestContract()
	c.ExpectedPlanSHA256 = strings.Repeat("a", 64)
	raw, _ := json.Marshal(map[string]any{"version": 1, "contract": c, "backend_hash": "sha256:" + strings.Repeat("b", 64), "closure_sha256": strings.Repeat("c", 64),
		"epoch": 0, "horizon": 1, "issued_at": now, "expires_at": now.Add(10 * time.Minute), "inputs": []any{},
		"tuples": []any{map[string]any{"table": "fugue_platform_artifacts", "key": c.ArtifactID, "xmin": "1", "ctid": "(1,1)", "relation": 1, "filenode": 1}}, "signature": ""})
	var ticket store.PublicationInputPreparationTicket
	if err := json.Unmarshal(raw, &ticket); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(ticket)
	ticket.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw))
	plan := store.PublicationInputPreparationPlan{Ticket: ticket, VerificationKey: key}
	raw, _ = json.Marshal(plan)
	if _, err := decodePublicationPreparationPlan(bytes.NewReader(raw), now); err != nil {
		t.Fatal("valid public plan", err)
	}
	for _, bad := range []string{string(raw) + `{}`, strings.TrimSuffix(string(raw), "}") + `,"guard":{}}`, strings.Replace(string(raw), `"epoch":0`, `"epoch":1`, 1), strings.Repeat(" ", (513<<10)+1) + string(raw)} {
		if _, err := decodePublicationPreparationPlan(strings.NewReader(bad), now); err == nil {
			t.Fatal("non-strict public plan accepted")
		}
	}
	if _, err := decodePublicationPreparationPlan(bytes.NewReader(raw), now.Add(10*time.Minute)); err == nil {
		t.Fatal("expired ticket accepted")
	}
	// A caller-provided verification key is not accepted as the worker trust
	// anchor. This path must refuse before opening a DB or an object reader.
	t.Setenv("FUGUE_PUBLICATION_INPUT_PREPARATION_VERIFICATION_KEY", "")
	path := filepath.Join(t.TempDir(), "public-plan.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runPublicationPreparation(t.Context(), "publication-input-restore", "", path, ""); !errors.Is(err, store.ErrConflict) {
		t.Fatal("unconfigured worker accepted")
	}
}
