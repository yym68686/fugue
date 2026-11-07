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

	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func consumerTestContract() store.ConsumerInputPreparationContract {
	return store.ConsumerInputPreparationContract{Version: 1, Mode: "trusted", ExpectedSetID: "neutral-set", OperationKey: "neutral-consumer",
		Heartbeat:                      &platformcontrol.PlatformConsumerHeartbeatEnvelope{ExpectedConsumerSetID: "neutral-set", DesiredGeneration: "neutral-generation"},
		CompilerInputPreparationBudget: publicationTestContract().CompilerInputPreparationBudget}
}

func TestConsumerPreparationStrictRequestAndPurpose(t *testing.T) {
	c := consumerTestContract()
	raw, _ := json.Marshal(c)
	for _, mode := range []string{"trusted", "shadow"} {
		c.Mode = mode
		encoded, _ := json.Marshal(c)
		if _, err := decodeConsumerPreparationContract(bytes.NewReader(encoded)); err != nil {
			t.Fatal("valid purpose rejected", mode, err)
		}
	}
	for _, bad := range []string{string(raw) + `{}`, strings.TrimSuffix(string(raw), "}") + `,"guard":{}}`,
		strings.Replace(string(raw), `"heartbeat":{`, `"heartbeat":{"allow_initial_lkg":true,`, 1),
		strings.Replace(string(raw), `"sequence":0`, `"sequence":true`, 1), strings.Repeat(" ", (128<<10)+1) + string(raw)} {
		if _, err := decodeConsumerPreparationContract(strings.NewReader(bad)); err == nil {
			t.Fatal("untyped, unknown authority or oversized request accepted")
		}
	}
	for _, change := range []func(*store.ConsumerInputPreparationContract){
		func(c *store.ConsumerInputPreparationContract) { c.Mode = "retire" },
		func(c *store.ConsumerInputPreparationContract) { c.Mode = "register" },
		func(c *store.ConsumerInputPreparationContract) { c.ExpectedSetID = " other " },
		func(c *store.ConsumerInputPreparationContract) { c.ExpectedSetID = "different" },
		func(c *store.ConsumerInputPreparationContract) { c.MaxSeconds = 541 },
		func(c *store.ConsumerInputPreparationContract) { c.MaxInputs = 33 },
		func(c *store.ConsumerInputPreparationContract) { c.ExpectedPlanSHA256 = strings.Repeat("a", 64) },
	} {
		bad := consumerTestContract()
		change(&bad)
		encoded, _ := json.Marshal(bad)
		if _, err := decodeConsumerPreparationContract(bytes.NewReader(encoded)); err == nil {
			t.Fatal("ambiguous purpose or weakened bounds accepted")
		}
	}
}

func TestConsumerPreparationPublicTicketPinnedKeyAndStrictDecode(t *testing.T) {
	now := time.Now().UTC()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{10}, ed25519.SeedSize))
	key := base64.StdEncoding.EncodeToString(private.Public().(ed25519.PublicKey))
	c := consumerTestContract()
	c.ExpectedPlanSHA256 = strings.Repeat("a", 64)
	raw, _ := json.Marshal(map[string]any{"version": 1, "contract": c, "backend_hash": "sha256:" + strings.Repeat("b", 64), "closure_sha256": strings.Repeat("c", 64),
		"epoch": 0, "horizon": 1, "issued_at": now, "expires_at": now.Add(10 * time.Minute), "inputs": []any{},
		"tuples": []any{map[string]any{"table": "fugue_platform_expected_consumer_sets", "key": c.ExpectedSetID, "xmin": "1", "ctid": "(1,1)", "relation": 1, "filenode": 1}}, "signature": ""})
	var ticket store.ConsumerInputPreparationTicket
	if err := json.Unmarshal(raw, &ticket); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(ticket)
	ticket.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(private, raw))
	plan := store.ConsumerInputPreparationPlan{Ticket: ticket, VerificationKey: key}
	raw, _ = json.Marshal(plan)
	if _, err := decodeConsumerPreparationPlan(bytes.NewReader(raw), now); err != nil {
		t.Fatal("valid public plan", err)
	}
	for _, bad := range []string{string(raw) + `{}`, strings.TrimSuffix(string(raw), "}") + `,"private_key":"invented"}`,
		strings.Replace(string(raw), `"epoch":0`, `"epoch":1`, 1), strings.Repeat(" ", (513<<10)+1) + string(raw)} {
		if _, err := decodeConsumerPreparationPlan(strings.NewReader(bad), now); err == nil {
			t.Fatal("untrusted plan accepted")
		}
	}
	if _, err := decodeConsumerPreparationPlan(bytes.NewReader(raw), now.Add(10*time.Minute)); err == nil {
		t.Fatal("expired ticket accepted")
	}
	path := filepath.Join(t.TempDir(), "consumer-plan.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	for _, anchor := range []string{"", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{11}, 32))} {
		t.Setenv("FUGUE_CONSUMER_INPUT_PREPARATION_VERIFICATION_KEY", anchor)
		if _, err := runConsumerPreparation(t.Context(), "consumer-input-restore", "", path, ""); !errors.Is(err, store.ErrConflict) {
			t.Fatal("unconfigured or different worker key accepted", err)
		}
	}
}
