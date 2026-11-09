package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestObservationRetentionAcceptsOnlyWriterBoundHeartbeatKinds(t *testing.T) {
	for _, target := range []string{"consumer-a", "shadow_consumer-a"} {
		event := model.AuditEvent{TargetID: target, ChainID: "platform-consumer-heartbeat:" + target, Action: "platform_consumer.heartbeat_accepted", TargetType: "platform_consumer"}
		if target == "shadow_consumer-a" {
			event.Action, event.TargetType = "platform_consumer.shadow_observed", "platform_shadow_consumer"
		}
		if !heartbeatRetentionEventAllowed(event, event.ChainID) {
			t.Fatal("normal writer event refused", target)
		}
		for name, mutate := range map[string]func(*model.AuditEvent){
			"business action":       func(e *model.AuditEvent) { e.Action = "app.delete" },
			"other target kind":     func(e *model.AuditEvent) { e.TargetType = "platform_artifact" },
			"other target identity": func(e *model.AuditEvent) { e.TargetID += "-other" },
			"other chain":           func(e *model.AuditEvent) { e.ChainID += "-other" },
			"missing target":        func(e *model.AuditEvent) { e.TargetID = "" },
			"cross consumer role": func(e *model.AuditEvent) {
				if target == "shadow_consumer-a" {
					e.Action, e.TargetType = "platform_consumer.heartbeat_accepted", "platform_consumer"
				} else {
					e.Action, e.TargetType = "platform_consumer.shadow_observed", "platform_shadow_consumer"
				}
			},
		} {
			t.Run(target+"/"+name, func(t *testing.T) {
				invalid := event
				mutate(&invalid)
				if heartbeatRetentionEventAllowed(invalid, event.ChainID) {
					t.Fatal("unrelated or incorrectly bound audit event accepted")
				}
			})
		}
	}
}

func TestObservationRetentionPostgresShadowWriterPreservesSignedSuffixAndHead(t *testing.T) {
	s := retentionPostgres(t)
	now := time.Now().UTC()
	keys := bundleauth.NewKeyring("synthetic-shadow-key", "key-a", "", "", nil)
	consumer := model.PlatformConsumerInstance{ID: "shadow_consumer-a", ConsumerID: "consumer-a", IdentityVerified: true, CredentialID: "credential-a", EvidenceHash: "evidence-a", Sequence: 1, IssuedAt: &now}
	chain := platformConsumerHeartbeatAuditChainID(consumer)
	var events []model.AuditEvent
	previous := ""
	for i := 1; i <= 5; i++ {
		consumer.LastHeartbeatAt = now.Add(-10 * 24 * time.Hour)
		if i == 5 {
			consumer.LastHeartbeatAt = now
		}
		event, err := buildPlatformConsumerHeartbeatAuditEvent(consumer, int64(i), previous, keys)
		if err != nil {
			t.Fatal(err)
		}
		metadata, _ := json.Marshal(event.Metadata)
		provenance, _ := json.Marshal(event.Provenance)
		if _, err = s.db.Exec(`INSERT INTO fugue_audit_events(id,actor_type,actor_id,action,target_type,target_id,metadata_json,chain_id,chain_sequence,previous_hash,event_hash,provenance_json,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, event.ID, event.ActorType, event.ActorID, event.Action, event.TargetType, event.TargetID, metadata, event.ChainID, event.ChainSequence, event.PreviousHash, event.EventHash, provenance, event.CreatedAt); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
		previous = event.EventHash
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_security_audit_chain_state VALUES($1,5,$2,$3)`, chain, previous, now); err != nil {
		t.Fatal(err)
	}
	p := testRetentionPolicy()
	prune := func() (int64, error) {
		t.Helper()
		conn, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		return pruneHeartbeatChain(context.Background(), conn, chain, p, keys, now)
	}
	for round := 1; round <= 2; round++ {
		deleted, err := prune()
		if err != nil || deleted != 2 {
			t.Fatal("bounded normal shadow history cleanup", round, deleted, err)
		}
		var raw string
		if err := s.db.QueryRow(`SELECT value FROM fugue_meta WHERE key=$1`, heartbeatRetentionAnchorPrefix+chain).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var anchor model.AuditEvent
		if json.Unmarshal([]byte(raw), &anchor) != nil || anchor.EventHash != events[round*2-1].EventHash || platformsafety.VerifyTamperEvidentAuditEvent(anchor, keys) != nil {
			t.Fatal("original signed shadow anchor not retained")
		}
		var head int64
		var headHash string
		if err := s.db.QueryRow(`SELECT last_sequence,last_hash FROM fugue_security_audit_chain_state WHERE chain_id=$1`, chain).Scan(&head, &headHash); err != nil || head != 5 || headHash != previous {
			t.Fatal("replay-protection head changed", err)
		}
	}
	var remaining, protected int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_audit_events),(SELECT count(*) FROM fugue_platform_artifacts)`).Scan(&remaining, &protected); err != nil || remaining != 1 || protected != 1 {
		t.Fatal("protected head or business artifact changed", remaining, protected, err)
	}
	deleted, err := prune()
	if err != nil || deleted != 0 {
		t.Fatal(fmt.Sprintf("protected head pruned: %v %v", deleted, err))
	}
}
