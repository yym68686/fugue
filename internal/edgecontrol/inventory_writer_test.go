package edgecontrol

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestGroupInventoryHeartbeatAuthenticatesAndAdvancesExactGroupCAS(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 16, 0, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	stateDir := privateStateDir(t)
	store, err := OpenPersistentGroupStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := "edge-us-node-1"
	keyringDir, token := writeInventoryPlatformIdentityGroupFixture(t, now, groupID, nodeID)
	if _, err := loadInventoryPlatformIdentityKeyring(filepath.Join(keyringDir, groupID+".json"), "edge-group-country-de"); err == nil {
		t.Fatal("US inventory platform identity keyring was accepted for DE")
	}
	handler, err := NewGroupInventoryHeartbeatHandler(GroupInventoryHeartbeatHandlerConfig{
		Store: store, GroupIDs: []string{groupID}, KeyringDir: keyringDir,
		Authority: "edge-control", PublicationEnabled: true, Path: GroupAuthorityInventoryHeartbeatPathV1, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-1", true)
	inventory.ObservedAt = now
	inventory.Generation = ProducerInventoryEnvelopeGeneration(1)
	inventory.Instances = inventory.Instances[len(inventory.Instances)-1:]
	inventory.Instances[0].EdgeID = nodeID
	envelope := GroupInventoryHeartbeat{
		Schema: GroupInventoryHeartbeatSchemaV1, GroupID: groupID, FaultDomainID: inventory.FaultDomainID, EdgePoolID: inventory.EdgePoolID, ProducerNodeID: nodeID, ProducerGeneration: 1,
		ExpectedSequence: 0, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(),
		Nonce: "heartbeat-us-00000001", Inventory: inventory,
	}
	queryBody, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	queryRecorder := httptest.NewRecorder()
	queryRequest := httptest.NewRequest(http.MethodPost, GroupAuthorityInventoryHeartbeatPathV1+"?token=forbidden", bytesReader(queryBody))
	queryRequest.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(queryRecorder, queryRequest)
	if queryRecorder.Code != http.StatusNotFound {
		t.Fatalf("query credential status=%d body=%s", queryRecorder.Code, queryRecorder.Body.String())
	}
	if _, err := store.ReadGroupInventory(context.Background(), groupID); !errors.Is(err, ErrGroupInventoryNotFound) {
		t.Fatalf("query credential changed inventory: %v", err)
	}
	recorder := performAuthorityInventoryHeartbeat(t, handler, envelope, token)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var receipt GroupInventoryHeartbeatReceipt
	if err := json.Unmarshal(recorder.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Schema != GroupInventoryHeartbeatReceiptSchemaV1 || receipt.GroupID != groupID || receipt.Sequence != 1 || receipt.Generation == inventory.Generation || receipt.InventoryDigest == "" ||
		receipt.Authority != "edge-control" || !receipt.Publication || receipt.ProducerNodeID != nodeID || receipt.ProducerGeneration != 1 {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	stored, err := store.ReadGroupInventory(context.Background(), groupID)
	if err != nil || stored.Sequence != 1 || stored.Generation != receipt.Generation {
		t.Fatalf("stored inventory = %+v, %v", stored, err)
	}
	restarted, err := OpenPersistentGroupStore(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	producer, exists, err := restarted.ReadGroupInventoryProducerState(context.Background(), groupID)
	if err != nil || !exists || producer.Generation != 1 || len(producer.Observations) != 1 || producer.Observations[0].NodeID != nodeID {
		t.Fatalf("restarted producer cursor=%+v exists=%t err=%v", producer, exists, err)
	}

	replay := performAuthorityInventoryHeartbeat(t, handler, envelope, token)
	if replay.Code != http.StatusConflict {
		t.Fatalf("replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	stored, err = store.ReadGroupInventory(context.Background(), groupID)
	if err != nil || stored.Sequence != 1 {
		t.Fatalf("replay advanced inventory = %+v, %v", stored, err)
	}
}

func TestAuthorityInventoryProducerRejectsCrossGroupReplayAndIsolatesStaleGroup(t *testing.T) {
	t.Parallel()

	current := time.Date(2026, 8, 4, 18, 0, 0, 0, time.UTC)
	groups := []string{"edge-group-country-de", "edge-group-country-us"}
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	type producerFixture struct {
		handler http.Handler
		token   string
		nodeID  string
	}
	fixtures := map[string]producerFixture{}
	for _, groupID := range groups {
		nodeID := "node-" + groupID
		keyringDir, token := writeInventoryPlatformIdentityGroupFixture(t, current, groupID, nodeID)
		handler, buildErr := NewGroupInventoryHeartbeatHandler(GroupInventoryHeartbeatHandlerConfig{
			Store: store, GroupIDs: []string{groupID}, KeyringDir: keyringDir,
			Authority: "edge-control", PublicationEnabled: true, Path: GroupAuthorityInventoryHeartbeatPathV1,
			Now: func() time.Time { return current },
		})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		fixtures[groupID] = producerFixture{handler: handler, token: token, nodeID: nodeID}
		heartbeat := authorityInventoryHeartbeatFixture(groupID, nodeID, 0, 1, current, "heartbeat-initial-"+groupID)
		if recorder := performAuthorityInventoryHeartbeat(t, handler, heartbeat, token); recorder.Code != http.StatusCreated {
			t.Fatalf("%s initial status=%d body=%s", groupID, recorder.Code, recorder.Body.String())
		}
	}

	de := fixtures[groups[0]]
	deReplay := authorityInventoryHeartbeatFixture(groups[0], de.nodeID, 0, 1, current, "heartbeat-initial-"+groups[0])
	if recorder := performAuthorityInventoryHeartbeat(t, de.handler, deReplay, de.token); recorder.Code != http.StatusConflict {
		t.Fatalf("DE replay status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	us := fixtures[groups[1]]
	crossGroup := authorityInventoryHeartbeatFixture(groups[1], us.nodeID, 1, 2, current, "heartbeat-cross-group-0001")
	if recorder := performAuthorityInventoryHeartbeat(t, us.handler, crossGroup, de.token); recorder.Code != http.StatusUnauthorized {
		t.Fatalf("cross-group identity status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	inactiveSlot := authorityInventoryHeartbeatFixture(groups[1], us.nodeID, 1, 2, current, "heartbeat-inactive-slot-01")
	inactiveSlot.Inventory.Instances[0].Slot = model.EdgeSlotA
	if recorder := performAuthorityInventoryHeartbeat(t, us.handler, inactiveSlot, us.token); recorder.Code != http.StatusForbidden {
		t.Fatalf("inactive-slot producer status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	current = current.Add(3 * time.Minute)
	usAdvance := authorityInventoryHeartbeatFixture(groups[1], us.nodeID, 1, 2, current, "heartbeat-us-fresh-000002")
	if recorder := performAuthorityInventoryHeartbeat(t, us.handler, usAdvance, us.token); recorder.Code != http.StatusCreated {
		t.Fatalf("US advance status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	deInventory, err := store.ReadGroupInventory(context.Background(), groups[0])
	if err != nil || deInventory.Sequence != 1 {
		t.Fatalf("DE replay/cross-group request changed state: %+v err=%v", deInventory, err)
	}
	usInventory, err := store.ReadGroupInventory(context.Background(), groups[1])
	if err != nil || usInventory.Sequence != 2 {
		t.Fatalf("US independent producer did not advance: %+v err=%v", usInventory, err)
	}

	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return current }, InventoryMaxAge: GroupInventoryHeartbeatMaxAge}
	batch, err := compiler.Reconcile(context.Background(), routeIntentFixture(), groups)
	if err != nil || batch.Succeeded != 1 || batch.Failed != 1 {
		t.Fatalf("stale group isolation batch=%+v err=%v", batch, err)
	}
	results := shadowResultsByGroup(batch.Results)
	if results[groups[0]].FailureCode != GroupShadowFailureInventoryInvalid || results[groups[1]].Status != GroupShadowStatusCompiled {
		t.Fatalf("stale DE contaminated fresh US: %+v", results)
	}
}

func TestAuthorityInventoryProducerPreservesLegacyGroupLKGUntilThatGroupReports(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)
	groups := []string{"edge-group-country-de", "edge-group-country-us"}
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, groupID := range groups {
		inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "legacy-"+groupID+"-b", "legacy-"+groupID+"-1", false)
		if err := store.StoreGroupInventoryCAS(ctx, groupID, 0, inventory); err != nil {
			t.Fatal(err)
		}
	}
	signer := &fixtureGroupSigner{keys: map[string][]byte{
		groups[0]: bytes.Repeat([]byte{0x51}, 32), groups[1]: bytes.Repeat([]byte{0x52}, 32),
	}, validFor: time.Hour}
	compiler := GroupShadowCompiler{Inventory: store, Ledger: store, Now: func() time.Time { return now }, InventoryMaxAge: GroupInventoryHeartbeatMaxAge}
	publisher := GroupAuthorityPublisher{Store: store, Signer: signer, Now: func() time.Time { return now }}
	initial, err := compiler.Reconcile(ctx, routeIntentFixture(), groups)
	if err != nil {
		t.Fatal(err)
	}
	if published, err := publisher.Publish(ctx, initial); err != nil || published.Published != 2 {
		t.Fatalf("initial legacy publication=%+v err=%v", published, err)
	}
	deBefore, err := store.ReadGroupAuthority(ctx, groups[0])
	if err != nil || !deBefore.PublishedExists {
		t.Fatalf("legacy DE publication=%+v err=%v", deBefore, err)
	}

	now = now.Add(3 * time.Minute)
	usNodeID := "edge-us-node-1"
	keyringDir, token := writeInventoryPlatformIdentityGroupFixture(t, now, groups[1], usNodeID)
	handler, err := NewGroupInventoryHeartbeatHandler(GroupInventoryHeartbeatHandlerConfig{
		Store: store, GroupIDs: []string{groups[1]}, KeyringDir: keyringDir,
		Authority: "edge-control", PublicationEnabled: true, Path: GroupAuthorityInventoryHeartbeatPathV1,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := authorityInventoryHeartbeatFixture(groups[1], usNodeID, 1, 1, now, "heartbeat-us-cutover-0001")
	if recorder := performAuthorityInventoryHeartbeat(t, handler, heartbeat, token); recorder.Code != http.StatusCreated {
		t.Fatalf("US producer cutover status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, exists, err := store.ReadGroupInventoryProducerState(ctx, groups[0]); err != nil || exists {
		t.Fatalf("unreported legacy DE gained a producer cursor: exists=%t err=%v", exists, err)
	}

	intent := routeIntentFixture()
	intent.Generation = "route-intents-legacy-cutover-2"
	intent.Routes[0].Generation = "route-all-legacy-cutover-2"
	compiled, err := compiler.Reconcile(ctx, intent, groups)
	if err != nil || compiled.Succeeded != 1 || compiled.Failed != 1 {
		t.Fatalf("mixed legacy/producer reconcile=%+v err=%v", compiled, err)
	}
	resultByGroup := shadowResultsByGroup(compiled.Results)
	if resultByGroup[groups[0]].FailureCode != GroupShadowFailureInventoryInvalid || resultByGroup[groups[1]].Status != GroupShadowStatusCompiled {
		t.Fatalf("legacy DE contaminated reporting US: %+v", resultByGroup)
	}
	published, err := publisher.Publish(ctx, compiled)
	if err != nil || published.Published != 1 || published.Failed != 1 {
		t.Fatalf("mixed legacy/producer publication=%+v err=%v", published, err)
	}
	deAfter, err := store.ReadGroupAuthority(ctx, groups[0])
	if err != nil || !deAfter.PublishedExists || deAfter.Published.Digest != deBefore.Published.Digest || deAfter.LedgerHead.Status != GroupAuthorityStatusFailed {
		t.Fatalf("unreported legacy DE did not preserve its LKG: before=%+v after=%+v err=%v", deBefore, deAfter, err)
	}
}

func TestGroupInventoryHeartbeatFailsClosedBeforeStore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 16, 15, 0, 0, time.UTC)
	groupID := "edge-group-country-de"
	for _, test := range []struct {
		name   string
		mutate func(*GroupInventoryHeartbeat)
	}{
		{name: "expired", mutate: func(value *GroupInventoryHeartbeat) { value.ExpiresAtUnix = now.Add(-time.Second).Unix() }},
		{name: "long ttl", mutate: func(value *GroupInventoryHeartbeat) { value.ExpiresAtUnix = now.Add(3 * time.Minute).Unix() }},
		{name: "unowned group", mutate: func(value *GroupInventoryHeartbeat) {
			value.GroupID = "edge-group-country-jp"
			value.Inventory.GroupID = value.GroupID
			value.Inventory.ActiveEpoch.GroupID = value.GroupID
		}},
		{name: "tampered inventory", mutate: func(value *GroupInventoryHeartbeat) { value.Inventory.Generation = "tampered" }},
		{name: "wrong key", mutate: func(value *GroupInventoryHeartbeat) { value.KeyID = "writer-unknown" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := OpenPersistentGroupStore(privateStateDir(t))
			if err != nil {
				t.Fatal(err)
			}
			keyFile, secret := writeInventoryWriterKeyringFixture(t, now, false)
			handler, err := NewGroupInventoryHeartbeatHandler(GroupInventoryHeartbeatHandlerConfig{
				Store: store, GroupIDs: []string{groupID}, KeyringFile: keyFile, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			inventory := groupInventoryFixture(groupID, model.EdgeSlotA, "epoch-de-a", "inventory-de-1", false)
			envelope := GroupInventoryHeartbeat{
				Schema: GroupInventoryHeartbeatSchemaV1, KeyID: "writer-current", GroupID: groupID,
				ExpectedSequence: 0, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(),
				Nonce: "heartbeat-de-00000001", Inventory: inventory,
			}
			if err := SignGroupInventoryHeartbeat(&envelope, secret); err != nil {
				t.Fatal(err)
			}
			test.mutate(&envelope)
			recorder := performInventoryHeartbeat(t, handler, envelope)
			if recorder.Code < 400 || recorder.Code >= 500 {
				t.Fatalf("unsafe heartbeat status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if _, err := store.ReadGroupInventory(context.Background(), groupID); err != ErrGroupInventoryNotFound {
				t.Fatalf("rejected heartbeat changed store: %v", err)
			}
		})
	}
}

func TestGroupInventoryHeartbeatKeyringProjectionRotatesAndRevokes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 4, 16, 30, 0, 0, time.UTC)
	groupID := "edge-group-country-us"
	store, err := OpenPersistentGroupStore(privateStateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	firstSecret := []byte("0123456789abcdef0123456789abcdef")
	secondSecret := []byte("abcdef0123456789abcdef0123456789")
	writeInventoryKeyringVersion(t, root, "..v1", now, "writer-first", firstSecret, false)
	writeInventoryKeyringVersion(t, root, "..v2", now, "writer-second", secondSecret, false)
	if err := os.Symlink("..v1", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..data/keyring.json", filepath.Join(root, "keyring.json")); err != nil {
		t.Fatal(err)
	}
	handler, err := NewGroupInventoryHeartbeatHandler(GroupInventoryHeartbeatHandlerConfig{
		Store: store, GroupIDs: []string{groupID}, KeyringFile: filepath.Join(root, "keyring.json"), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	makeEnvelope := func(sequence uint64, keyID string, secret []byte) GroupInventoryHeartbeat {
		inventory := groupInventoryFixture(groupID, model.EdgeSlotB, "epoch-us-b", "inventory-us-"+keyID, false)
		inventory.Sequence = sequence
		value := GroupInventoryHeartbeat{Schema: GroupInventoryHeartbeatSchemaV1, KeyID: keyID, GroupID: groupID, FaultDomainID: inventory.FaultDomainID, EdgePoolID: inventory.EdgePoolID, ExpectedSequence: sequence - 1,
			IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(), Nonce: "heartbeat-rotation-" + keyID, Inventory: inventory}
		if err := SignGroupInventoryHeartbeat(&value, secret); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if recorder := performInventoryHeartbeat(t, handler, makeEnvelope(1, "writer-first", firstSecret)); recorder.Code != http.StatusCreated {
		t.Fatalf("first key status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := os.Remove(filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..v2", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if recorder := performInventoryHeartbeat(t, handler, makeEnvelope(2, "writer-first", firstSecret)); recorder.Code != http.StatusUnauthorized {
		t.Fatalf("rotated-out key status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder := performInventoryHeartbeat(t, handler, makeEnvelope(2, "writer-second", secondSecret)); recorder.Code != http.StatusCreated {
		t.Fatalf("rotated key status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func performInventoryHeartbeat(t *testing.T, handler http.Handler, envelope GroupInventoryHeartbeat) *httptest.ResponseRecorder {
	return performInventoryHeartbeatAtPath(t, handler, envelope, GroupInventoryHeartbeatPathV1)
}

func performInventoryHeartbeatAtPath(t *testing.T, handler http.Handler, envelope GroupInventoryHeartbeat, path string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, bytesReader(body))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recorder, request)
	return recorder
}

func performAuthorityInventoryHeartbeat(t *testing.T, handler http.Handler, envelope GroupInventoryHeartbeat, token string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, GroupAuthorityInventoryHeartbeatPathV1, bytesReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	handler.ServeHTTP(recorder, request)
	return recorder
}

func authorityInventoryHeartbeatFixture(groupID, nodeID string, expectedSequence, producerGeneration uint64, now time.Time, nonce string) GroupInventoryHeartbeat {
	slot := model.EdgeSlotB
	releaseEpoch := "release-" + groupID
	faultDomainID := "fault-domain-primary"
	edgePoolID := "edge-pool-public"
	return GroupInventoryHeartbeat{
		Schema: GroupInventoryHeartbeatSchemaV1, GroupID: groupID, ProducerNodeID: nodeID, ProducerGeneration: producerGeneration,
		FaultDomainID: faultDomainID, EdgePoolID: edgePoolID,
		ExpectedSequence: expectedSequence, IssuedAtUnix: now.Unix(), ExpiresAtUnix: now.Add(time.Minute).Unix(), Nonce: nonce,
		Inventory: GroupInventorySnapshot{
			Schema: GroupInventorySchemaV1, GroupID: groupID, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Sequence: expectedSequence + 1,
			Generation: ProducerInventoryEnvelopeGeneration(producerGeneration), ObservedAt: now,
			ActiveEpoch: GroupActiveEpoch{GroupID: groupID, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Slot: slot, ReleaseEpoch: releaseEpoch, FenceSequence: 7, MinHealthyInstances: 1},
			Instances: []GroupInstance{{
				EdgeID: nodeID, GroupID: groupID, FaultDomainID: faultDomainID, EdgePoolID: edgePoolID, Slot: slot, InstanceUID: "uid-" + nodeID, ReleaseEpoch: releaseEpoch,
				EffectiveHealthy: true, NodeHealthy: true, NodeStatus: model.EdgeHealthHealthy,
			}},
		},
	}
}

func TestInventoryTopologyRequiresCompleteCanonicalPair(t *testing.T) {
	t.Parallel()

	for _, value := range []struct {
		faultDomainID string
		edgePoolID    string
	}{
		{},
		{faultDomainID: "fault-domain-primary"},
		{edgePoolID: "edge-pool-public"},
		{faultDomainID: "Country/DE", edgePoolID: "edge-pool-public"},
	} {
		if err := validateInventoryTopology(value.faultDomainID, value.edgePoolID); err == nil {
			t.Fatalf("invalid topology pair was accepted: %+v", value)
		}
	}
	if err := validateInventoryTopology("fault-domain-primary", "edge-pool-public"); err != nil {
		t.Fatalf("canonical topology pair was rejected: %v", err)
	}
}

func TestAuthorityBootstrapEligibilityIsExactlyBoundAndNeverServingHealthy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	groupID := "edge-group-neutral-bootstrap"
	nodeID := "edge-bootstrap-1"
	value := authorityInventoryHeartbeatFixture(groupID, nodeID, 0, 7, now, "bootstrap-binding-0001")
	serving := false
	instance := &value.Inventory.Instances[0]
	instance.EffectiveHealthy = false
	instance.ServingHealthy = &serving
	instance.BootstrapEligibility = &GroupBootstrapEligibility{
		GroupID: groupID, ReleaseEpoch: instance.ReleaseEpoch, ProducerGeneration: value.ProducerGeneration,
		ValidUntil: time.Unix(value.ExpiresAtUnix, 0).UTC(),
	}
	identity := GroupInventoryProducerIdentity{NodeID: nodeID, GroupID: groupID}
	if err := validateAuthorityInventoryProducerHeartbeat(value, identity, now); err != nil {
		t.Fatalf("valid bootstrap eligibility: %v", err)
	}
	mutations := []func(*GroupInventoryHeartbeat){
		func(v *GroupInventoryHeartbeat) {
			v.Inventory.Instances[0].BootstrapEligibility.GroupID = "edge-group-other"
		},
		func(v *GroupInventoryHeartbeat) {
			v.Inventory.Instances[0].BootstrapEligibility.ReleaseEpoch = "other-epoch"
		},
		func(v *GroupInventoryHeartbeat) { v.Inventory.Instances[0].BootstrapEligibility.ProducerGeneration++ },
		func(v *GroupInventoryHeartbeat) {
			v.Inventory.Instances[0].BootstrapEligibility.ValidUntil = now.Add(30 * time.Second)
		},
		func(v *GroupInventoryHeartbeat) {
			trueValue := true
			v.Inventory.Instances[0].ServingHealthy = &trueValue
		},
	}
	for index, mutate := range mutations {
		candidate := value
		candidate.Inventory.Instances = append([]GroupInstance(nil), value.Inventory.Instances...)
		eligibility := *value.Inventory.Instances[0].BootstrapEligibility
		candidate.Inventory.Instances[0].BootstrapEligibility = &eligibility
		mutate(&candidate)
		if err := validateAuthorityInventoryProducerHeartbeat(candidate, identity, now); err == nil {
			t.Fatalf("bootstrap binding mutation %d was accepted", index)
		}
	}
}

func TestGroupInventoryHeartbeatBindsFaultDomainAndPoolAcrossEnvelope(t *testing.T) {
	value := authorityInventoryHeartbeatFixture("edge-group-neutral-a", "edge-01", 0, 1, time.Now().UTC(), "topology-binding-0001")
	value.FaultDomainID = "fault-domain-ovh-fra-1"
	value.EdgePoolID = "edge-pool-public-primary"
	value.Inventory.FaultDomainID = value.FaultDomainID
	value.Inventory.EdgePoolID = value.EdgePoolID
	value.Inventory.ActiveEpoch.FaultDomainID = value.FaultDomainID
	value.Inventory.ActiveEpoch.EdgePoolID = value.EdgePoolID
	value.Inventory.Instances[0].FaultDomainID = value.FaultDomainID
	value.Inventory.Instances[0].EdgePoolID = value.EdgePoolID
	if err := validateGroupInventoryHeartbeat(value, value.GroupID); err != nil {
		t.Fatalf("valid topology binding: %v", err)
	}

	mutations := []func(*GroupInventoryHeartbeat){
		func(v *GroupInventoryHeartbeat) { v.Inventory.EdgePoolID = "edge-pool-other" },
		func(v *GroupInventoryHeartbeat) { v.Inventory.ActiveEpoch.FaultDomainID = "fault-domain-other" },
		func(v *GroupInventoryHeartbeat) { v.Inventory.Instances[0].EdgePoolID = "edge-pool-other" },
		func(v *GroupInventoryHeartbeat) { v.FaultDomainID = "Country/DE" },
	}
	for index, mutate := range mutations {
		candidate := value
		candidate.Inventory.Instances = append([]GroupInstance(nil), value.Inventory.Instances...)
		mutate(&candidate)
		if err := validateGroupInventoryHeartbeat(candidate, candidate.GroupID); err == nil {
			t.Fatalf("topology mutation %d was accepted", index)
		}
	}
}

func writeInventoryWriterKeyringFixture(t *testing.T, now time.Time, revoked bool) (string, []byte) {
	t.Helper()
	root := t.TempDir()
	secret := []byte("0123456789abcdef0123456789abcdef")
	writeInventoryKeyringVersion(t, root, "version", now, "writer-current", secret, revoked)
	return filepath.Join(root, "version", "keyring.json"), secret
}

func writeInventoryWriterGroupKeyringFixture(t *testing.T, now time.Time, groupID string) (string, []byte) {
	t.Helper()
	root := t.TempDir()
	secret := []byte("0123456789abcdef0123456789abcdef")
	value := inventoryWriterKeyring{
		Schema: InventoryWriterKeyringSchemaV1, Generation: 1, GroupID: groupID,
		Keys: []inventoryWriterKey{{
			KeyID: "writer-current", Secret: base64.RawURLEncoding.EncodeToString(secret),
			NotBeforeUnix: now.Add(-time.Minute).Unix(), NotAfterUnix: now.Add(time.Hour).Unix(),
		}},
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, groupID+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return root, secret
}

func writeInventoryPlatformIdentityGroupFixture(t *testing.T, now time.Time, groupID, nodeID string) (string, string) {
	t.Helper()
	root := t.TempDir()
	activeKey := "inventory-platform-identity-key-0123456789abcdef"
	activeKeyID := "inventory-platform-current"
	value := inventoryPlatformIdentityKeyringFile{
		Schema: InventoryPlatformIdentityKeyringSchemaV1, Generation: 1, GroupID: groupID,
		ActiveKeyID: activeKeyID, ActiveKey: activeKey,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, groupID+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring(activeKey, activeKeyID, "", "", nil)
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: "inventory-producer-credential", Component: model.PlatformConsumerComponentEdgeWorker,
		NodeID: nodeID, ScopeKey: groupID, ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle},
	}, now, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return root, token
}

func writeInventoryKeyringVersion(t *testing.T, root, name string, now time.Time, keyID string, secret []byte, revoked bool) {
	t.Helper()
	directory := filepath.Join(root, name)
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	value := map[string]any{
		"schema":     InventoryWriterKeyringSchemaV1,
		"generation": 1,
		"keys": []map[string]any{{
			"key_id": keyID, "secret": base64.RawURLEncoding.EncodeToString(secret),
			"not_before_unix": now.Add(-time.Minute).Unix(), "not_after_unix": now.Add(time.Hour).Unix(), "revoked": revoked,
		}},
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "keyring.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func bytesReader(data []byte) *bytes.Reader { return bytes.NewReader(data) }
