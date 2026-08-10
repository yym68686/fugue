package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

const maxMonitorRecordBytes = 900 << 10

type monitorBundle struct {
	Plan     declarativerelease.Plan
	Artifact declarativerelease.ArtifactReceipt
	Prepared declarativerelease.ExecutionPlan
	Terminal declarativerelease.ExecutionResult
	Forward  []byte
	LKG      []byte
	Record   declarativerelease.MonitorRecord
	Raw      map[string][]byte
}

type monitorSnapshot struct {
	Namespace  string
	StateName  string
	StateUID   string
	StateRV    string
	RecordName string
	State      declarativerelease.MonitorState
	Bundle     monitorBundle
}

type monitorStore struct {
	client coreclient.CoreV1Interface
	now    func() time.Time
}

func newMonitorStore() (*monitorStore, error) {
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes client config for release monitor: %w", err)
	}
	client, err := coreclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client for release monitor: %w", err)
	}
	return &monitorStore{client: client, now: time.Now}, nil
}

func decodeMonitorBundle(files map[string][]byte, terminalRaw []byte) (monitorBundle, error) {
	want := []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "release-plan.json"}
	if len(files) != len(want) {
		return monitorBundle{}, errors.New("monitor bundle file inventory is invalid")
	}
	for _, name := range want {
		if len(files[name]) == 0 {
			return monitorBundle{}, fmt.Errorf("monitor bundle file %q is empty", name)
		}
	}
	plan, err := declarativerelease.DecodePlan(bytes.NewReader(files["release-plan.json"]))
	if err != nil {
		return monitorBundle{}, err
	}
	artifact, err := declarativerelease.DecodeArtifactReceipt(bytes.NewReader(files["artifact-receipt.json"]))
	if err != nil {
		return monitorBundle{}, err
	}
	prepared, err := declarativerelease.DecodeRecordedExecutionPlan(bytes.NewReader(files["execution-plan.json"]), plan, files["forward.json"], files["lkg.json"])
	if err != nil {
		return monitorBundle{}, err
	}
	terminal, err := declarativerelease.DecodeExecutionResult(bytes.NewReader(terminalRaw))
	if err != nil {
		return monitorBundle{}, err
	}
	record, err := declarativerelease.NewMonitorRecord(plan, artifact, prepared, terminal, files["forward.json"], files["lkg.json"])
	if err != nil {
		return monitorBundle{}, err
	}
	raw := make(map[string][]byte, len(files)+2)
	for name, value := range files {
		raw[name] = bytes.TrimSpace(append([]byte(nil), value...))
	}
	raw["terminal-result.json"] = bytes.TrimSpace(append([]byte(nil), terminalRaw...))
	recordRaw, err := declarativerelease.CanonicalJSON(record)
	if err != nil {
		return monitorBundle{}, err
	}
	raw["record.json"] = recordRaw
	return monitorBundle{Plan: plan, Artifact: artifact, Prepared: prepared, Terminal: terminal, Forward: raw["forward.json"], LKG: raw["lkg.json"], Record: record, Raw: raw}, nil
}

func decodeStoredMonitorBundle(data map[string]string) (monitorBundle, error) {
	files := make(map[string][]byte, 5)
	for _, name := range []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "release-plan.json"} {
		value, exists := data[name]
		if !exists {
			return monitorBundle{}, fmt.Errorf("stored monitor record lacks %q", name)
		}
		files[name] = []byte(value)
	}
	bundle, err := decodeMonitorBundle(files, []byte(data["terminal-result.json"]))
	if err != nil {
		return monitorBundle{}, err
	}
	storedRecord, err := decodeMonitorRecord([]byte(data["record.json"]))
	if err != nil {
		return monitorBundle{}, err
	}
	if storedRecord != bundle.Record {
		return monitorBundle{}, errors.New("stored monitor record envelope does not match its canonical files")
	}
	return bundle, nil
}

func decodeMonitorRecord(raw []byte) (declarativerelease.MonitorRecord, error) {
	var record declarativerelease.MonitorRecord
	if err := decodeStrictJSON(raw, &record); err != nil {
		return declarativerelease.MonitorRecord{}, fmt.Errorf("decode monitor record: %w", err)
	}
	return record, nil
}

func decodeMonitorState(raw []byte) (declarativerelease.MonitorState, error) {
	var state declarativerelease.MonitorState
	if err := decodeStrictJSON(raw, &state); err != nil {
		return declarativerelease.MonitorState{}, fmt.Errorf("decode monitor state: %w", err)
	}
	if err := state.Validate(); err != nil {
		return declarativerelease.MonitorState{}, err
	}
	return state, nil
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing content")
	}
	return nil
}

func (store *monitorStore) persistVerified(ctx context.Context, release declarativerelease.PlanRelease, files map[string][]byte, terminal declarativerelease.ExecutionResult) (monitorSnapshot, error) {
	if store == nil || store.client == nil || store.now == nil {
		return monitorSnapshot{}, errors.New("release monitor store is unavailable")
	}
	terminalRaw, err := declarativerelease.CanonicalJSON(terminal)
	if err != nil {
		return monitorSnapshot{}, err
	}
	bundle, err := decodeMonitorBundle(files, terminalRaw)
	if err != nil {
		return monitorSnapshot{}, err
	}
	if release.ComponentID != bundle.Record.Component || release.Workload.Namespace == "" {
		return monitorSnapshot{}, errors.New("monitor record release identity is invalid")
	}
	recordName := monitorRecordName(bundle.Record)
	stateName := monitorStateName(release.ComponentID)
	configMaps := store.client.ConfigMaps(release.Workload.Namespace)
	recordData, total, err := monitorRecordData(bundle.Raw)
	if err != nil {
		return monitorSnapshot{}, err
	}
	if total > maxMonitorRecordBytes {
		return monitorSnapshot{}, errors.New("monitor record exceeds the bounded Kubernetes ConfigMap size")
	}
	immutable := true
	desiredRecord := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: recordName, Namespace: release.Workload.Namespace, Labels: monitorLabels(release.ComponentID, bundle.Record.ConfigSHA)},
		Immutable:  &immutable, Data: recordData,
	}
	created, createErr := configMaps.Create(ctx, desiredRecord, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(createErr) {
		created, createErr = configMaps.Get(ctx, recordName, metav1.GetOptions{})
		if createErr == nil && (!mapsEqual(created.Data, desiredRecord.Data) || created.Immutable == nil || !*created.Immutable || !labelsMatch(created.Labels, desiredRecord.Labels)) {
			createErr = errors.New("existing immutable monitor record does not match the verified release")
		}
	}
	if createErr != nil {
		return monitorSnapshot{}, fmt.Errorf("persist immutable monitor record: %w", createErr)
	}
	state, _, err := declarativerelease.NewMonitorState(bundle.Record, declarativerelease.MonitorState{}, true, "", store.now())
	if err != nil {
		return monitorSnapshot{}, err
	}
	stateRaw, err := declarativerelease.CanonicalJSON(state)
	if err != nil {
		return monitorSnapshot{}, err
	}
	desiredState := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: stateName, Namespace: release.Workload.Namespace, Labels: monitorLabels(release.ComponentID, bundle.Record.ConfigSHA)},
		Data:       map[string]string{"recordName": recordName, "state.json": string(stateRaw)},
	}
	current, getErr := configMaps.Get(ctx, stateName, metav1.GetOptions{})
	if apierrors.IsNotFound(getErr) {
		current, getErr = configMaps.Create(ctx, desiredState, metav1.CreateOptions{})
	} else if getErr == nil {
		updated := current.DeepCopy()
		updated.Labels = desiredState.Labels
		updated.Data = desiredState.Data
		current, getErr = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	}
	if getErr != nil {
		return monitorSnapshot{}, fmt.Errorf("activate verified monitor record with resourceVersion CAS: %w", getErr)
	}
	return monitorSnapshot{Namespace: release.Workload.Namespace, StateName: stateName, StateUID: string(current.UID), StateRV: current.ResourceVersion, RecordName: recordName, State: state, Bundle: bundle}, nil
}

func (store *monitorStore) load(ctx context.Context, namespace, component string) (monitorSnapshot, error) {
	if store == nil || store.client == nil {
		return monitorSnapshot{}, errors.New("release monitor store is unavailable")
	}
	configMaps := store.client.ConfigMaps(namespace)
	stateName := monitorStateName(component)
	stateMap, err := configMaps.Get(ctx, stateName, metav1.GetOptions{})
	if err != nil {
		return monitorSnapshot{}, fmt.Errorf("read component monitor state: %w", err)
	}
	recordName := strings.TrimSpace(stateMap.Data["recordName"])
	state, err := decodeMonitorState([]byte(stateMap.Data["state.json"]))
	if err != nil || state.Component != component || recordName != monitorRecordNameFromIdentity(component, state.RecordDigest) {
		return monitorSnapshot{}, errors.New("component monitor state binding is invalid")
	}
	recordMap, err := configMaps.Get(ctx, recordName, metav1.GetOptions{})
	if err != nil {
		return monitorSnapshot{}, fmt.Errorf("read immutable component monitor record: %w", err)
	}
	if recordMap.Immutable == nil || !*recordMap.Immutable || string(recordMap.Labels["fugue.pro/component"]) != component {
		return monitorSnapshot{}, errors.New("immutable component monitor record metadata is invalid")
	}
	bundle, err := decodeStoredMonitorBundle(recordMap.Data)
	if err != nil || bundle.Record.RecordDigest != state.RecordDigest || bundle.Record.ConfigSHA != state.ConfigSHA {
		return monitorSnapshot{}, errors.New("component monitor record/state binding is invalid")
	}
	return monitorSnapshot{Namespace: namespace, StateName: stateName, StateUID: string(stateMap.UID), StateRV: stateMap.ResourceVersion, RecordName: recordName, State: state, Bundle: bundle}, nil
}

func (store *monitorStore) updateState(ctx context.Context, snapshot monitorSnapshot, state declarativerelease.MonitorState) (monitorSnapshot, error) {
	if err := state.Validate(); err != nil || state.RecordDigest != snapshot.Bundle.Record.RecordDigest || state.Component != snapshot.Bundle.Record.Component {
		return monitorSnapshot{}, errors.New("updated component monitor state is invalid")
	}
	configMaps := store.client.ConfigMaps(snapshot.Namespace)
	current, err := configMaps.Get(ctx, snapshot.StateName, metav1.GetOptions{})
	if err != nil {
		return monitorSnapshot{}, err
	}
	if string(current.UID) != snapshot.StateUID || current.ResourceVersion != snapshot.StateRV || current.Data["recordName"] != snapshot.RecordName {
		return monitorSnapshot{}, errors.New("component monitor state changed before resourceVersion CAS")
	}
	raw, err := declarativerelease.CanonicalJSON(state)
	if err != nil {
		return monitorSnapshot{}, err
	}
	updated := current.DeepCopy()
	updated.Labels = monitorLabels(state.Component, state.ConfigSHA)
	updated.Data = map[string]string{"recordName": snapshot.RecordName, "state.json": string(raw)}
	updated, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return monitorSnapshot{}, fmt.Errorf("update component monitor state with resourceVersion CAS: %w", err)
	}
	snapshot.State = state
	snapshot.StateRV = updated.ResourceVersion
	return snapshot, nil
}

func (store *monitorStore) activateExistingRecord(ctx context.Context, current monitorSnapshot, recordName string, bundle monitorBundle) (monitorSnapshot, error) {
	if store == nil || store.client == nil || recordName != monitorRecordName(bundle.Record) || bundle.Record.Component != current.Bundle.Record.Component {
		return monitorSnapshot{}, errors.New("existing monitor record activation is invalid")
	}
	configMaps := store.client.ConfigMaps(current.Namespace)
	recordMap, err := configMaps.Get(ctx, recordName, metav1.GetOptions{})
	if err != nil || recordMap.Immutable == nil || !*recordMap.Immutable {
		return monitorSnapshot{}, errors.New("existing monitor record is absent or mutable")
	}
	stored, err := decodeStoredMonitorBundle(recordMap.Data)
	if err != nil || stored.Record != bundle.Record {
		return monitorSnapshot{}, errors.New("existing monitor record content is invalid")
	}
	state, _, err := declarativerelease.NewMonitorState(bundle.Record, declarativerelease.MonitorState{}, true, "", store.now())
	if err != nil {
		return monitorSnapshot{}, err
	}
	state.RollbackStatus = "lkg-restored"
	stateRaw, err := declarativerelease.CanonicalJSON(state)
	if err != nil {
		return monitorSnapshot{}, err
	}
	pointer, err := configMaps.Get(ctx, current.StateName, metav1.GetOptions{})
	if err != nil || string(pointer.UID) != current.StateUID || pointer.ResourceVersion != current.StateRV || pointer.Data["recordName"] != current.RecordName {
		return monitorSnapshot{}, errors.New("component monitor pointer changed before LKG activation CAS")
	}
	updated := pointer.DeepCopy()
	updated.Labels = monitorLabels(bundle.Record.Component, bundle.Record.ConfigSHA)
	updated.Data = map[string]string{"recordName": recordName, "state.json": string(stateRaw)}
	updated, err = configMaps.Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		return monitorSnapshot{}, fmt.Errorf("activate existing LKG monitor record with resourceVersion CAS: %w", err)
	}
	return monitorSnapshot{
		Namespace: current.Namespace, StateName: current.StateName, StateUID: string(updated.UID), StateRV: updated.ResourceVersion,
		RecordName: recordName, State: state, Bundle: bundle,
	}, nil
}

func monitorRecordData(raw map[string][]byte) (map[string]string, int, error) {
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	data := make(map[string]string, len(names))
	total := 0
	for _, name := range names {
		value := bytes.TrimSpace(raw[name])
		if len(value) == 0 || strings.ContainsRune(name, '\x00') {
			return nil, 0, errors.New("monitor record data is invalid")
		}
		data[name] = string(value)
		total += len(name) + len(value)
	}
	return data, total, nil
}

func monitorRecordName(record declarativerelease.MonitorRecord) string {
	return monitorRecordNameFromIdentity(record.Component, record.RecordDigest)
}

func monitorRecordNameFromIdentity(component, digest string) string {
	suffix := strings.TrimPrefix(digest, "sha256:")
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
	return "fugue-release-record-" + component + "-" + suffix
}

func monitorStateName(component string) string {
	return "fugue-release-monitor-" + component
}

func monitorLabels(component, configSHA string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/managed-by": "fugue-declarative-release",
		"fugue.pro/component":          component,
		"fugue.pro/config-sha":         configSHA,
	}
}

func mapsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func labelsMatch(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	return true
}
