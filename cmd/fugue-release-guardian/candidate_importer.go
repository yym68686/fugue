package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseguardian"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	edgeCandidateEnvelopeSchemaV1 = "edge-control-group-candidate-bundle/v1"
	edgeCandidateEnvelopePathV1   = "/v1/edge/candidate-envelope"
	maxCandidateEnvelopeBytes     = 2 << 20
)

// candidateImportConfig is deliberately a fixed, read-only input. The
// importer never receives credentials in a URL and never writes a workload.
type candidateImportConfig struct {
	GroupID   string
	Endpoint  string
	TokenFile string
}

type candidateEnvelope struct {
	Schema                  string                             `json:"schema"`
	GroupID                 string                             `json:"edge_group_id"`
	Epoch                   uint64                             `json:"epoch"`
	AuthorityLedgerSequence uint64                             `json:"authority_ledger_sequence"`
	CandidateLedgerSequence uint64                             `json:"candidate_ledger_sequence"`
	RouteIntentGeneration   string                             `json:"route_intent_generation"`
	InventoryGeneration     string                             `json:"inventory_generation"`
	ReleaseRecordDigest     string                             `json:"release_record_digest"`
	WorkerSlot              releaseguardian.AuthoritySlot      `json:"worker_slot"`
	PublishedAt             time.Time                          `json:"published_at"`
	CurrentRecord           *releaseguardian.RouteBundleRecord `json:"current_record"`
	CurrentBundle           *model.EdgeRouteBundle             `json:"current_bundle"`
	CurrentWorkerSlot       releaseguardian.AuthoritySlot      `json:"current_worker_slot"`
	Record                  releaseguardian.RouteBundleRecord  `json:"record"`
	Bundle                  model.EdgeRouteBundle              `json:"bundle"`
}

type candidateImportStore interface {
	CreateRouteBundleRecord(context.Context, releaseguardian.RouteBundleRecord) error
	LoadCandidate(context.Context, string) (releaseguardian.CandidateAuthority, types.UID, string, error)
	LoadCurrent(context.Context, string) (releaseguardian.CurrentAuthority, types.UID, string, error)
	PutCandidate(context.Context, releaseguardian.CandidateAuthority, types.UID, string) (types.UID, string, error)
	SwitchCurrent(context.Context, releaseguardian.CurrentAuthority, types.UID, string) (types.UID, string, error)
	ReplaceLoadedCandidate(context.Context, releaseguardian.CandidateAuthority, types.UID, string) (types.UID, string, error)
}

func parseCandidateImports(value string) ([]candidateImportConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	configs := make([]candidateImportConfig, 0, 2)
	seen := map[string]bool{}
	for _, raw := range strings.Split(value, ";") {
		fields := strings.Split(raw, ",")
		if len(fields) != 3 {
			return nil, errors.New("candidate import must be group,endpoint,token-file")
		}
		config := candidateImportConfig{GroupID: strings.TrimSpace(fields[0]), Endpoint: strings.TrimSpace(fields[1]), TokenFile: strings.TrimSpace(fields[2])}
		parsed, err := url.Parse(config.Endpoint)
		if err != nil || (releaseguardian.Key{Component: "edge", Group: config.GroupID}).Validate() != nil || seen[config.GroupID] ||
			(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Path != edgeCandidateEnvelopePathV1 ||
			parsed.RawQuery != "" || parsed.Fragment != "" || !filepath.IsAbs(config.TokenFile) || filepath.Clean(config.TokenFile) != config.TokenFile {
			return nil, errors.New("candidate import configuration is invalid")
		}
		seen[config.GroupID] = true
		configs = append(configs, config)
	}
	return configs, nil
}

func startCandidateImporters(ctx context.Context, store *releaseguardian.AuthorityStore, client kubernetes.Interface, configs []candidateImportConfig) {
	if store == nil || client == nil {
		return
	}
	for _, config := range configs {
		config := config
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				if _, err := importCandidateOnce(ctx, store, client, config, time.Now().UTC()); err != nil {
					fmt.Fprintf(os.Stderr, "candidate import %s: %v\n", config.GroupID, err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
}

func importCandidateOnce(ctx context.Context, store candidateImportStore, client kubernetes.Interface, config candidateImportConfig, now time.Time) (bool, error) {
	if store == nil || client == nil {
		return false, errors.New("candidate importer dependency is nil")
	}
	edgeID, err := candidateImportEdgeID(ctx, client, config.GroupID)
	if err != nil {
		return false, err
	}
	tokenRaw, err := os.ReadFile(config.TokenFile)
	if err != nil {
		return false, errors.New("candidate import credential is unavailable")
	}
	token := strings.TrimSpace(string(tokenRaw))
	if len(token) < 32 || len(token) > 256 || strings.ContainsAny(token, "\r\n\t ") {
		return false, errors.New("candidate import credential is invalid")
	}
	envelope, err := fetchCandidateEnvelope(ctx, config, edgeID, token)
	if err != nil {
		return false, err
	}
	if err := validateCandidateEnvelope(config.GroupID, envelope, now.UTC()); err != nil {
		return false, err
	}
	// Immutable records are idempotent. Persist both sides before creating the
	// mutable pointers, so an invalid envelope can never create a partial
	// authority state.
	if err := store.CreateRouteBundleRecord(ctx, *envelope.CurrentRecord); err != nil {
		return false, fmt.Errorf("persist current route record: %w", err)
	}
	if err := store.CreateRouteBundleRecord(ctx, envelope.Record); err != nil {
		return false, fmt.Errorf("persist candidate route record: %w", err)
	}
	changed := false
	current := releaseguardian.CurrentAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: config.GroupID,
		CurrentRecordDigest: envelope.CurrentRecord.RecordDigest, CurrentWorkerSlot: envelope.CurrentWorkerSlot,
		AuthorityEpoch: envelope.CurrentRecord.Epoch,
	}
	_, _, _, err = store.LoadCurrent(ctx, config.GroupID)
	currentMissing := apierrors.IsNotFound(err)
	if err != nil && !currentMissing {
		return false, fmt.Errorf("load current authority: %w", err)
	}
	candidate := releaseguardian.CandidateAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind, GroupID: config.GroupID,
		RecordDigest: envelope.Record.RecordDigest, BundleGeneration: envelope.Bundle.Version, ServingGeneration: envelope.Bundle.Generation,
		AuthoritySequence: envelope.AuthorityLedgerSequence, CandidateSequence: envelope.CandidateLedgerSequence,
		WorkerSlot: envelope.WorkerSlot, ReleaseRecordDigest: envelope.ReleaseRecordDigest,
		State: releaseguardian.CandidateAuthorityLoaded, Generation: 1,
	}
	existingCandidate, candidateUID, candidateRV, err := store.LoadCandidate(ctx, config.GroupID)
	candidateMissing := apierrors.IsNotFound(err)
	if err != nil && !candidateMissing {
		return false, fmt.Errorf("read candidate authority: %w", err)
	}
	candidateChanged := !candidateMissing && (existingCandidate.GroupID != candidate.GroupID || existingCandidate.RecordDigest != candidate.RecordDigest ||
		existingCandidate.BundleGeneration != candidate.BundleGeneration || existingCandidate.ServingGeneration != candidate.ServingGeneration ||
		existingCandidate.AuthoritySequence != candidate.AuthoritySequence || existingCandidate.CandidateSequence != candidate.CandidateSequence ||
		existingCandidate.WorkerSlot != candidate.WorkerSlot || existingCandidate.ReleaseRecordDigest != candidate.ReleaseRecordDigest)
	if candidateChanged && existingCandidate.State != releaseguardian.CandidateAuthorityLoaded {
		return false, errors.New("candidate envelope conflicts with terminal candidate authority")
	}
	if candidateMissing {
		if _, _, err := store.PutCandidate(ctx, candidate, "", ""); err != nil {
			return false, fmt.Errorf("load candidate authority: %w", err)
		}
		changed = true
	} else if candidateChanged {
		candidate.Generation = existingCandidate.Generation + 1
		if _, _, err := store.ReplaceLoadedCandidate(ctx, candidate, candidateUID, candidateRV); err != nil {
			return false, fmt.Errorf("replace imported candidate authority: %w", err)
		}
		changed = true
	}
	if currentMissing {
		if _, _, err := store.SwitchCurrent(ctx, current, "", ""); err != nil {
			return false, fmt.Errorf("bootstrap current authority: %w", err)
		}
		changed = true
	}
	return changed, nil
}

func candidateImportEdgeID(ctx context.Context, client kubernetes.Interface, groupID string) (string, error) {
	selector := labels.Set{"fugue.io/edge-group-id": groupID, "fugue.io/edge-control-client": "true"}.AsSelector().String()
	pods, err := client.CoreV1().Pods("fugue-system").List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 8})
	if err != nil || pods.Continue != "" || len(pods.Items) == 0 || len(pods.Items) > 7 {
		return "", errors.New("candidate import worker identity is unavailable")
	}
	nodes := map[string]bool{}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.DeletionTimestamp != nil || strings.TrimSpace(pod.Spec.NodeName) == "" {
			continue
		}
		nodes[strings.TrimSpace(pod.Spec.NodeName)] = true
	}
	if len(nodes) != 1 {
		return "", errors.New("candidate import worker identity is ambiguous")
	}
	for node := range nodes {
		if len(node) < 3 || len(node) > 63 || strings.ContainsAny(node, "\r\n\t ,&=?#") {
			return "", errors.New("candidate import worker identity is invalid")
		}
		return node, nil
	}
	return "", errors.New("candidate import worker identity is unavailable")
}

func fetchCandidateEnvelope(ctx context.Context, config candidateImportConfig, edgeID, token string) (candidateEnvelope, error) {
	endpoint, _ := url.Parse(config.Endpoint)
	query := endpoint.Query()
	query.Set("edge_group_id", config.GroupID)
	query.Set("edge_id", edgeID)
	endpoint.RawQuery = query.Encode()
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return candidateEnvelope{}, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return candidateEnvelope{}, errors.New("candidate envelope request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return candidateEnvelope{}, errors.New("candidate envelope request was rejected")
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxCandidateEnvelopeBytes+1))
	decoder.DisallowUnknownFields()
	var envelope candidateEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return candidateEnvelope{}, errors.New("candidate envelope is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return candidateEnvelope{}, errors.New("candidate envelope has trailing data")
	}
	return envelope, nil
}

func validateCandidateEnvelope(groupID string, envelope candidateEnvelope, now time.Time) error {
	if envelope.Schema != edgeCandidateEnvelopeSchemaV1 || envelope.GroupID != groupID || envelope.Epoch == 0 ||
		envelope.AuthorityLedgerSequence == 0 || envelope.CandidateLedgerSequence == 0 ||
		strings.TrimSpace(envelope.RouteIntentGeneration) == "" || strings.TrimSpace(envelope.InventoryGeneration) == "" ||
		!exactSHA256Digest(envelope.ReleaseRecordDigest) || envelope.PublishedAt.IsZero() || !envelope.PublishedAt.Equal(envelope.PublishedAt.UTC()) ||
		envelope.CurrentRecord == nil || envelope.CurrentBundle == nil || envelope.CurrentRecord.Validate() != nil || envelope.Record.Validate() != nil ||
		envelope.CurrentRecord.GroupID != groupID || envelope.Record.GroupID != groupID || envelope.Record.Epoch != int64(envelope.Epoch) ||
		envelope.CurrentRecord.Epoch >= envelope.Record.Epoch || envelope.CurrentWorkerSlot.Validate() != nil || envelope.WorkerSlot.Validate() != nil ||
		envelope.CurrentWorkerSlot == envelope.WorkerSlot || envelope.CurrentRecord.RecordDigest == envelope.Record.RecordDigest ||
		envelope.CurrentRecord.SourceSHA != envelope.Record.SourceSHA || envelope.CurrentRecord.ControlImageDigest != envelope.Record.ControlImageDigest ||
		envelope.CurrentRecord.ManifestDigest != envelope.Record.ManifestDigest || envelope.CurrentRecord.HealthContractDigest != envelope.Record.HealthContractDigest ||
		envelope.CurrentRecord.KeyID != envelope.CurrentBundle.KeyID || envelope.CurrentRecord.Signature != envelope.CurrentBundle.Signature ||
		envelope.Record.KeyID != envelope.Bundle.KeyID || envelope.Record.Signature != envelope.Bundle.Signature {
		return errors.New("candidate envelope identity is invalid")
	}
	if candidateBundleDigest(*envelope.CurrentBundle) != envelope.CurrentRecord.BundleDigest || candidateBundleDigest(envelope.Bundle) != envelope.Record.BundleDigest ||
		envelope.CurrentBundle.EdgeGroupID != groupID || envelope.Bundle.EdgeGroupID != groupID ||
		!envelope.CurrentBundle.ValidUntil.After(now) || !envelope.Bundle.ValidUntil.After(now) || envelope.CurrentBundle.GeneratedAt.IsZero() || envelope.Bundle.GeneratedAt.IsZero() ||
		!envelope.CurrentBundle.ValidUntil.After(envelope.CurrentBundle.GeneratedAt) || !envelope.Bundle.ValidUntil.After(envelope.Bundle.GeneratedAt) {
		return errors.New("candidate envelope bundle binding is invalid")
	}
	return nil
}

func candidateBundleDigest(bundle model.EdgeRouteBundle) string {
	raw, err := json.Marshal(bundle)
	if err != nil {
		return ""
	}
	return shaDigest(raw)
}
