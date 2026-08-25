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
	"regexp"
	"strconv"
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

var candidateServingAuthorityTokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{1,255}$`)

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
	WorkerSourceSHA         string                             `json:"worker_source_sha,omitempty"`
	WorkerImageDigest       string                             `json:"worker_image_digest,omitempty"`
	WorkerSlot              releaseguardian.AuthoritySlot      `json:"worker_slot"`
	PublishedAt             time.Time                          `json:"published_at"`
	CurrentRecord           *releaseguardian.RouteBundleRecord `json:"current_record"`
	CurrentBundle           *model.EdgeRouteBundle             `json:"current_bundle"`
	CurrentWorkerSlot       releaseguardian.AuthoritySlot      `json:"current_worker_slot"`
	ServingAuthority        *candidateServingAuthorityWitness  `json:"serving_authority,omitempty"`
	AllowDegradedPrevious   bool                               `json:"allow_degraded_previous,omitempty"`
	StandbyOnly             bool                               `json:"standby_only,omitempty"`
	Record                  releaseguardian.RouteBundleRecord  `json:"record"`
	Bundle                  model.EdgeRouteBundle              `json:"bundle"`
}

// candidateServingAuthorityWitness is the read-only Guardian projection of
// Edge Control's optional serving witness. Older Controls omit it. Newer
// Controls use it to bind the envelope to the exact Guardian CurrentAuthority
// and Front activation. Exact historical publications remain preferred; an
// explicitly degraded recovery may use the current signed Control publication
// when the historical bundle was pruned inside the same recovery epoch or the
// live Front authority was normalized ahead of Control's retained history.
type candidateServingAuthorityWitness struct {
	CurrentRecordDigest string                        `json:"current_record_digest"`
	AuthorityEpoch      int64                         `json:"authority_epoch"`
	CurrentAuthorityUID string                        `json:"current_authority_uid"`
	CurrentAuthorityRV  string                        `json:"current_authority_resource_version"`
	FrontGeneration     uint64                        `json:"front_generation"`
	BundleVersion       string                        `json:"bundle_version"`
	WorkerSlot          releaseguardian.AuthoritySlot `json:"worker_slot"`
	WorkerSourceSHA     string                        `json:"worker_source_sha"`
	WorkerImageDigest   string                        `json:"worker_image_digest"`
}

type candidateImportStore interface {
	CreateRouteBundleRecord(context.Context, releaseguardian.RouteBundleRecord) error
	LoadCandidate(context.Context, string) (releaseguardian.CandidateAuthority, types.UID, string, error)
	LoadCurrent(context.Context, string) (releaseguardian.CurrentAuthority, types.UID, string, error)
	PutCandidate(context.Context, releaseguardian.CandidateAuthority, types.UID, string) (types.UID, string, error)
	SwitchCurrent(context.Context, releaseguardian.CurrentAuthority, types.UID, string) (types.UID, string, error)
	ReplaceLoadedCandidate(context.Context, releaseguardian.CandidateAuthority, types.UID, string) (types.UID, string, error)
	ReplaceSettledCandidate(context.Context, releaseguardian.CandidateAuthority, types.UID, string) (types.UID, string, error)
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
	current, currentUID, currentRV, err := store.LoadCurrent(ctx, config.GroupID)
	currentMissing := apierrors.IsNotFound(err)
	if err != nil && !currentMissing {
		return false, fmt.Errorf("load current authority: %w", err)
	}
	if !currentMissing && candidateEnvelopeMatchesSettledCurrent(envelope, current) {
		return false, nil
	}
	if envelope.ServingAuthority != nil {
		if currentMissing || validateCandidateServingAuthorityBinding(envelope, current, currentUID, currentRV) != nil {
			return false, errors.New("candidate serving authority does not match Guardian current authority")
		}
	}
	// Standby envelopes restore the inactive LKG route source after promotion.
	// They are verified here but never enter Guardian's promotable state.
	if envelope.StandbyOnly {
		return false, nil
	}
	currentPublicationSequence, currentRecoveryEpoch, _ := parseAuthorityBundleVersion(envelope.CurrentBundle.Generation, envelope.CurrentBundle.Version)
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
	bootstrapCurrent := releaseguardian.CurrentAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: config.GroupID,
		CurrentRecordDigest: envelope.CurrentRecord.RecordDigest, CurrentWorkerSlot: envelope.CurrentWorkerSlot,
		AuthorityEpoch: envelope.CurrentRecord.Epoch,
	}
	candidate := releaseguardian.CandidateAuthority{
		APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CandidateAuthorityKind, GroupID: config.GroupID,
		RecordDigest: envelope.Record.RecordDigest, BundleGeneration: envelope.Bundle.Version, ServingGeneration: envelope.Bundle.Generation,
		AuthoritySequence: envelope.AuthorityLedgerSequence, CandidateSequence: envelope.CandidateLedgerSequence,
		CurrentPublicationSequence: currentPublicationSequence, CurrentRecoveryEpoch: currentRecoveryEpoch,
		CurrentBundleDigest: envelope.CurrentRecord.BundleDigest, CurrentServingGeneration: envelope.CurrentBundle.Generation, CandidateEpoch: envelope.Epoch,
		WorkerSlot: envelope.WorkerSlot, ReleaseRecordDigest: envelope.ReleaseRecordDigest,
		WorkerSourceSHA: envelope.WorkerSourceSHA, WorkerImageDigest: envelope.WorkerImageDigest,
		AllowDegradedPrevious: envelope.AllowDegradedPrevious,
		State:                 releaseguardian.CandidateAuthorityLoaded, Generation: 1,
	}
	if !candidate.HasWorkerReleaseIdentity() {
		return false, errors.New("candidate envelope lacks an explicitly staged Worker release")
	}
	existingCandidate, candidateUID, candidateRV, err := store.LoadCandidate(ctx, config.GroupID)
	candidateMissing := apierrors.IsNotFound(err)
	if err != nil && !candidateMissing {
		return false, fmt.Errorf("read candidate authority: %w", err)
	}
	candidateChanged := !candidateMissing && (existingCandidate.GroupID != candidate.GroupID || existingCandidate.RecordDigest != candidate.RecordDigest ||
		existingCandidate.BundleGeneration != candidate.BundleGeneration || existingCandidate.ServingGeneration != candidate.ServingGeneration ||
		existingCandidate.AuthoritySequence != candidate.AuthoritySequence || existingCandidate.CandidateSequence != candidate.CandidateSequence ||
		existingCandidate.CurrentPublicationSequence != candidate.CurrentPublicationSequence || existingCandidate.CurrentRecoveryEpoch != candidate.CurrentRecoveryEpoch ||
		existingCandidate.CurrentBundleDigest != candidate.CurrentBundleDigest || existingCandidate.CurrentServingGeneration != candidate.CurrentServingGeneration || existingCandidate.CandidateEpoch != candidate.CandidateEpoch ||
		existingCandidate.WorkerSlot != candidate.WorkerSlot || existingCandidate.ReleaseRecordDigest != candidate.ReleaseRecordDigest ||
		existingCandidate.AllowDegradedPrevious != candidate.AllowDegradedPrevious)
	candidateChanged = candidateChanged || existingCandidate.WorkerSourceSHA != candidate.WorkerSourceSHA || existingCandidate.WorkerImageDigest != candidate.WorkerImageDigest
	if candidateMissing {
		if _, _, err := store.PutCandidate(ctx, candidate, "", ""); err != nil {
			return false, fmt.Errorf("load candidate authority: %w", err)
		}
		changed = true
	} else if candidateChanged {
		candidate.Generation = existingCandidate.Generation + 1
		var replaceErr error
		if existingCandidate.State == releaseguardian.CandidateAuthorityLoaded {
			_, _, replaceErr = store.ReplaceLoadedCandidate(ctx, candidate, candidateUID, candidateRV)
		} else {
			_, _, replaceErr = store.ReplaceSettledCandidate(ctx, candidate, candidateUID, candidateRV)
		}
		if replaceErr != nil {
			return false, fmt.Errorf("replace imported candidate authority: %w", replaceErr)
		}
		changed = true
	}
	if currentMissing {
		if _, _, err := store.SwitchCurrent(ctx, bootstrapCurrent, "", ""); err != nil {
			return false, fmt.Errorf("bootstrap current authority: %w", err)
		}
		changed = true
	}
	return changed, nil
}

func candidateEnvelopeMatchesSettledCurrent(envelope candidateEnvelope, current releaseguardian.CurrentAuthority) bool {
	witness := envelope.ServingAuthority
	return !envelope.StandbyOnly && witness != nil &&
		current.AuthorityEpoch > witness.AuthorityEpoch && current.AuthorityEpoch-witness.AuthorityEpoch == 1 &&
		current.CurrentRecordDigest == envelope.Record.RecordDigest && current.CurrentWorkerSlot == envelope.WorkerSlot &&
		current.CurrentFrontGeneration > witness.FrontGeneration && current.CurrentFrontGeneration-witness.FrontGeneration == 1 &&
		current.CurrentBundleGeneration == envelope.Bundle.Version &&
		current.CurrentWorkerSourceSHA == envelope.WorkerSourceSHA && current.CurrentWorkerImageDigest == envelope.WorkerImageDigest &&
		current.PreviousRecordDigest == witness.CurrentRecordDigest && current.PreviousWorkerSlot == witness.WorkerSlot &&
		current.PreviousFrontGeneration == witness.FrontGeneration && current.PreviousBundleGeneration == witness.BundleVersion &&
		current.PreviousWorkerSourceSHA == witness.WorkerSourceSHA && current.PreviousWorkerImageDigest == witness.WorkerImageDigest
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
	if envelope.CurrentRecord == nil || envelope.CurrentBundle == nil {
		return errors.New("candidate envelope identity is invalid")
	}
	currentPublicationSequence, _, currentVersionErr := parseAuthorityBundleVersion(envelope.CurrentBundle.Generation, envelope.CurrentBundle.Version)
	candidatePublicationSequence, candidateRecoveryEpoch, candidateVersionErr := parseAuthorityBundleVersion(envelope.Bundle.Generation, envelope.Bundle.Version)
	if envelope.Schema != edgeCandidateEnvelopeSchemaV1 || envelope.GroupID != groupID || envelope.Epoch == 0 ||
		envelope.AuthorityLedgerSequence == 0 || envelope.CandidateLedgerSequence == 0 ||
		strings.TrimSpace(envelope.RouteIntentGeneration) == "" || strings.TrimSpace(envelope.InventoryGeneration) == "" ||
		!exactSHA256Digest(envelope.ReleaseRecordDigest) || envelope.PublishedAt.IsZero() || !envelope.PublishedAt.Equal(envelope.PublishedAt.UTC()) ||
		!exactSourceSHA(envelope.WorkerSourceSHA) || !exactSHA256Digest(envelope.WorkerImageDigest) ||
		envelope.CurrentRecord.Validate() != nil || envelope.Record.Validate() != nil ||
		envelope.CurrentRecord.GroupID != groupID || envelope.Record.GroupID != groupID || envelope.Record.Epoch != int64(envelope.Epoch) ||
		envelope.CurrentRecord.Epoch >= envelope.Record.Epoch || envelope.CurrentWorkerSlot.Validate() != nil || envelope.WorkerSlot.Validate() != nil ||
		envelope.CurrentWorkerSlot == envelope.WorkerSlot || envelope.CurrentRecord.RecordDigest == envelope.Record.RecordDigest ||
		envelope.CurrentRecord.SourceSHA != envelope.Record.SourceSHA || envelope.CurrentRecord.ControlImageDigest != envelope.Record.ControlImageDigest ||
		envelope.CurrentRecord.ManifestDigest != envelope.Record.ManifestDigest || envelope.CurrentRecord.HealthContractDigest != envelope.Record.HealthContractDigest ||
		envelope.CurrentRecord.KeyID != envelope.CurrentBundle.KeyID || envelope.CurrentRecord.Signature != envelope.CurrentBundle.Signature ||
		envelope.Record.KeyID != envelope.Bundle.KeyID || envelope.Record.Signature != envelope.Bundle.Signature ||
		currentVersionErr != nil || candidateVersionErr != nil || currentPublicationSequence != uint64(envelope.CurrentRecord.Epoch) ||
		candidatePublicationSequence != envelope.Epoch || candidateRecoveryEpoch != 0 {
		return errors.New("candidate envelope identity is invalid")
	}
	if candidateBundleDigest(*envelope.CurrentBundle) != envelope.CurrentRecord.BundleDigest || candidateBundleDigest(envelope.Bundle) != envelope.Record.BundleDigest ||
		envelope.CurrentBundle.EdgeGroupID != groupID || envelope.Bundle.EdgeGroupID != groupID ||
		!envelope.Bundle.ValidUntil.After(now) || envelope.CurrentBundle.GeneratedAt.IsZero() || envelope.Bundle.GeneratedAt.IsZero() ||
		envelope.CurrentBundle.GeneratedAt.After(now) || envelope.Bundle.GeneratedAt.After(now) ||
		!envelope.CurrentBundle.ValidUntil.After(envelope.CurrentBundle.GeneratedAt) || !envelope.Bundle.ValidUntil.After(envelope.Bundle.GeneratedAt) {
		return errors.New("candidate envelope bundle binding is invalid")
	}
	if envelope.ServingAuthority != nil && validateCandidateServingAuthorityEnvelope(envelope) != nil {
		return errors.New("candidate envelope serving authority is invalid")
	}
	if envelope.AllowDegradedPrevious && envelope.ServingAuthority == nil {
		return errors.New("candidate envelope degraded previous authorization is invalid")
	}
	if envelope.StandbyOnly && (envelope.ServingAuthority == nil || envelope.AllowDegradedPrevious) {
		return errors.New("candidate envelope standby authorization is invalid")
	}
	return nil
}

func validateCandidateServingAuthorityEnvelope(envelope candidateEnvelope) error {
	witness := envelope.ServingAuthority
	if witness == nil {
		return nil
	}
	if !exactSHA256Digest(witness.CurrentRecordDigest) || witness.AuthorityEpoch < 1 ||
		!candidateServingAuthorityTokenPattern.MatchString(witness.CurrentAuthorityUID) ||
		!candidateServingAuthorityTokenPattern.MatchString(witness.CurrentAuthorityRV) || witness.FrontGeneration == 0 ||
		witness.WorkerSlot.Validate() != nil || witness.WorkerSlot != envelope.CurrentWorkerSlot || witness.WorkerSlot == envelope.WorkerSlot ||
		!exactSourceSHA(witness.WorkerSourceSHA) || !exactSHA256Digest(witness.WorkerImageDigest) {
		return errors.New("candidate serving authority identity is invalid")
	}
	witnessGeneration, witnessPublication, witnessRecovery, err := parseUnboundAuthorityBundleVersion(witness.BundleVersion)
	if err != nil {
		return errors.New("candidate serving publication is invalid")
	}
	if witnessGeneration == envelope.Bundle.Generation {
		return nil
	}
	if !envelope.AllowDegradedPrevious || envelope.StandbyOnly || envelope.CurrentBundle == nil ||
		envelope.Bundle.Generation != envelope.CurrentBundle.Generation {
		return errors.New("candidate serving publication is invalid")
	}
	currentPublication, currentRecovery, err := parseAuthorityBundleVersion(envelope.CurrentBundle.Generation, envelope.CurrentBundle.Version)
	if err != nil || !servingAuthorityCanUseCurrentControlPublication(witnessPublication, witnessRecovery, currentPublication, currentRecovery) {
		return errors.New("candidate serving publication is outside the current recovery window")
	}
	return nil
}

func servingAuthorityCanUseCurrentControlPublication(witnessPublication, witnessRecovery, currentPublication, currentRecovery uint64) bool {
	if witnessPublication < currentPublication && witnessRecovery == currentRecovery {
		return true
	}
	return witnessPublication > currentPublication && witnessRecovery == 0
}

func validateCandidateServingAuthorityBinding(envelope candidateEnvelope, current releaseguardian.CurrentAuthority, uid types.UID, resourceVersion string) error {
	witness := envelope.ServingAuthority
	if witness == nil {
		return nil
	}
	if current.Validate() != nil || current.GroupID != envelope.GroupID || current.CurrentRecordDigest != witness.CurrentRecordDigest ||
		current.AuthorityEpoch != witness.AuthorityEpoch || string(uid) != witness.CurrentAuthorityUID || resourceVersion != witness.CurrentAuthorityRV {
		return errors.New("candidate serving authority binding is invalid")
	}
	if current.CurrentFrontGeneration == witness.FrontGeneration && current.CurrentBundleGeneration == witness.BundleVersion &&
		current.CurrentWorkerSlot == witness.WorkerSlot && current.CurrentWorkerSourceSHA == witness.WorkerSourceSHA &&
		current.CurrentWorkerImageDigest == witness.WorkerImageDigest {
		return nil
	}
	// During controlled recovery Front may already have committed one signed
	// activation beyond Guardian CurrentAuthority. The activation can be the
	// declared release LKG even when Guardian's previous metadata predates it,
	// so bind recovery to the signed envelope, exact CurrentAuthority CAS,
	// consecutive Front generation, slot switch, and immutable bundle family.
	// Ordinary candidates must still match current or previous metadata exactly.
	if current.CurrentWorkerSlot == witness.WorkerSlot || current.CurrentFrontGeneration == 0 ||
		witness.FrontGeneration != current.CurrentFrontGeneration+1 {
		return errors.New("candidate serving authority binding is invalid")
	}
	currentGeneration, _, _, currentErr := parseUnboundAuthorityBundleVersion(current.CurrentBundleGeneration)
	witnessGeneration, _, _, witnessErr := parseUnboundAuthorityBundleVersion(witness.BundleVersion)
	if currentErr != nil || witnessErr != nil || currentGeneration != witnessGeneration {
		return errors.New("candidate serving authority binding is invalid")
	}
	previousMatches := current.PreviousWorkerSlot == witness.WorkerSlot && current.PreviousBundleGeneration == witness.BundleVersion &&
		current.PreviousWorkerSourceSHA == witness.WorkerSourceSHA && current.PreviousWorkerImageDigest == witness.WorkerImageDigest &&
		current.PreviousFrontGeneration < current.CurrentFrontGeneration
	if previousMatches || envelope.AllowDegradedPrevious && !envelope.StandbyOnly {
		return nil
	}
	return errors.New("candidate serving authority binding is invalid")
}

func parseAuthorityBundleVersion(generation, version string) (uint64, uint64, error) {
	parsedGeneration, sequence, recoveryEpoch, err := parseUnboundAuthorityBundleVersion(version)
	if err != nil || strings.TrimSpace(generation) == "" || parsedGeneration != strings.TrimSpace(generation) {
		return 0, 0, errors.New("authority bundle version is invalid")
	}
	return sequence, recoveryEpoch, nil
}

func parseUnboundAuthorityBundleVersion(version string) (string, uint64, uint64, error) {
	version = strings.TrimSpace(version)
	recoverySeparator := strings.LastIndex(version, ".r")
	if recoverySeparator <= 0 || recoverySeparator+2 >= len(version) {
		return "", 0, 0, errors.New("authority bundle version is invalid")
	}
	publicationSeparator := strings.LastIndex(version[:recoverySeparator], ".p")
	if publicationSeparator <= 0 || publicationSeparator+2 >= recoverySeparator {
		return "", 0, 0, errors.New("authority bundle version is invalid")
	}
	generation := version[:publicationSeparator]
	sequence, sequenceErr := strconv.ParseUint(version[publicationSeparator+2:recoverySeparator], 10, 64)
	recoveryEpoch, recoveryErr := strconv.ParseUint(version[recoverySeparator+2:], 10, 64)
	if sequenceErr != nil || recoveryErr != nil || sequence == 0 {
		return "", 0, 0, errors.New("authority bundle version is invalid")
	}
	return generation, sequence, recoveryEpoch, nil
}

func candidateBundleDigest(bundle model.EdgeRouteBundle) string {
	raw, err := json.Marshal(bundle)
	if err != nil {
		return ""
	}
	return shaDigest(raw)
}
