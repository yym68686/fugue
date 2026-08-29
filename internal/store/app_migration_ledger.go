package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

var errMigrationCutoverEvidenceMissing = errors.New("migration cutover evidence is missing or not verified")

const migrationCutoverEvidenceMaxAge = 15 * time.Minute

// ValidateAppMigrationCutover is the single fail-closed gate used by managed
// controllers and external agents before a migration operation can complete.
// It deliberately accepts only affirmative, current evidence; historical
// status fields and an operation's success message are never sufficient.
func ValidateAppMigrationCutover(ledger model.AppMigrationLedger) error {
	return validateAppMigrationCutover(ledger, true)
}

// validateAppMigrationCutover reuses the same affirmative evidence checks for
// both the completion transition and later artifact-retirement decisions. The
// latter deliberately skips the short freshness window: a successfully
// completed migration remains a valid historical permission for its 90-day
// audit lifetime and must not become blocked merely because it is old.
func validateAppMigrationCutover(ledger model.AppMigrationLedger, requireFresh bool) error {
	// Do not let NormalizeAppMigrationLedger manufacture a timestamp for a
	// completion payload: a cutover gate needs a real point-in-time witness.
	if ledger.ObservedAt.IsZero() {
		return fmt.Errorf("%w: observed_at is missing", errMigrationCutoverEvidenceMissing)
	}
	ledger = model.NormalizeAppMigrationLedger(ledger, time.Now().UTC())
	missing := []string{}
	if ledger.OperationID == "" || ledger.AppID == "" || ledger.NewRuntimeID == "" {
		missing = append(missing, "operation/app/target runtime identity")
	}
	if ledger.OldClusterID == "" || ledger.NewClusterID == "" {
		missing = append(missing, "old/new cluster id")
	}
	if ledger.AssociatedOperationID == "" || ledger.AssociatedOperationID != ledger.OperationID {
		missing = append(missing, "associated operation id")
	}
	if ledger.OperatorType == "" || ledger.OperatorID == "" {
		missing = append(missing, "operator identity")
	}
	if ledger.EvidenceSource == "" {
		missing = append(missing, "evidence source")
	}
	if requireFresh {
		now := time.Now().UTC()
		observedAt := ledger.ObservedAt.UTC()
		if observedAt.After(now.Add(2*time.Minute)) || now.Sub(observedAt) > migrationCutoverEvidenceMaxAge {
			missing = append(missing, "fresh cutover observation")
		}
	}
	if ledger.ImageReplicationStatus != model.AppMigrationEvidenceVerified {
		missing = append(missing, "image replication")
	}
	if ledger.RuntimeObjectStatus != model.AppMigrationEvidenceVerified && ledger.RuntimeObjectStatus != model.AppMigrationEvidenceReady {
		missing = append(missing, "runtime object")
	}
	if ledger.EndpointRequired {
		if ledger.EndpointStatus != model.AppMigrationEvidenceReady || ledger.EndpointReady == nil || !*ledger.EndpointReady {
			missing = append(missing, "ready endpoint")
		}
	} else if ledger.EndpointStatus != model.AppMigrationEvidenceNotApplicable && ledger.EndpointStatus != model.AppMigrationEvidenceReady {
		missing = append(missing, "endpoint applicability/readiness")
	}
	if ledger.DesiredReplicas > 0 {
		if ledger.PhysicalReplicas == nil || *ledger.PhysicalReplicas < ledger.DesiredReplicas {
			missing = append(missing, "physical replicas")
		}
		if ledger.Generation <= 0 || ledger.ObservedGeneration < ledger.Generation {
			missing = append(missing, "current generation")
		}
	}
	if len(ledger.InvariantViolations) > 0 {
		missing = append(missing, "runtime invariants: "+strings.Join(ledger.InvariantViolations, ","))
	}
	if !ledger.OldArtifactsProtected {
		missing = append(missing, "old artifact protection")
	}
	if ledger.CutoverStatus != model.AppMigrationCutoverVerified && ledger.CutoverStatus != model.AppMigrationCutoverCompleted {
		missing = append(missing, "verified cutover status")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: %s", errMigrationCutoverEvidenceMissing, strings.Join(missing, "; "))
	}
	return nil
}

func migrationLedgerEvidenceType(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case model.AppMigrationCutoverVerified, model.AppMigrationCutoverCompleted:
		return model.OperationEvidenceTypeMigrationCompleted
	case model.AppMigrationCutoverFailed, model.AppMigrationCutoverBlocked:
		return model.OperationEvidenceTypeMigrationFailed
	default:
		return model.OperationEvidenceTypeMigrationStarted
	}
}

func migrationLedgerPayload(ledger model.AppMigrationLedger) map[string]any {
	data, err := json.Marshal(ledger)
	if err != nil {
		return map[string]any{"schema_version": model.AppMigrationLedgerSchemaVersion}
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil || payload == nil {
		return map[string]any{"schema_version": model.AppMigrationLedgerSchemaVersion}
	}
	return payload
}

func migrationLedgerFromEvidence(evidence model.OperationEvidence) (model.AppMigrationLedger, error) {
	if len(evidence.Payload) == 0 {
		return model.AppMigrationLedger{}, errMigrationCutoverEvidenceMissing
	}
	data, err := json.Marshal(evidence.Payload)
	if err != nil {
		return model.AppMigrationLedger{}, err
	}
	var ledger model.AppMigrationLedger
	if err := json.Unmarshal(data, &ledger); err != nil {
		return model.AppMigrationLedger{}, err
	}
	ledger = model.NormalizeAppMigrationLedger(ledger, evidence.CollectedAt)
	if ledger.ID == "" {
		ledger.ID = evidence.ID
	}
	if ledger.TenantID == "" {
		ledger.TenantID = evidence.TenantID
	}
	if ledger.AppID == "" {
		ledger.AppID = evidence.AppID
	}
	if ledger.OperationID == "" {
		ledger.OperationID = evidence.OperationID
	}
	return ledger, nil
}

// RecordAppMigrationLedger appends an immutable, versioned ledger snapshot to
// operation evidence.  Keeping snapshots immutable makes the 90-day audit
// trail reconstructible even when a later attempt fails.
func (s *Store) RecordAppMigrationLedger(ledger model.AppMigrationLedger) (model.AppMigrationLedger, error) {
	if s == nil {
		return model.AppMigrationLedger{}, ErrInvalidInput
	}
	ledger = model.NormalizeAppMigrationLedger(ledger, time.Now().UTC())
	if ledger.ID == "" {
		ledger.ID = model.NewID("migration")
	}
	if ledger.TenantID == "" || ledger.AppID == "" || ledger.OperationID == "" {
		return model.AppMigrationLedger{}, ErrInvalidInput
	}
	op, err := s.GetOperation(ledger.OperationID)
	if err != nil {
		return model.AppMigrationLedger{}, err
	}
	if op.Type != model.OperationTypeMigrate || strings.TrimSpace(op.AppID) != ledger.AppID || strings.TrimSpace(op.TenantID) != ledger.TenantID {
		return model.AppMigrationLedger{}, ErrInvalidInput
	}
	if ledger.OldRuntimeID != "" && ledger.OldRuntimeID != strings.TrimSpace(op.SourceRuntimeID) {
		return model.AppMigrationLedger{}, ErrInvalidInput
	}
	if ledger.NewRuntimeID != "" && ledger.NewRuntimeID != strings.TrimSpace(op.TargetRuntimeID) {
		return model.AppMigrationLedger{}, ErrInvalidInput
	}
	if ledger.ProjectID == "" {
		if app, appErr := s.GetApp(ledger.AppID); appErr == nil {
			ledger.ProjectID = app.ProjectID
		}
	}
	if ledger.OperatorType == "" {
		ledger.OperatorType = op.RequestedByType
	}
	if ledger.OperatorID == "" {
		ledger.OperatorID = op.RequestedByID
	}
	if ledger.OperatorType == "" {
		ledger.OperatorType = "unknown"
	}
	if ledger.OperatorID == "" {
		ledger.OperatorID = "unknown"
	}
	if ledger.EvidenceSource == "" {
		ledger.EvidenceSource = model.OperationEvidenceSourceController
	}
	if ledger.OldRuntimeID == "" {
		ledger.OldRuntimeID = op.SourceRuntimeID
	}
	if ledger.NewRuntimeID == "" {
		ledger.NewRuntimeID = op.TargetRuntimeID
	}
	if ledger.AssociatedOperationID == "" {
		ledger.AssociatedOperationID = op.ID
	} else if ledger.AssociatedOperationID != op.ID {
		return model.AppMigrationLedger{}, ErrInvalidInput
	}
	if ledger.RetainUntil.IsZero() {
		ledger.RetainUntil = ledger.CreatedAt.Add(90 * 24 * time.Hour)
	}
	if ledger.UpdatedAt.IsZero() {
		ledger.UpdatedAt = ledger.ObservedAt
	}
	evidence := model.OperationEvidence{
		ID:              ledger.ID,
		TenantID:        ledger.TenantID,
		ProjectID:       ledger.ProjectID,
		AppID:           ledger.AppID,
		OperationID:     ledger.OperationID,
		Type:            migrationLedgerEvidenceType(ledger.CutoverStatus),
		Source:          ledger.EvidenceSource,
		Severity:        model.OperationEvidenceSeverityInfo,
		Confidence:      model.OperationEvidenceConfidenceEvidenceBacked,
		SubjectKind:     "app_migration",
		SubjectName:     ledger.AppID,
		ObservedAt:      ledger.ObservedAt,
		CollectedAt:     ledger.UpdatedAt,
		Summary:         "application migration ledger snapshot",
		Message:         ledger.FailureReason,
		Reason:          ledger.CutoverStatus,
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload:         migrationLedgerPayload(ledger),
		PayloadVersion:  model.AppMigrationLedgerSchemaVersion,
	}
	if ledger.CutoverStatus == model.AppMigrationCutoverFailed || ledger.CutoverStatus == model.AppMigrationCutoverBlocked {
		evidence.Severity = model.OperationEvidenceSeverityError
		evidence.Confidence = model.OperationEvidenceConfidenceConfirmed
	}
	// The independent archive is the retention authority. Write it before the
	// diagnostic copy so a later app/operation/tenant cascade cannot erase the
	// mandatory audit record.
	if err := s.recordAppMigrationLedgerArchive(ledger); err != nil {
		return model.AppMigrationLedger{}, err
	}
	if _, err := s.RecordOperationEvidence(evidence); err != nil {
		return model.AppMigrationLedger{}, err
	}
	return ledger, nil
}

func (s *Store) ListAppMigrationLedgers(filter model.OperationEvidenceFilter) ([]model.AppMigrationLedger, error) {
	filter.Types = []string{
		model.OperationEvidenceTypeMigrationStarted,
		model.OperationEvidenceTypeMigrationCompleted,
		model.OperationEvidenceTypeMigrationFailed,
	}
	filter.IncludeMigrationLedger = true
	filter.Limit = unboundedMigrationEvidenceLimit
	ledgers, err := s.listAppMigrationLedgerArchive(filter)
	if err != nil {
		return nil, err
	}
	out := append([]model.AppMigrationLedger(nil), ledgers...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func (s *Store) LatestAppMigrationLedger(operationID string) (model.AppMigrationLedger, bool, error) {
	items, err := s.ListAppMigrationLedgers(model.OperationEvidenceFilter{OperationID: strings.TrimSpace(operationID), PlatformAdmin: true})
	if err != nil {
		return model.AppMigrationLedger{}, false, err
	}
	if len(items) == 0 {
		return model.AppMigrationLedger{}, false, nil
	}
	return items[0], true, nil
}

// latestMigrationLedgerForApp resolves migration history by operation
// chronology, not by the timestamp of the last evidence write. A delayed
// callback from an older migration must never override the retirement gate of
// a newer migration merely because it was recorded later.
func (s *Store) latestMigrationLedgerForApp(appID string) (model.AppMigrationLedger, model.Operation, bool, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.AppMigrationLedger{}, model.Operation{}, false, ErrInvalidInput
	}
	operations, err := s.ListOperationsFiltered("", true, OperationListFilter{
		AppID: appID,
		Types: []string{model.OperationTypeMigrate},
		Limit: 1,
	})
	if err != nil {
		return model.AppMigrationLedger{}, model.Operation{}, false, err
	}
	if len(operations) == 0 {
		// The independent audit archive intentionally survives app/operation
		// purge. Without the parent operation status we cannot prove completion,
		// so return the newest archived attempt with a non-completed synthetic
		// operation and keep retirement fail-closed for the retention window.
		byApp, ledgerErr := s.LatestAppMigrationLedgersByApp()
		if ledgerErr != nil {
			return model.AppMigrationLedger{}, model.Operation{}, false, ledgerErr
		}
		latest, exists := byApp[appID]
		if !exists {
			return model.AppMigrationLedger{}, model.Operation{}, false, nil
		}
		return latest, model.Operation{
			ID: latest.OperationID, TenantID: latest.TenantID, AppID: latest.AppID,
			Type: model.OperationTypeMigrate, CreatedAt: latest.CreatedAt,
		}, true, nil
	}
	operation := operations[len(operations)-1]
	ledger, found, err := s.LatestAppMigrationLedger(operation.ID)
	if err != nil {
		return model.AppMigrationLedger{}, model.Operation{}, false, err
	}
	// Resolving evidence by the latest operation ID avoids a global/per-app
	// evidence-window truncation from hiding a missing ledger behind snapshots
	// from older migrations.
	return ledger, operation, found, nil
}

// LatestAppMigrationLedgersByApp is used by global artifact/cache cleanup,
// which has no app-specific request context. It applies the same operation
// chronology rule as the per-app retirement gate.
func (s *Store) LatestAppMigrationLedgersByApp() (map[string]model.AppMigrationLedger, error) {
	latestArchiveByOperation, err := s.latestAppMigrationLedgerArchiveByOperation()
	if err != nil {
		return nil, err
	}
	operations, err := s.ListOperationsFiltered("", true, OperationListFilter{
		Types: []string{model.OperationTypeMigrate},
	})
	if err != nil {
		return nil, err
	}
	type migrationLedgerCandidate struct {
		operationID    string
		appID          string
		createdAt      time.Time
		ledger         model.AppMigrationLedger
		ledgerFound    bool
		operationFound bool
	}
	operationByID := make(map[string]model.Operation, len(operations))
	for _, operation := range operations {
		operationByID[strings.TrimSpace(operation.ID)] = operation
	}
	candidates := make([]migrationLedgerCandidate, 0, len(latestArchiveByOperation)+len(operations))
	seenOperations := make(map[string]struct{}, len(latestArchiveByOperation)+len(operations))
	for opID, archive := range latestArchiveByOperation {
		ledger := archive.ledger
		operation, operationFound := operationByID[opID]
		createdAt := archive.createdAt
		appID := strings.TrimSpace(ledger.AppID)
		if operationFound {
			createdAt = operation.CreatedAt
			appID = strings.TrimSpace(operation.AppID)
		}
		candidates = append(candidates, migrationLedgerCandidate{
			operationID: opID, appID: appID, createdAt: createdAt,
			ledger: ledger, ledgerFound: true, operationFound: operationFound,
		})
		seenOperations[opID] = struct{}{}
	}
	for _, operation := range operations {
		opID := strings.TrimSpace(operation.ID)
		if _, exists := seenOperations[opID]; exists {
			continue
		}
		candidates = append(candidates, migrationLedgerCandidate{
			operationID: opID, appID: strings.TrimSpace(operation.AppID),
			createdAt: operation.CreatedAt, operationFound: true,
		})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].createdAt.Equal(candidates[j].createdAt) {
			return candidates[i].operationID > candidates[j].operationID
		}
		return candidates[i].createdAt.After(candidates[j].createdAt)
	})
	out := make(map[string]model.AppMigrationLedger)
	for _, candidate := range candidates {
		appID := strings.TrimSpace(candidate.appID)
		if appID == "" {
			continue
		}
		if _, exists := out[appID]; exists {
			continue
		}
		if candidate.ledgerFound && candidate.operationFound {
			out[appID] = candidate.ledger
			continue
		}
		if candidate.ledgerFound {
			ledger := candidate.ledger
			ledger.CutoverStatus = model.AppMigrationCutoverBlocked
			ledger.OldArtifactsProtected = true
			ledger.FailureReason = "migration parent operation was purged before the audit retention window expired"
			out[appID] = ledger
			continue
		}
		// Preserve chronology even when the mandatory ledger write was lost:
		// returning a synthetic blocked record prevents global cache cleanup
		// from falling through to an older completed migration and deleting the
		// source artifact.
		out[appID] = model.AppMigrationLedger{
			TenantID:              operationByID[candidate.operationID].TenantID,
			AppID:                 appID,
			OperationID:           candidate.operationID,
			OldRuntimeID:          operationByID[candidate.operationID].SourceRuntimeID,
			NewRuntimeID:          operationByID[candidate.operationID].TargetRuntimeID,
			CutoverStatus:         model.AppMigrationCutoverBlocked,
			OldArtifactsProtected: true,
			FailureReason:         "migration ledger is missing for the latest migration operation",
		}
	}
	return out, nil
}

// MigrationArtifactsRetirementBlocked reports whether a pending, failed, or
// otherwise unverified migration still protects the app's old image/runtime
// artifacts.  A successful cutover is the only affirmative permission to
// retire them; absence of a migration ledger means this app has no migration
// retirement gate.
func (s *Store) MigrationArtifactsRetirementBlocked(appID string) (bool, string, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return false, "", ErrInvalidInput
	}
	latest, operation, found, err := s.latestMigrationLedgerForApp(appID)
	if err != nil {
		return false, "", err
	}
	if operation.ID != "" && !found {
		return true, "migration ledger is missing for the latest migration operation", nil
	}
	if !found {
		return false, "", nil
	}
	if operation.Status == model.OperationStatusCompleted &&
		(latest.CutoverStatus == model.AppMigrationCutoverVerified || latest.CutoverStatus == model.AppMigrationCutoverCompleted) {
		if err := validateAppMigrationCutover(latest, false); err != nil {
			return true, "migration cutover ledger failed re-validation: " + err.Error(), nil
		}
		return false, "", nil
	}
	reason := strings.TrimSpace(latest.FailureReason)
	if reason == "" {
		reason = "migration cutover has not been completed"
	}
	return true, reason, nil
}

// RecordMigrationArtifactRetirementBlocked emits a durable ledger event when
// cleanup is attempted while a migration is not verified.  It deliberately
// does not change the operation state or delete anything.
func (s *Store) RecordMigrationArtifactRetirementBlocked(appID, reason string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return ErrInvalidInput
	}
	latest, operation, found, err := s.latestMigrationLedgerForApp(appID)
	if err != nil {
		return err
	}
	if operation.ID != "" && !found {
		var projectID, imageRef string
		var desiredReplicas int
		if app, appErr := s.GetApp(appID); appErr == nil {
			projectID = app.ProjectID
			imageRef = strings.TrimSpace(app.Spec.Image)
			desiredReplicas = app.Spec.Replicas
		} else if !errors.Is(appErr, ErrNotFound) {
			return appErr
		}
		_, recordErr := s.RecordAppMigrationLedger(model.AppMigrationLedger{
			TenantID:              operation.TenantID,
			ProjectID:             projectID,
			AppID:                 appID,
			OperationID:           operation.ID,
			OldRuntimeID:          operation.SourceRuntimeID,
			NewRuntimeID:          operation.TargetRuntimeID,
			ImageRef:              imageRef,
			DesiredReplicas:       desiredReplicas,
			CutoverStatus:         model.AppMigrationCutoverBlocked,
			OldArtifactsProtected: true,
			FailureReason:         "migration ledger is missing for the latest migration operation",
			EvidenceSource:        model.OperationEvidenceSourceController,
		})
		return recordErr
	}
	if !found {
		return nil
	}
	if operation.ID != "" && strings.TrimSpace(operation.Status) == "" {
		// The parent operation was intentionally purged, while the independent
		// archive remains. It already protects artifacts; no child operation row
		// exists to accept another diagnostic snapshot.
		return nil
	}
	if operation.Status == model.OperationStatusCompleted &&
		(latest.CutoverStatus == model.AppMigrationCutoverVerified || latest.CutoverStatus == model.AppMigrationCutoverCompleted) {
		if err := validateAppMigrationCutover(latest, false); err != nil {
			reason = "migration cutover ledger failed re-validation: " + err.Error()
		} else {
			return nil
		}
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "old migration artifacts remain protected until cutover verification"
	}
	if latest.CutoverStatus == model.AppMigrationCutoverBlocked && latest.FailureReason == reason {
		return nil
	}
	latest.ID = ""
	latest.CutoverStatus = model.AppMigrationCutoverBlocked
	latest.OldArtifactsProtected = true
	latest.FailureReason = reason
	latest.ObservedAt = time.Now().UTC()
	latest.UpdatedAt = latest.ObservedAt
	_, err = s.RecordAppMigrationLedger(latest)
	return err
}

func (s *Store) recordMigrationFailureLedger(op model.Operation, message, source string) error {
	if op.Type != model.OperationTypeMigrate {
		return nil
	}
	var projectID, imageRef string
	var desiredReplicas int
	if app, err := s.GetApp(op.AppID); err == nil {
		projectID = app.ProjectID
		imageRef = strings.TrimSpace(app.Spec.Image)
		desiredReplicas = app.Spec.Replicas
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	ledger := model.AppMigrationLedger{
		TenantID:              op.TenantID,
		ProjectID:             projectID,
		AppID:                 op.AppID,
		OperationID:           op.ID,
		OldRuntimeID:          op.SourceRuntimeID,
		NewRuntimeID:          op.TargetRuntimeID,
		ImageRef:              imageRef,
		DesiredReplicas:       desiredReplicas,
		CutoverStatus:         model.AppMigrationCutoverFailed,
		OldArtifactsProtected: true,
		FailureReason:         strings.TrimSpace(message),
		OperatorType:          op.RequestedByType,
		OperatorID:            op.RequestedByID,
	}
	if ledger.FailureReason == "" {
		ledger.FailureReason = "migration operation failed"
	}
	if source != "" {
		ledger.EvidenceSource = strings.TrimSpace(source)
	}
	_, err := s.RecordAppMigrationLedger(ledger)
	return err
}

func (s *Store) requireMigrationCutover(operationID string) error {
	ledger, found, err := s.LatestAppMigrationLedger(operationID)
	if err != nil {
		return err
	}
	if !found {
		return errMigrationCutoverEvidenceMissing
	}
	if ledger.CutoverStatus != model.AppMigrationCutoverVerified && ledger.CutoverStatus != model.AppMigrationCutoverCompleted {
		return fmt.Errorf("%w: cutover status=%s", errMigrationCutoverEvidenceMissing, ledger.CutoverStatus)
	}
	return ValidateAppMigrationCutover(ledger)
}

func validateMigrationCutoverInState(state *model.State, op model.Operation) error {
	if state == nil {
		return errMigrationCutoverEvidenceMissing
	}
	var latest model.AppMigrationLedger
	found := false
	for _, candidate := range state.AppMigrationLedgers {
		if strings.TrimSpace(candidate.OperationID) != strings.TrimSpace(op.ID) {
			continue
		}
		if !found || candidate.UpdatedAt.After(latest.UpdatedAt) || (candidate.UpdatedAt.Equal(latest.UpdatedAt) && candidate.ID > latest.ID) {
			latest = cloneAppMigrationLedger(candidate)
			found = true
		}
	}
	if !found {
		return errMigrationCutoverEvidenceMissing
	}
	if latest.CutoverStatus != model.AppMigrationCutoverVerified && latest.CutoverStatus != model.AppMigrationCutoverCompleted {
		return fmt.Errorf("%w: cutover status=%s", errMigrationCutoverEvidenceMissing, latest.CutoverStatus)
	}
	return ValidateAppMigrationCutover(latest)
}

func (s *Store) pgLatestMigrationLedgerTx(ctx context.Context, tx *sql.Tx, operationID string) (model.AppMigrationLedger, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT ledger_json, collected_at
FROM fugue_app_migration_ledgers
WHERE operation_id = $1
ORDER BY collected_at DESC, id DESC
LIMIT 1000
`, strings.TrimSpace(operationID))
	if err != nil {
		return model.AppMigrationLedger{}, false, mapDBErr(err)
	}
	defer rows.Close()
	var latest model.AppMigrationLedger
	found := false
	for rows.Next() {
		var payloadJSON []byte
		var collectedAt time.Time
		if err := rows.Scan(&payloadJSON, &collectedAt); err != nil {
			return model.AppMigrationLedger{}, false, mapDBErr(err)
		}
		var candidate model.AppMigrationLedger
		if err := json.Unmarshal(payloadJSON, &candidate); err != nil {
			continue
		}
		candidate = model.NormalizeAppMigrationLedger(candidate, collectedAt)
		if !found {
			latest, found = candidate, true
		}
	}
	if err := rows.Err(); err != nil {
		return model.AppMigrationLedger{}, false, mapDBErr(err)
	}
	return latest, found, nil
}
