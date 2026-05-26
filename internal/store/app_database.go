package store

import (
	"crypto/subtle"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) CreateAppDatabaseImportJob(job model.AppDatabaseImportJob) (model.AppDatabaseImportJob, error) {
	job.AppID = strings.TrimSpace(job.AppID)
	job.TenantID = strings.TrimSpace(job.TenantID)
	job.SourceUploadID = strings.TrimSpace(job.SourceUploadID)
	job.SourceUploadFilename = strings.TrimSpace(job.SourceUploadFilename)
	job.SourceUploadSHA256 = strings.TrimSpace(job.SourceUploadSHA256)
	job.Label = strings.TrimSpace(job.Label)
	job.Format = normalizeAppDatabaseImportFormat(job.Format)
	job.Status = normalizeAppDatabaseImportStatus(job.Status)
	job.RequestedByType = strings.TrimSpace(job.RequestedByType)
	job.RequestedByID = strings.TrimSpace(job.RequestedByID)
	job.RetryOfJobID = strings.TrimSpace(job.RetryOfJobID)
	if job.AppID == "" || job.TenantID == "" || job.SourceUploadID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	if job.Status == "" {
		job.Status = model.OperationStatusPending
	}
	if job.Format == "" {
		job.Format = model.AppDatabaseImportFormatAuto
	}
	if s.usingDatabase() {
		return s.pgCreateAppDatabaseImportJob(job)
	}

	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, job.AppID)
		if appIndex < 0 {
			return ErrNotFound
		}
		if strings.TrimSpace(state.Apps[appIndex].TenantID) != job.TenantID {
			return ErrNotFound
		}
		now := time.Now().UTC()
		if strings.TrimSpace(job.ID) == "" {
			job.ID = model.NewID("dbimport")
		}
		if job.CreatedAt.IsZero() {
			job.CreatedAt = now
		}
		job.UpdatedAt = now
		state.AppDatabaseImportJobs = append(state.AppDatabaseImportJobs, job)
		return nil
	})
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	return redactAppDatabaseImportJob(job), nil
}

func (s *Store) ListAppDatabaseImportJobs(appID string) ([]model.AppDatabaseImportJob, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListAppDatabaseImportJobs(appID)
	}

	jobs := []model.AppDatabaseImportJob{}
	err := s.withLockedState(false, func(state *model.State) error {
		if findApp(state, appID) < 0 {
			return ErrNotFound
		}
		for _, job := range state.AppDatabaseImportJobs {
			if strings.TrimSpace(job.AppID) == appID {
				jobs = append(jobs, redactAppDatabaseImportJob(job))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
		}
		return jobs[i].ID > jobs[j].ID
	})
	return jobs, nil
}

func (s *Store) GetAppDatabaseImportJob(appID, jobID string) (model.AppDatabaseImportJob, error) {
	appID = strings.TrimSpace(appID)
	jobID = strings.TrimSpace(jobID)
	if appID == "" || jobID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAppDatabaseImportJob(appID, jobID)
	}

	var job model.AppDatabaseImportJob
	err := s.withLockedState(false, func(state *model.State) error {
		index := findAppDatabaseImportJob(state, jobID)
		if index < 0 || strings.TrimSpace(state.AppDatabaseImportJobs[index].AppID) != appID {
			return ErrNotFound
		}
		job = state.AppDatabaseImportJobs[index]
		return nil
	})
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	return redactAppDatabaseImportJob(job), nil
}

func (s *Store) ListPendingAppDatabaseImportJobs(limit int) ([]model.AppDatabaseImportJob, error) {
	if limit <= 0 {
		limit = 10
	}
	if s.usingDatabase() {
		return s.pgListPendingAppDatabaseImportJobs(limit)
	}

	jobs := []model.AppDatabaseImportJob{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, job := range state.AppDatabaseImportJobs {
			if strings.TrimSpace(job.Status) != model.OperationStatusPending {
				continue
			}
			jobs = append(jobs, redactAppDatabaseImportJob(job))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
		}
		return jobs[i].ID < jobs[j].ID
	})
	if len(jobs) > limit {
		jobs = jobs[:limit]
	}
	return jobs, nil
}

func (s *Store) ClaimAppDatabaseImportJob(jobID string) (model.AppDatabaseImportJob, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgClaimAppDatabaseImportJob(jobID)
	}

	var job model.AppDatabaseImportJob
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppDatabaseImportJob(state, jobID)
		if index < 0 {
			return ErrNotFound
		}
		if state.AppDatabaseImportJobs[index].Status != model.OperationStatusPending {
			return ErrConflict
		}
		now := time.Now().UTC()
		state.AppDatabaseImportJobs[index].Status = model.OperationStatusRunning
		state.AppDatabaseImportJobs[index].StartedAt = &now
		state.AppDatabaseImportJobs[index].UpdatedAt = now
		job = state.AppDatabaseImportJobs[index]
		return nil
	})
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	return redactAppDatabaseImportJob(job), nil
}

func (s *Store) AppendAppDatabaseImportJobLog(jobID, message string) (model.AppDatabaseImportJob, error) {
	jobID = strings.TrimSpace(jobID)
	message = strings.TrimSpace(message)
	if jobID == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAppendAppDatabaseImportJobLog(jobID, message)
	}

	var job model.AppDatabaseImportJob
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppDatabaseImportJob(state, jobID)
		if index < 0 {
			return ErrNotFound
		}
		now := time.Now().UTC()
		if message != "" {
			state.AppDatabaseImportJobs[index].Logs = append(state.AppDatabaseImportJobs[index].Logs, model.AppDatabaseImportJobLog{
				At:      now,
				Message: message,
			})
		}
		state.AppDatabaseImportJobs[index].UpdatedAt = now
		job = state.AppDatabaseImportJobs[index]
		return nil
	})
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	return redactAppDatabaseImportJob(job), nil
}

func (s *Store) CompleteAppDatabaseImportJob(jobID, status, message, errorMessage string) (model.AppDatabaseImportJob, error) {
	jobID = strings.TrimSpace(jobID)
	status = normalizeTerminalAppDatabaseImportStatus(status)
	if jobID == "" || status == "" {
		return model.AppDatabaseImportJob{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCompleteAppDatabaseImportJob(jobID, status, message, errorMessage)
	}

	var job model.AppDatabaseImportJob
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppDatabaseImportJob(state, jobID)
		if index < 0 {
			return ErrNotFound
		}
		if state.AppDatabaseImportJobs[index].Status != model.OperationStatusRunning {
			return ErrConflict
		}
		now := time.Now().UTC()
		state.AppDatabaseImportJobs[index].Status = status
		state.AppDatabaseImportJobs[index].ResultMessage = strings.TrimSpace(message)
		state.AppDatabaseImportJobs[index].ErrorMessage = strings.TrimSpace(errorMessage)
		state.AppDatabaseImportJobs[index].CompletedAt = &now
		state.AppDatabaseImportJobs[index].UpdatedAt = now
		job = state.AppDatabaseImportJobs[index]
		return nil
	})
	if err != nil {
		return model.AppDatabaseImportJob{}, err
	}
	return redactAppDatabaseImportJob(job), nil
}

func (s *Store) ListAppDatabaseAccessGrants(appID string) ([]model.AppDatabaseAccessGrant, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListAppDatabaseAccessGrants(appID)
	}

	grants := []model.AppDatabaseAccessGrant{}
	err := s.withLockedState(false, func(state *model.State) error {
		if findApp(state, appID) < 0 {
			return ErrNotFound
		}
		for _, grant := range state.AppDatabaseAccessGrants {
			if strings.TrimSpace(grant.AppID) != appID {
				continue
			}
			normalizeAppDatabaseAccessGrantForRead(&grant)
			grants = append(grants, redactAppDatabaseAccessGrant(grant))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(grants, func(i, j int) bool {
		if !grants[i].CreatedAt.Equal(grants[j].CreatedAt) {
			return grants[i].CreatedAt.After(grants[j].CreatedAt)
		}
		return grants[i].ID > grants[j].ID
	})
	return grants, nil
}

func (s *Store) CreateAppDatabaseAccessGrant(grant model.AppDatabaseAccessGrant) (model.AppDatabaseAccessGrant, string, error) {
	grant.AppID = strings.TrimSpace(grant.AppID)
	grant.TenantID = strings.TrimSpace(grant.TenantID)
	grant.Label = strings.TrimSpace(grant.Label)
	grant.Mode = normalizeAppDatabaseAccessMode(grant.Mode)
	grant.RequestedByType = strings.TrimSpace(grant.RequestedByType)
	grant.RequestedByID = strings.TrimSpace(grant.RequestedByID)
	if grant.AppID == "" || grant.TenantID == "" || grant.Mode == "" {
		return model.AppDatabaseAccessGrant{}, "", ErrInvalidInput
	}
	secret := model.NewSecret("fugue_db")
	grant.TokenPrefix = model.SecretPrefix(secret)
	grant.TokenHash = model.HashSecret(secret)
	grant.Status = model.AppDatabaseAccessGrantStatusActive
	if s.usingDatabase() {
		return s.pgCreateAppDatabaseAccessGrant(grant, secret)
	}

	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, grant.AppID)
		if appIndex < 0 {
			return ErrNotFound
		}
		if strings.TrimSpace(state.Apps[appIndex].TenantID) != grant.TenantID {
			return ErrNotFound
		}
		now := time.Now().UTC()
		if strings.TrimSpace(grant.ID) == "" {
			grant.ID = model.NewID("dbaccess")
		}
		if grant.Label == "" {
			grant.Label = "database tunnel"
		}
		grant.CreatedAt = now
		grant.UpdatedAt = now
		state.AppDatabaseAccessGrants = append(state.AppDatabaseAccessGrants, grant)
		return nil
	})
	if err != nil {
		return model.AppDatabaseAccessGrant{}, "", err
	}
	normalizeAppDatabaseAccessGrantForRead(&grant)
	return redactAppDatabaseAccessGrant(grant), secret, nil
}

func (s *Store) RevokeAppDatabaseAccessGrant(appID, grantID string) (bool, error) {
	appID = strings.TrimSpace(appID)
	grantID = strings.TrimSpace(grantID)
	if appID == "" || grantID == "" {
		return false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRevokeAppDatabaseAccessGrant(appID, grantID)
	}

	var removed bool
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppDatabaseAccessGrant(state, appID, grantID)
		if index < 0 {
			return nil
		}
		now := time.Now().UTC()
		if state.AppDatabaseAccessGrants[index].RevokedAt == nil {
			state.AppDatabaseAccessGrants[index].RevokedAt = &now
			state.AppDatabaseAccessGrants[index].Status = model.AppDatabaseAccessGrantStatusRevoked
			state.AppDatabaseAccessGrants[index].UpdatedAt = now
		}
		removed = true
		return nil
	})
	return removed, err
}

func (s *Store) AuthenticateAppDatabaseAccessGrant(appID, grantID, secret string) (model.AppDatabaseAccessGrant, error) {
	appID = strings.TrimSpace(appID)
	grantID = strings.TrimSpace(grantID)
	secret = strings.TrimSpace(secret)
	if appID == "" || grantID == "" || secret == "" {
		return model.AppDatabaseAccessGrant{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAuthenticateAppDatabaseAccessGrant(appID, grantID, secret)
	}

	var grant model.AppDatabaseAccessGrant
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAppDatabaseAccessGrant(state, appID, grantID)
		if index < 0 {
			return ErrNotFound
		}
		current := state.AppDatabaseAccessGrants[index]
		if subtle.ConstantTimeCompare([]byte(current.TokenHash), []byte(model.HashSecret(secret))) != 1 {
			return ErrNotFound
		}
		normalizeAppDatabaseAccessGrantForRead(&current)
		if current.Status != model.AppDatabaseAccessGrantStatusActive {
			return ErrConflict
		}
		now := time.Now().UTC()
		current.LastUsedAt = &now
		current.UpdatedAt = now
		state.AppDatabaseAccessGrants[index] = current
		grant = current
		return nil
	})
	if err != nil {
		return model.AppDatabaseAccessGrant{}, err
	}
	return redactAppDatabaseAccessGrant(grant), nil
}

func normalizeAppDatabaseImportFormat(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseImportFormatAuto:
		return model.AppDatabaseImportFormatAuto
	case model.AppDatabaseImportFormatSQL:
		return model.AppDatabaseImportFormatSQL
	case model.AppDatabaseImportFormatCustom:
		return model.AppDatabaseImportFormatCustom
	default:
		return ""
	}
}

func normalizeAppDatabaseImportStatus(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.OperationStatusPending:
		return model.OperationStatusPending
	case model.OperationStatusRunning:
		return model.OperationStatusRunning
	case model.OperationStatusCompleted:
		return model.OperationStatusCompleted
	case model.OperationStatusFailed:
		return model.OperationStatusFailed
	default:
		return ""
	}
}

func normalizeTerminalAppDatabaseImportStatus(raw string) string {
	switch normalizeAppDatabaseImportStatus(raw) {
	case model.OperationStatusCompleted:
		return model.OperationStatusCompleted
	case model.OperationStatusFailed:
		return model.OperationStatusFailed
	default:
		return ""
	}
}

func normalizeAppDatabaseAccessMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseAccessModeReadWrite:
		return model.AppDatabaseAccessModeReadWrite
	case model.AppDatabaseAccessModeReadOnly:
		return model.AppDatabaseAccessModeReadOnly
	default:
		return ""
	}
}

func normalizeAppDatabaseAccessGrantForRead(grant *model.AppDatabaseAccessGrant) {
	if grant == nil {
		return
	}
	grant.Mode = normalizeAppDatabaseAccessMode(grant.Mode)
	switch {
	case grant.RevokedAt != nil:
		grant.Status = model.AppDatabaseAccessGrantStatusRevoked
	case grant.ExpiresAt != nil && time.Now().UTC().After(grant.ExpiresAt.UTC()):
		grant.Status = model.AppDatabaseAccessGrantStatusExpired
	case strings.TrimSpace(grant.Status) == "":
		grant.Status = model.AppDatabaseAccessGrantStatusActive
	}
	if strings.TrimSpace(grant.Label) == "" {
		grant.Label = "database tunnel"
	}
}

func redactAppDatabaseAccessGrant(grant model.AppDatabaseAccessGrant) model.AppDatabaseAccessGrant {
	normalizeAppDatabaseAccessGrantForRead(&grant)
	grant.TokenHash = ""
	return grant
}

func redactAppDatabaseImportJob(job model.AppDatabaseImportJob) model.AppDatabaseImportJob {
	job.Format = normalizeAppDatabaseImportFormat(job.Format)
	job.Status = normalizeAppDatabaseImportStatus(job.Status)
	job.Logs = append([]model.AppDatabaseImportJobLog(nil), job.Logs...)
	return job
}

func findAppDatabaseImportJob(state *model.State, jobID string) int {
	jobID = strings.TrimSpace(jobID)
	if state == nil || jobID == "" {
		return -1
	}
	for idx := range state.AppDatabaseImportJobs {
		if strings.TrimSpace(state.AppDatabaseImportJobs[idx].ID) == jobID {
			return idx
		}
	}
	return -1
}

func findAppDatabaseAccessGrant(state *model.State, appID, grantID string) int {
	appID = strings.TrimSpace(appID)
	grantID = strings.TrimSpace(grantID)
	if state == nil || appID == "" || grantID == "" {
		return -1
	}
	for idx := range state.AppDatabaseAccessGrants {
		grant := state.AppDatabaseAccessGrants[idx]
		if strings.TrimSpace(grant.AppID) == appID && strings.TrimSpace(grant.ID) == grantID {
			return idx
		}
	}
	return -1
}

func deleteAppDatabaseImportJobsByTenant(jobs []model.AppDatabaseImportJob, tenantID string) []model.AppDatabaseImportJob {
	tenantID = strings.TrimSpace(tenantID)
	out := jobs[:0]
	for _, job := range jobs {
		if strings.TrimSpace(job.TenantID) == tenantID {
			continue
		}
		out = append(out, job)
	}
	return out
}

func deleteAppDatabaseImportJobsByApp(jobs []model.AppDatabaseImportJob, appID string) []model.AppDatabaseImportJob {
	appID = strings.TrimSpace(appID)
	out := jobs[:0]
	for _, job := range jobs {
		if strings.TrimSpace(job.AppID) == appID {
			continue
		}
		out = append(out, job)
	}
	return out
}

func deleteAppDatabaseImportJobsByAppIDs(jobs []model.AppDatabaseImportJob, appIDs []string) []model.AppDatabaseImportJob {
	remove := trimmedStringSet(appIDs)
	if len(remove) == 0 {
		return jobs
	}
	out := jobs[:0]
	for _, job := range jobs {
		if _, ok := remove[strings.TrimSpace(job.AppID)]; ok {
			continue
		}
		out = append(out, job)
	}
	return out
}

func deleteAppDatabaseAccessGrantsByTenant(grants []model.AppDatabaseAccessGrant, tenantID string) []model.AppDatabaseAccessGrant {
	tenantID = strings.TrimSpace(tenantID)
	out := grants[:0]
	for _, grant := range grants {
		if strings.TrimSpace(grant.TenantID) == tenantID {
			continue
		}
		out = append(out, grant)
	}
	return out
}

func deleteAppDatabaseAccessGrantsByApp(grants []model.AppDatabaseAccessGrant, appID string) []model.AppDatabaseAccessGrant {
	appID = strings.TrimSpace(appID)
	out := grants[:0]
	for _, grant := range grants {
		if strings.TrimSpace(grant.AppID) == appID {
			continue
		}
		out = append(out, grant)
	}
	return out
}

func deleteAppDatabaseAccessGrantsByAppIDs(grants []model.AppDatabaseAccessGrant, appIDs []string) []model.AppDatabaseAccessGrant {
	remove := trimmedStringSet(appIDs)
	if len(remove) == 0 {
		return grants
	}
	out := grants[:0]
	for _, grant := range grants {
		if _, ok := remove[strings.TrimSpace(grant.AppID)]; ok {
			continue
		}
		out = append(out, grant)
	}
	return out
}
