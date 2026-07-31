package cli

import "fugue/internal/model"

// sanitizeAppBackupStatusForOutput is the CLI-side safety boundary for backup
// status responses. It protects callers using older API servers and also
// covers diagnostic strings that can carry serialized events or receipts.
// Stable resource identifiers and integrity metadata remain available for
// incident correlation.
func sanitizeAppBackupStatusForOutput(status appBackupStatusResponse) appBackupStatusResponse {
	out := status
	out.App = sanitizeBackupStatusApp(status.App)

	if len(status.Policies) > 0 {
		out.Policies = append([]model.BackupPolicy(nil), status.Policies...)
		for index := range out.Policies {
			out.Policies[index].DisabledReason = redactDiagnosticString(out.Policies[index].DisabledReason)
		}
	}
	if len(status.Artifacts) > 0 {
		out.Artifacts = make([]model.BackupArtifact, len(status.Artifacts))
		for index, artifact := range status.Artifacts {
			out.Artifacts[index] = sanitizeBackupStatusArtifact(artifact)
		}
	}
	if len(status.Posture) > 0 {
		out.Posture = append([]model.BackupPosture(nil), status.Posture...)
		for index := range out.Posture {
			out.Posture[index].Message = redactDiagnosticString(out.Posture[index].Message)
		}
	}
	return out
}

func sanitizeBackupStatusApp(app model.App) model.App {
	out := redactAppForOutput(app)
	out.Source = sanitizeBackupStatusSource(out.Source)
	out.OriginSource = sanitizeBackupStatusSource(out.OriginSource)
	out.BuildSource = sanitizeBackupStatusSource(out.BuildSource)
	out.Spec.Image = redactDiagnosticString(out.Spec.Image)
	for index := range out.Spec.Command {
		out.Spec.Command[index] = redactDiagnosticString(out.Spec.Command[index])
	}
	for index := range out.Spec.Args {
		out.Spec.Args[index] = redactDiagnosticString(out.Spec.Args[index])
	}
	if out.Spec.Workspace != nil && out.Spec.Workspace.ResetToken != "" {
		workspace := *out.Spec.Workspace
		workspace.ResetToken = redactedSecretValue
		out.Spec.Workspace = &workspace
	}
	if out.Spec.PersistentStorage != nil && out.Spec.PersistentStorage.ResetToken != "" {
		storage := *out.Spec.PersistentStorage
		storage.ResetToken = redactedSecretValue
		out.Spec.PersistentStorage = &storage
	}
	out.Status = sanitizeBackupStatusAppState(out.Status)
	if out.StoredStatus != nil {
		stored := sanitizeBackupStatusAppState(*out.StoredStatus)
		out.StoredStatus = &stored
	}
	if out.ObservedStatus != nil {
		observed := *out.ObservedStatus
		observed.ImageRef = redactDiagnosticString(observed.ImageRef)
		observed.Message = redactDiagnosticString(observed.Message)
		observed.EvidenceSources = redactDiagnosticStringSlice(observed.EvidenceSources)
		observed.InvariantViolations = redactDiagnosticStringSlice(observed.InvariantViolations)
		out.ObservedStatus = &observed
	}
	if out.Route != nil {
		route := *out.Route
		route.PublicURL = redactDiagnosticString(route.PublicURL)
		out.Route = &route
	}
	for index := range out.BackingServices {
		if out.BackingServices[index].RuntimeStatus == nil {
			continue
		}
		runtimeStatus := *out.BackingServices[index].RuntimeStatus
		runtimeStatus.Message = redactDiagnosticString(runtimeStatus.Message)
		out.BackingServices[index].RuntimeStatus = &runtimeStatus
	}
	return out
}

func sanitizeBackupStatusSource(source *model.AppSource) *model.AppSource {
	if source == nil {
		return nil
	}
	out := *source
	out.RepoURL = redactDiagnosticString(out.RepoURL)
	out.ImageRef = redactDiagnosticString(out.ImageRef)
	out.ResolvedImageRef = redactDiagnosticString(out.ResolvedImageRef)
	return &out
}

func sanitizeBackupStatusAppState(status model.AppStatus) model.AppStatus {
	out := status
	out.LastMessage = redactDiagnosticString(out.LastMessage)
	if status.LastFailedOperation != nil {
		failure := *status.LastFailedOperation
		failure.ErrorMessage = redactDiagnosticString(failure.ErrorMessage)
		failure.ResultMessage = redactDiagnosticString(failure.ResultMessage)
		out.LastFailedOperation = &failure
	}
	if status.SourceSync != nil {
		sourceSync := *status.SourceSync
		sourceSync.LastErrorMessage = redactDiagnosticString(sourceSync.LastErrorMessage)
		out.SourceSync = &sourceSync
	}
	return out
}

func sanitizeBackupStatusArtifact(artifact model.BackupArtifact) model.BackupArtifact {
	out := artifact
	out.Manifest = artifact.Manifest
	out.Manifest.Invariants = redactDiagnosticStringMap(artifact.Manifest.Invariants)
	out.Manifest.Metadata = redactDiagnosticStringMap(artifact.Manifest.Metadata)
	if len(artifact.Manifest.Files) > 0 {
		out.Manifest.Files = append([]model.BackupManifestFile(nil), artifact.Manifest.Files...)
	}
	if artifact.Manifest.Encryption != nil {
		encryption := *artifact.Manifest.Encryption
		out.Manifest.Encryption = &encryption
	}
	return out
}
