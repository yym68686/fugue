package store

import (
	"bytes"
	"encoding/json"
	"strings"

	"fugue/internal/model"
)

type persistedAppSourceState struct {
	OriginSource *model.AppSource `json:"origin_source,omitempty"`
	BuildSource  *model.AppSource `json:"build_source,omitempty"`
}

type persistedOperationSourceState struct {
	DesiredSource       *model.AppSource `json:"desired_source,omitempty"`
	DesiredOriginSource *model.AppSource `json:"desired_origin_source,omitempty"`
}

func marshalAppSourceState(app model.App) ([]byte, error) {
	normalized := app
	model.NormalizeAppSourceState(&normalized)
	if normalized.OriginSource == nil && normalized.BuildSource == nil {
		return nil, nil
	}
	return json.Marshal(persistedAppSourceState{
		OriginSource: normalized.OriginSource,
		BuildSource:  normalized.BuildSource,
	})
}

func decodeAppSourceState(raw []byte) (*model.AppSource, *model.AppSource, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, err
	}
	if len(fields) == 0 {
		return nil, nil, nil
	}
	if _, ok := fields["origin_source"]; ok {
		return decodeAppSourceStateEnvelope(raw)
	}
	if _, ok := fields["build_source"]; ok {
		return decodeAppSourceStateEnvelope(raw)
	}
	return decodeLegacyAppSourceState(raw)
}

func decodeAppSourceStateEnvelope(raw []byte) (*model.AppSource, *model.AppSource, error) {
	var envelope persistedAppSourceState
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, nil, err
	}
	app := model.App{
		OriginSource: model.CloneAppSource(envelope.OriginSource),
		BuildSource:  model.CloneAppSource(envelope.BuildSource),
	}
	model.NormalizeAppSourceState(&app)
	return model.CloneAppSource(app.OriginSource), model.CloneAppSource(app.BuildSource), nil
}

func decodeLegacyAppSourceState(raw []byte) (*model.AppSource, *model.AppSource, error) {
	var legacy model.AppSource
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return nil, nil, err
	}
	if appSourceEmpty(legacy) {
		return nil, nil, nil
	}
	source := model.CloneAppSource(&legacy)
	return model.CloneAppSource(source), source, nil
}

func marshalOperationSourceState(op model.Operation) ([]byte, error) {
	if op.DesiredSource == nil && op.DesiredOriginSource == nil {
		return nil, nil
	}
	return json.Marshal(persistedOperationSourceState{
		DesiredSource:       model.CloneAppSource(op.DesiredSource),
		DesiredOriginSource: model.CloneAppSource(op.DesiredOriginSource),
	})
}

func decodeOperationSourceState(raw []byte) (*model.AppSource, *model.AppSource, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, err
	}
	if len(fields) == 0 {
		return nil, nil, nil
	}
	if _, ok := fields["desired_source"]; ok {
		return decodeOperationSourceStateEnvelope(raw)
	}
	if _, ok := fields["desired_origin_source"]; ok {
		return decodeOperationSourceStateEnvelope(raw)
	}
	return decodeLegacyAppSourceState(raw)
}

func decodeOperationSourceStateEnvelope(raw []byte) (*model.AppSource, *model.AppSource, error) {
	var envelope persistedOperationSourceState
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, nil, err
	}
	op := model.Operation{
		DesiredSource:       model.CloneAppSource(envelope.DesiredSource),
		DesiredOriginSource: model.CloneAppSource(envelope.DesiredOriginSource),
	}
	model.NormalizeOperationSourceState(&op)
	return model.CloneAppSource(op.DesiredSource), model.CloneAppSource(op.DesiredOriginSource), nil
}

func appSourceEmpty(source model.AppSource) bool {
	return strings.TrimSpace(source.Type) == "" &&
		strings.TrimSpace(source.RepoURL) == "" &&
		strings.TrimSpace(source.RepoBranch) == "" &&
		strings.TrimSpace(source.RepoAuthToken) == "" &&
		strings.TrimSpace(source.ImageRef) == "" &&
		strings.TrimSpace(source.ResolvedImageRef) == "" &&
		strings.TrimSpace(source.UploadID) == "" &&
		strings.TrimSpace(source.UploadFilename) == "" &&
		strings.TrimSpace(source.ArchiveSHA256) == "" &&
		source.ArchiveSizeBytes == 0 &&
		strings.TrimSpace(source.SourceDir) == "" &&
		strings.TrimSpace(source.BuildStrategy) == "" &&
		strings.TrimSpace(source.CommitSHA) == "" &&
		strings.TrimSpace(source.CommitCommittedAt) == "" &&
		strings.TrimSpace(source.DockerfilePath) == "" &&
		strings.TrimSpace(source.BuildContextDir) == "" &&
		strings.TrimSpace(source.ImageNameSuffix) == "" &&
		strings.TrimSpace(source.ComposeService) == "" &&
		len(source.ComposeDependsOn) == 0 &&
		strings.TrimSpace(source.DetectedProvider) == "" &&
		strings.TrimSpace(source.DetectedStack) == ""
}
