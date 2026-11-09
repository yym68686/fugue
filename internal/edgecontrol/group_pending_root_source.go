package edgecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

const groupPendingRootSourceSchema = "fugue.edge-control.pending-root-source.v1"

// Original values only: no client signature_verified, custody or policy flags.
// Keeping this envelope separate from the registration lets the registry
// independently derive every concrete typed reference under its own trust.
type groupPendingRootSource struct {
	GroupID       string                    `json:"edge_group_id"`
	Input         GroupCompileInput         `json:"input"`
	Publication   GroupAuthorityLedgerEntry `json:"publication"`
	Bundle        model.EdgeRouteBundle     `json:"bundle"`
	RecoveryEpoch uint64                    `json:"recovery_epoch"`
}

func pendingGroupRootRequest(group string, input GroupCompileInput, publication GroupAuthorityLedgerEntry, bundle model.EdgeRouteBundle, recovery uint64, binding GroupOriginalPublicationBinding) (inputarchive.PendingRootRequest, error) {
	r, err := pendingGroupRootRegistration(group, binding)
	if err != nil {
		return inputarchive.PendingRootRequest{}, err
	}
	raw, err := json.Marshal(groupPendingRootSource{group, input, publication, bundle, recovery})
	if err != nil {
		return inputarchive.PendingRootRequest{}, ErrGroupPendingRoot
	}
	request := inputarchive.PendingRootRequest{Schema: inputarchive.PendingRootRequestSchema, Registration: r, SourceSchema: groupPendingRootSourceSchema, Source: raw}
	if _, _, err := request.Canonical(); err != nil {
		return inputarchive.PendingRootRequest{}, ErrGroupPendingRoot
	}
	return request, nil
}

// GroupPendingRootSourceVerifier is configured for ONE authenticated owner.
// The transport must select it from its independently authenticated principal,
// never from a request's owner/group or a caller-supplied key/path. These keys
// authenticate the full native input MAC and original primary publication;
// they do not prove complete file custody, restore pins or resource admission.
type GroupPendingRootSourceVerifier struct {
	group string
	path  string
}

func NewGroupPendingRootSourceVerifier(keyringDir, group string) (*GroupPendingRootSourceVerifier, error) {
	if !filepath.IsAbs(keyringDir) || filepath.Clean(keyringDir) != keyringDir || len(group) > 128 || !edgeGroupIDPattern.MatchString(group) {
		return nil, ErrGroupPendingRoot
	}
	v := &GroupPendingRootSourceVerifier{group: group, path: filepath.Join(keyringDir, group+".json")}
	if _, err := loadGroupBundleSigningKeyring(v.path, v.group); err != nil {
		return nil, ErrGroupPendingRoot
	}
	return v, nil
}

type groupPendingRootSourceWitness struct {
	verifier *GroupPendingRootSourceVerifier
	hash     string
	keys     groupBundleSigningKey
}

func (v *GroupPendingRootSourceVerifier) VerifyPendingRootSource(ctx context.Context, request inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
	if v == nil || ctx.Err() != nil || request.SourceSchema != groupPendingRootSourceSchema || request.Registration.Owner != "edge-control/"+v.group {
		return nil, ErrGroupPendingRoot
	}
	_, hash, err := request.Canonical()
	if err != nil {
		return nil, ErrGroupPendingRoot
	}
	var source groupPendingRootSource
	if err := json.Unmarshal(request.Source, &source); err != nil {
		return nil, ErrGroupPendingRoot
	}
	// Require the native canonical encoding, rejecting unknown/duplicate fields,
	// ambiguous trailing values and a second representation of signed content.
	raw, err := json.Marshal(source)
	if err != nil || !bytes.Equal(raw, request.Source) || source.GroupID != v.group {
		return nil, ErrGroupPendingRoot
	}
	keys, err := loadGroupBundleSigningKeyring(v.path, v.group)
	if err != nil {
		return nil, ErrGroupPendingRoot
	}
	binding, err := bindGroupOriginalPublication(v.group, source.Input, source.Publication, source.Bundle, source.RecoveryEpoch, keys)
	if err != nil {
		return nil, ErrGroupPendingRoot
	}
	derived, err := pendingGroupRootRegistration(v.group, binding)
	if err != nil || !reflect.DeepEqual(derived, request.Registration) {
		return nil, ErrGroupPendingRoot
	}
	witness := &groupPendingRootSourceWitness{verifier: v, hash: hash, keys: keys}
	if err := witness.CheckPendingRootSource(ctx, request); err != nil {
		return nil, err
	}
	return witness, nil
}

func (w *groupPendingRootSourceWitness) CheckPendingRootSource(ctx context.Context, request inputarchive.PendingRootRequest) error {
	if w == nil || w.verifier == nil || ctx.Err() != nil {
		return ErrGroupPendingRoot
	}
	_, hash, err := request.Canonical()
	if err != nil || hash != w.hash {
		return ErrGroupPendingRoot
	}
	keys, err := loadGroupBundleSigningKeyring(w.verifier.path, w.verifier.group)
	if err != nil || ctx.Err() != nil || !reflect.DeepEqual(keys, w.keys) {
		return ErrGroupPendingRoot
	}
	return nil
}
