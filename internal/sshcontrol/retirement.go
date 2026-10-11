package sshcontrol

import (
	"context"
	"errors"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
)

type RetirementRuntime interface {
	RetirementPolicy(context.Context, model.ManagedSSHPublicBinding, managedssh.ServingSlot) ([]EdgeConsumerInstance, time.Duration, error)
	StopDrainedSlot(context.Context, managedssh.ServingSlot) (bool, error)
}

type EdgeConsumerInstance struct {
	EdgeID      string
	EdgeGroupID string
	InstanceUID string
}

func (reconciler *Reconciler) retireSlots(ctx context.Context, state managedssh.ControlState, now time.Time) error {
	runtime, supported := reconciler.Runtime.(RetirementRuntime)
	if !supported || state.Active == nil || len(state.Draining) == 0 || state.RollbackHold {
		return nil
	}
	binding, err := reconciler.Store.GetManagedSSHBinding(state.TenantID, state.AppID)
	if err != nil {
		return err
	}
	edges, retention, err := runtime.RetirementPolicy(ctx, binding, *state.Active)
	if err != nil {
		return err
	}
	if len(edges) == 0 || retention < time.Hour {
		return errors.New("SSH slot retirement requires an explicit consumer set and rollback retention of at least one hour")
	}
	if !state.Active.Verified(now) {
		return managedssh.ErrCandidateUnverified
	}
	for _, edge := range edges {
		ack, err := reconciler.Store.GetEdgeSSHAcknowledgement(edge.EdgeID, edge.InstanceUID)
		if err != nil {
			return err
		}
		ready, err := retirementAcknowledged(state, binding, edge, ack, now)
		if err != nil || !ready {
			return err
		}
	}
	for index := range state.Draining {
		slot := state.Draining[index]
		if slot.DrainingSince == nil || now.Sub(*slot.DrainingSince) < retention {
			continue
		}
		if slot.RetiringAt == nil {
			signer, err := managedssh.LoadConfigurationSigner(reconciler.SigningKeyFile)
			if err != nil {
				return err
			}
			configuration := slot.Artifact.Configuration
			configuration.Generation = state.Generation + 1
			configuration.Draining = true
			artifact, err := managedssh.SignArtifact(configuration, signer.KeyID, signer.PrivateKey)
			clear(signer.PrivateKey)
			if err != nil {
				return err
			}
			slot.Artifact, slot.Evidence, slot.RetiringAt = artifact, nil, &now
			state.Draining[index] = slot
			state.Generation++
			state.Revision++
			return reconciler.Store.SaveManagedSSHControl(state.Revision-1, state)
		}
		if !slot.Artifact.Configuration.Draining || now.Sub(*slot.RetiringAt) < 30*time.Second {
			continue
		}
		stopped, err := runtime.StopDrainedSlot(ctx, slot)
		if err != nil {
			return err
		}
		if !stopped {
			continue
		}
		if len(state.Retired) >= 64 {
			return errors.New("SSH retired-slot retention limit requires archival before further retirement")
		}
		slot.RetiredAt = &now
		state.Retired = append(state.Retired, slot)
		state.Draining = append(state.Draining[:index:index], state.Draining[index+1:]...)
		state.Revision++
		return reconciler.Store.SaveManagedSSHControl(state.Revision-1, state)
	}
	return nil
}

func retirementAcknowledged(state managedssh.ControlState, binding model.ManagedSSHPublicBinding, edge EdgeConsumerInstance, ack model.EdgeSSHAcknowledgement, now time.Time) (bool, error) {
	if state.Active == nil || !ack.Persisted || ack.Connections == nil || !ack.ExpiresAt.After(now) || ack.EdgeID != edge.EdgeID || ack.InstanceUID != edge.InstanceUID || ack.EdgeGroupID != edge.EdgeGroupID || ack.ObservedAt.After(now.Add(5*time.Second)) || ack.LoadedAt.After(now.Add(5*time.Second)) || state.ActivatedAt == nil || ack.LoadedAt.Before(*state.ActivatedAt) || ack.RouteDigest != model.SSHRoutesDigest(ack.Routes) {
		return false, errors.New("SSH edge acknowledgement is stale or belongs to another activation")
	}
	activeHost := state.Active.ID + "." + state.Active.Namespace + ".svc.cluster.local"
	matched := false
	for _, route := range ack.Routes {
		if route.AppID == state.AppID && route.TenantID == state.TenantID && route.PublicPort == binding.PublicPort && route.TargetHost == activeHost && route.TargetPort == 2222 {
			matched = true
		}
	}
	if !matched {
		return false, errors.New("SSH edge still serves a different target")
	}
	for _, connection := range ack.Connections {
		if connection.AppID == state.AppID && (connection.TargetHost != activeHost || connection.TargetPort != 2222) && connection.Count > 0 {
			return false, nil
		}
	}
	return true, nil
}
