package sshcontrol

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/store"
)

type Runtime interface {
	Resolve(context.Context, model.App, model.ManagedSSHIntent, managedssh.Target) (managedssh.Target, managedssh.RuntimeRelease, error)
	PublishAuthorization(context.Context, managedssh.ServingSlot, map[string]ed25519.PublicKey) error
	Apply(context.Context, managedssh.ServingSlot, []byte, map[string]ed25519.PublicKey) error
	Observe(context.Context, managedssh.ServingSlot) (managedssh.CandidateEvidence, error)
}

type Reconciler struct {
	Store          *store.Store
	Runtime        Runtime
	Namespace      string
	SigningKeyFile string
	Now            func() time.Time
}

func (reconciler *Reconciler) Reconcile(ctx context.Context, tenantID, appID string) error {
	if reconciler.Store == nil || reconciler.Runtime == nil || reconciler.Namespace == "" {
		return managedssh.ErrConfiguration
	}
	_, err := reconciler.Store.WithAdvisoryLock(ctx, "fugue.managed-ssh/"+appID, func() error { return reconciler.reconcile(ctx, tenantID, appID) })
	return err
}

func (reconciler *Reconciler) currentTime() time.Time {
	if reconciler.Now != nil {
		return reconciler.Now().UTC()
	}
	return time.Now().UTC()
}

func (reconciler *Reconciler) reconcile(ctx context.Context, tenantID, appID string) error {
	now := reconciler.currentTime()
	intent, err := reconciler.Store.GetManagedSSHIntent(tenantID, appID)
	if err != nil {
		return err
	}
	identity, err := reconciler.Store.GetAppSSHIdentity(tenantID, appID)
	if err != nil {
		return err
	}
	state, err := reconciler.Store.GetManagedSSHControl(tenantID, appID)
	if errors.Is(err, store.ErrNotFound) {
		if !intent.Spec.Enabled {
			return nil
		}
		state = managedssh.ControlState{Schema: "fugue.managed-ssh-control/v1", TenantID: tenantID, AppID: appID, IdentityID: identity.ID}
	} else if err != nil {
		return err
	}
	if state.IdentityID != identity.ID {
		return managedssh.ErrIdentityChange
	}
	signer, err := managedssh.LoadConfigurationSigner(reconciler.SigningKeyFile)
	if err != nil {
		return err
	}
	defer clear(signer.PrivateKey)
	privateKey, err := reconciler.Store.AppSSHIdentityPrivateKey(tenantID, appID, identity.ID)
	if err != nil {
		return err
	}
	defer clear(privateKey)
	var inventory []model.SSHKey
	if intent.Spec.Enabled {
		inventory, err = reconciler.Store.ListSSHKeys(tenantID, false)
		if err != nil {
			return err
		}
	}
	expected := state.Revision
	changed := state.IntentGeneration != intent.Generation
	state.IntentGeneration = intent.Generation
	refresh := func(slot *managedssh.ServingSlot) error {
		if slot == nil {
			return nil
		}
		if _, err := managedssh.VerifyArtifact(slot.Artifact, signer.PublicKeys); err != nil {
			return err
		}
		desiredIntent := intent
		target := slot.Artifact.Configuration.Target
		if target.Container != intent.Spec.Container || target.User != intent.Spec.User || target.WorkingDirectory != intent.Spec.WorkingDirectory {
			desiredIntent.Spec.Enabled = false
			desiredIntent.Spec.Container, desiredIntent.Spec.User, desiredIntent.Spec.WorkingDirectory = target.Container, target.User, target.WorkingDirectory
		}
		configuration, err := managedssh.CompileConfiguration(desiredIntent, identity, target, inventory, state.Generation+1, now)
		if err != nil {
			return err
		}
		previous := slot.Artifact.Configuration
		configuration.Draining = slot.RetiringAt != nil
		comparison := configuration
		comparison.Generation, comparison.ValidUntil = previous.Generation, previous.ValidUntil
		comparison.Keys = append([]managedssh.KeyGrant{}, configuration.Keys...)
		for index := range comparison.Keys {
			comparison.Keys[index].ExpiresAt = previous.ValidUntil
		}
		if reflect.DeepEqual(comparison, previous) && previous.ValidUntil.Sub(now) > managedssh.GrantLifetime-time.Minute {
			return nil
		}
		artifact, err := managedssh.SignArtifact(configuration, signer.KeyID, signer.PrivateKey)
		if err != nil {
			return err
		}
		slot.Artifact, slot.Evidence = artifact, nil
		state.Generation++
		changed = true
		return nil
	}
	for _, slot := range []*managedssh.ServingSlot{state.Active, state.Candidate} {
		if err := refresh(slot); err != nil {
			return err
		}
	}
	for index := range state.Draining {
		if err := refresh(&state.Draining[index]); err != nil {
			return err
		}
	}
	var reconcileErrors []error
	if changed && len(state.Slots()) > 0 {
		state.Revision++
		if err := reconciler.Store.SaveManagedSSHControl(expected, state); err != nil {
			return err
		}
		expected, changed = state.Revision, false
	}
	for _, slot := range state.Slots() {
		if err := reconciler.Runtime.PublishAuthorization(ctx, slot, signer.PublicKeys); err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("publish managed SSH authorization %s: %w", slot.ID, err))
		}
	}
	if intent.Spec.Enabled {
		app, err := reconciler.Store.GetApp(appID)
		if err != nil {
			return err
		}
		previous := managedssh.Target{}
		if state.Active != nil {
			previous = state.Active.Artifact.Configuration.Target
		}
		target, release, resolveError := reconciler.Runtime.Resolve(ctx, app, intent, previous)
		if resolveError != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("resolve managed SSH target: %w", resolveError))
		} else if err := managedssh.ValidateRuntimeRelease(release); err != nil {
			reconcileErrors = append(reconcileErrors, err)
		} else {
			wanted := managedssh.SlotID(identity.ID, target, release)
			if state.Candidate != nil && state.Candidate.ID != wanted && len(state.Draining) < 8 {
				obsolete := *state.Candidate
				obsolete.DrainingSince = &now
				state.Draining = append(state.Draining, obsolete)
				state.Candidate = nil
				changed = true
			}
			if state.Candidate == nil && (state.Active == nil || state.Active.ID != wanted) {
				for index, retained := range state.Draining {
					if retained.RetiringAt != nil {
						continue
					}
					if retained.ID != wanted {
						continue
					}
					retained.DrainingSince = nil
					state.Candidate = &retained
					state.Draining = append(state.Draining[:index:index], state.Draining[index+1:]...)
					changed = true
					break
				}
			}
			if (state.Active == nil || state.Active.ID != wanted) && (state.Candidate == nil || state.Candidate.ID != wanted) {
				if state.Candidate != nil || len(state.Draining) >= 8 {
					reconcileErrors = append(reconcileErrors, fmt.Errorf("previous candidate or draining slots require resolution before creating another managed SSH slot"))
				} else {
					configuration, err := managedssh.CompileConfiguration(intent, identity, target, inventory, state.Generation+1, now)
					if err != nil {
						reconcileErrors = append(reconcileErrors, err)
					} else {
						artifact, err := managedssh.SignArtifact(configuration, signer.KeyID, signer.PrivateKey)
						if err != nil {
							return err
						}
						state.Candidate = &managedssh.ServingSlot{ID: wanted, Namespace: reconciler.Namespace, Release: release, Artifact: artifact, CreatedAt: now}
						for index, retired := range state.Retired {
							if retired.ID == wanted {
								state.Retired = append(state.Retired[:index:index], state.Retired[index+1:]...)
								break
							}
						}
						state.Generation++
						changed = true
					}
				}
			}
		}
	}
	if len(state.Slots()) == 0 {
		return errors.Join(reconcileErrors...)
	}
	if changed {
		state.Revision++
		if err := reconciler.Store.SaveManagedSSHControl(expected, state); err != nil {
			return err
		}
	}
	for _, slot := range state.Slots() {
		if err := reconciler.Runtime.Apply(ctx, slot, privateKey, signer.PublicKeys); err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("apply managed SSH slot %s: %w", slot.ID, err))
			continue
		}
		checkTime := reconciler.currentTime()
		if slot.Verified(checkTime) && slot.Evidence.ExpiresAt.After(checkTime.Add(20*time.Second)) {
			continue
		}
		if slot.RetiringAt != nil {
			continue
		}
		evidence, err := reconciler.Runtime.Observe(ctx, slot)
		observed := slot
		observed.Evidence = &evidence
		verifiedAt := reconciler.currentTime()
		if err != nil || !observed.Verified(verifiedAt) {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("verify slot %s: reason=%s observed_after_check_ms=%d: %w", slot.ID, observed.VerificationFailure(verifiedAt), evidence.ObservedAt.Sub(verifiedAt).Milliseconds(), errors.Join(managedssh.ErrCandidateUnverified, err)))
			if slot.Evidence == nil {
				continue
			}
			observed.Evidence = nil
		}
		*state.Slot(slot.ID) = observed
		state.Revision++
		if err := reconciler.Store.SaveManagedSSHControl(state.Revision-1, state); err != nil {
			return err
		}
	}
	if intent.Spec.Enabled && state.Active != nil && state.Candidate != nil && state.Candidate.Verified(reconciler.currentTime()) && !state.RollbackHold {
		binding, err := reconciler.Store.GetManagedSSHBinding(tenantID, appID)
		if err == nil {
			var activated managedssh.ControlState
			activated, err = reconciler.Store.ActivateManagedSSH(tenantID, appID, state.Revision, intent.Generation, state.Candidate.Artifact.Digest, identity.Fingerprint, binding.ID, binding.LegacyRouteDigest, "controller:managed-ssh")
			if err == nil {
				state = activated
			}
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	if publisher, supported := reconciler.Runtime.(BindingRuntime); supported && intent.Spec.Enabled && state.Active != nil {
		binding, err := reconciler.Store.GetManagedSSHBinding(tenantID, appID)
		if err == nil {
			err = publisher.ReconcilePublicBinding(ctx, binding, state, reconciler.Store.GetEdgeSSHAcknowledgement)
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			reconcileErrors = append(reconcileErrors, err)
		}
	}
	if err := reconciler.retireSlots(ctx, state); err != nil {
		reconcileErrors = append(reconcileErrors, err)
	}
	return errors.Join(reconcileErrors...)
}
