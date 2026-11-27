package migration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/pgimage"
)

func Execute(ctx context.Context, ledger Ledger, prepare Prepare) (executionErr error) {
	// Unknown ledger state is unsafe to classify as an ordinary failed job.
	recordedIntent, abortDrain, err := ledger.ReadIntent()
	if err != nil {
		return fmt.Errorf("%w: read durable intent: %v", ErrUnsettled, err)
	}
	intent := recordedIntent != nil
	resuming := intent
	defer func() {
		if executionErr != nil && intent && !errors.Is(executionErr, context.Canceled) && !ledger.IsInactive(executionErr) {
			ledger.Progress("image migration waiting for verification: " + executionErr.Error())
			executionErr = fmt.Errorf("%w: %v", ErrUnsettled, executionErr)
		}
	}()
	target, platform, err := prepare(ctx)
	if err != nil {
		return err
	}
	ns, name, image, budget := target.Namespace, target.Cluster, target.Image, target.Budget
	var witness pgimage.Witness
	// Once armed, a timeout is measured from the first durable intent, not
	// from each retry or a new controller leader.
	armDeadline := time.Now().Add(30 * time.Second)
	if intent {
		b, _ := json.Marshal(recordedIntent.Payload)
		if err = json.Unmarshal(b, &witness); err != nil {
			return err
		}
		if witness.Namespace != ns || witness.Cluster != name || witness.TargetImage != image || witness.ClusterUID == "" || witness.SystemID == "" || witness.PodUID == "" || witness.SpecHash == "" || len(witness.Volumes) == 0 ||
			(budget == 0 && witness.Drain != nil) || (budget != 0 && (witness.Drain == nil || witness.Drain.BudgetSeconds != budget || witness.Drain.OriginalSeconds <= budget || witness.Drain.Marker != pgimage.Hash([]string{target.OperationID, witness.ClusterUID, witness.PodUID}))) || witness.DrainRestored {
			return fmt.Errorf("durable migration witness is incomplete or mismatched")
		}
		armDeadline = recordedIntent.CollectedAt.Add(30 * time.Second)
	} else {
		witness, err = platform.Capture(ctx)
		if err != nil {
			return err
		}
		observation, err := platform.Observe(ctx)
		if err != nil {
			return err
		}
		if err = observation.Verify(witness); err != nil {
			return err
		}
		if observation.PodUID != witness.PodUID {
			return fmt.Errorf("primary changed during migration preflight")
		}
		if err = observation.Probe(ctx); err != nil {
			return err
		}
		if budget != 0 {
			if err := platform.ConfigureDrain(&witness, observation.Cluster); err != nil {
				return err
			}
			sample := platform.SampleDrain(ctx, witness)
			platform.RecordDrain(sample)
			if sample.Status != "eligible" || sample.LoadedMarker != "" {
				reason := sample.Reason
				if reason == "" {
					reason = "an unexpected drain marker is loaded"
				}
				return fmt.Errorf("fast drain preflight blocked: %s", reason)
			}
		}
		if err = ledger.WriteWitness(pgimage.BaselineEvidence, witness); err != nil {
			return err
		}
		if err = ledger.CheckOwnership(); err != nil {
			return err
		}
		// From here on even an uncertain write must retain the mutation interlock.
		if err = ledger.WriteWitness(pgimage.IntentEvidence, witness); err != nil {
			return err
		}
		intent = true
		armDeadline = time.Now().Add(30 * time.Second)
	}
	if witness.RestartResourceTarget != nil && (target.Resources == nil || *witness.RestartResourceTarget != *target.Resources) {
		return fmt.Errorf("accepted restart resources differ from the persisted in-place resource target")
	}
	ledger.Progress("verified database image migration: awaiting CNPG replacement and SQL verification")
	deadline := time.Now().Add(15 * time.Minute)
	waitingReason := "awaiting a new healthy primary Pod using the target image"
	reportedReason := ""
	var timing EndpointTiming
	if witness.Drain != nil {
		timing = platform.BeginEndpointTiming(ctx, witness, resuming)
		defer timing.StopAndRecord()
	}
	for {
		if err = ledger.CheckOwnership(); err != nil {
			return err
		}
		if resuming && witness.BootstrapPodPatch == nil && witness.Drain == nil {
			repaired, err := platform.RecoverBootstrap(ctx, witness)
			if err != nil {
				return err
			}
			if repaired {
				continue
			}
		}
		if witness.Drain != nil {
			drain, err := platform.ObserveDrain(ctx, witness)
			if err != nil {
				return err
			}
			if drain.OnSource && (abortDrain || time.Now().After(armDeadline)) {
				if err := platform.AbortDrain(ctx, witness, drain.Cluster, abortDrain, "old instance did not reach the approved cutover"); err != nil {
					return err
				}
				intent = false
				return fmt.Errorf("fast drain aborted before image replacement; original shutdown settings restored")
			}
			if !drain.OnSource && drain.Staged {
				// Restore only once the original writer has exited and CNPG has
				// created the exact replacement. No need to wait for SQL readiness.
				if platform.ReplacementExists(ctx, witness) {
					if err := platform.RestoreDrain(ctx, witness, drain.Cluster); err != nil {
						return err
					}
					continue
				}
			}
		}
		observation, observeErr := platform.Observe(ctx)
		if observeErr != nil {
			waitingReason = observeErr.Error()
		}
		if observeErr == nil {
			waitingReason = "awaiting a new healthy primary Pod using the target image"
			if err = observation.Verify(witness); err != nil {
				return err
			}
			currentImage := observation.CurrentImage
			if err := observation.VerifyVolumes(ctx, witness); err != nil {
				return err
			}
			if currentImage == witness.SourceImage {
				if observation.PodUID != witness.PodUID {
					return fmt.Errorf("source primary was replaced outside the migration")
				}
				if witness.Drain != nil {
					staged, err := platform.DrainStaged(witness, observation.Cluster)
					if err != nil {
						return err
					}
					sample := platform.SampleDrain(ctx, witness)
					if sample.Status != "eligible" {
						platform.RecordDrain(sample)
						if err := platform.AbortDrain(ctx, witness, observation.Cluster, abortDrain, sample.Reason); err != nil {
							return err
						}
						intent = false
						return fmt.Errorf("fast drain aborted: %s", sample.Reason)
					}
					if !staged {
						if sample.LoadedMarker != "" {
							return fmt.Errorf("old instance reports a foreign drain marker")
						}
						if err := ledger.CheckOwnership(); err != nil {
							return err
						}
						if err := platform.StageDrain(ctx, witness, observation.Cluster); err != nil {
							return err
						}
						continue
					}
					if sample.LoadedMarker != witness.Drain.Marker || !sample.MarkerFromFile {
						reason := "awaiting old instance manager acknowledgement of temporary shutdown settings"
						if reportedReason != reason {
							ledger.Progress("database image migration waiting: " + reason)
							reportedReason = reason
						}
						if err := waitForObservation(ctx, 500*time.Millisecond); err != nil {
							return err
						}
						continue
					}
					// UID and resourceVersion pin the old Pod and Cluster
					// observations across the acknowledged reload.
					if !platform.SourceStillPresent(ctx, witness) {
						return fmt.Errorf("primary changed while verifying temporary shutdown settings")
					}
					platform.RecordDrain(sample)
				}
				if err = ledger.CheckOwnership(); err != nil {
					return err
				}
				if timing != nil {
					timing.MarkCutover()
				}
				if err = platform.PatchImage(ctx, witness, observation.Cluster); err != nil {
					return fmt.Errorf("image compare-and-swap outcome requires verification: %w", err)
				}
			} else if observation.PodUID != witness.PodUID && observation.PodImage == image {
				if witness.Drain != nil {
					staged, err := platform.DrainStaged(witness, observation.Cluster)
					if err != nil {
						return err
					}
					if staged {
						if err := platform.RestoreDrain(ctx, witness, observation.Cluster); err != nil {
							return err
						}
						continue
					}
					if sample := platform.SampleDrain(ctx, witness); sample.SampledAt == nil || sample.LoadedMarker != "" {
						return fmt.Errorf("waiting for replacement instance to acknowledge restored shutdown settings")
					}
				}
				if err := observation.VerifyReplacement(image); err != nil {
					return err
				}
				if err = observation.Probe(ctx); err != nil {
					return err
				}
				verified := witness
				verified.VerifiedPodUID = observation.PodUID
				verified.DrainRestored = witness.Drain != nil
				if timing != nil {
					timing.StopAndRecord()
				}
				if err = ledger.WriteWitness(pgimage.VerifiedEvidence, verified); err != nil {
					return err
				}
				if err = ledger.CheckOwnership(); err != nil {
					return err
				}
				return ledger.Complete()
			}
		}
		if waitingReason != reportedReason {
			ledger.Progress("database image migration waiting: " + waitingReason)
			reportedReason = waitingReason
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("CNPG image migration verification timed out: %s; no rollback, deletion or storage mutation performed", waitingReason)
		}
		if err = waitForObservation(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

func waitForObservation(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
