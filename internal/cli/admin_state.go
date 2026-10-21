package cli

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"github.com/spf13/cobra"
	"reflect"
	"time"
)

type artifactStateView struct {
	Artifact           model.PlatformArtifact                    `json:"artifact"`
	Current            platformStateArtifactEnvelope             `json:"current"`
	Consumers          []model.PlatformConsumerInstance          `json:"consumers"`
	Convergence        []model.PlatformConsumerConvergenceStatus `json:"convergence"`
	MissingEvidence    []string                                  `json:"missing_evidence"`
	ObservedAt         time.Time                                 `json:"observed_at"`
	ConsumersConverged bool                                      `json:"consumers_converged"`
	VerifiedLKG        bool                                      `json:"verified_lkg"`
	Authority          string                                    `json:"verification_authority"`
}

const artifactStateReadAttempts = 3

func (c *CLI) loadArtifactState(client *Client, id, channel string) (artifactStateView, error) {
	return c.loadArtifactStateWithMode(client, id, channel, false)
}

func (c *CLI) loadCurrentArtifactState(client *Client, id, channel string) (artifactStateView, error) {
	return c.loadArtifactStateWithMode(client, id, channel, true)
}

func (c *CLI) loadArtifactStateWithMode(client *Client, id, channel string, requireCurrent bool) (artifactStateView, error) {
	artifactID := id
	var last artifactStateView
	for attempt := 0; attempt < artifactStateReadAttempts; attempt++ {
		view, nextID, stable, err := c.loadArtifactStateOnce(client, artifactID, channel, requireCurrent)
		if !stable || err != nil {
			view.ConsumersConverged, view.VerifiedLKG = false, false
		}
		last = view
		if err != nil {
			return view, err
		}
		if stable {
			return view, nil
		}
		if requireCurrent && nextID != "" {
			artifactID = nextID
		} else if !requireCurrent && view.Artifact.ID != "" {
			// Resolve a caller-supplied generation once; a retry must never
			// replace the explicitly requested artifact with the serving one.
			artifactID = view.Artifact.ID
		}
	}
	last.MissingEvidence = append(last.MissingEvidence, "state_snapshot_changed_during_read")
	return last, withExitCode(fmt.Errorf("platform state changed during read; no single release snapshot was established"), ExitCodeIndeterminate)
}

func (c *CLI) loadArtifactStateOnce(client *Client, id, channel string, requireCurrent bool) (artifactStateView, string, bool, error) {
	result := artifactStateView{ObservedAt: time.Now().UTC(), Authority: "control_plane", Consumers: []model.PlatformConsumerInstance{}, Convergence: []model.PlatformConsumerConvergenceStatus{}, MissingEvidence: []string{}}
	artifact, err := client.GetPlatformArtifact(id)
	if err != nil {
		return result, "", false, err
	}
	result.Artifact = artifact
	current, err := client.GetPlatformStateArtifact(artifact.ArtifactKind, artifact.ScopeKey, channel)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "current_release")
		return result, "", false, err
	}
	result.Current = current
	if requireCurrent && (current.Artifact == nil || current.Release == nil || current.Release.ArtifactID != current.Artifact.ID) {
		result.MissingEvidence = append(result.MissingEvidence, "current_release_binding")
		return result, "", false, nil
	}
	if requireCurrent && current.Artifact.ID != artifact.ID {
		return result, current.Artifact.ID, false, nil
	}
	consumers, err := client.ListPlatformArtifactConsumers(artifact.ID)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "consumers")
	} else {
		result.Consumers = consumers
	}
	filter := model.PlatformExpectedConsumerSetFilter{ArtifactKind: artifact.ArtifactKind, ScopeKey: artifact.ScopeKey, Limit: 200}
	if artifact.ArtifactKind == model.PlatformArtifactKindReleaseSet {
		filter.ReleaseSetID, filter.ArtifactKind = artifact.ID, ""
		if current.Release != nil && current.Release.ArtifactID == artifact.ID {
			filter.ArtifactReleaseID = current.Release.ID
		}
	}
	assessments, err := client.ListPlatformConsumerConvergence(filter)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "authoritative_consumer_convergence")
	} else {
		result.ConsumersConverged = true
		for _, assessment := range assessments {
			if artifact.ArtifactKind != model.PlatformArtifactKindReleaseSet && assessment.ExpectedGeneration != artifact.Generation {
				continue
			}
			result.Convergence = append(result.Convergence, assessment)
			if !assessment.Pass {
				result.ConsumersConverged = false
			}
		}
		if len(result.Convergence) == 0 {
			result.ConsumersConverged = false
			result.MissingEvidence = append(result.MissingEvidence, "matching_expected_consumer_set")
		}
		if artifact.ArtifactKind == model.PlatformArtifactKindReleaseSet {
			kinds, ok := artifact.Content["artifact_kinds"].([]any)
			observed := map[string]bool{}
			for _, assessment := range result.Convergence {
				observed[assessment.ArtifactKind] = true
			}
			if !ok || len(kinds) == 0 {
				result.ConsumersConverged = false
				result.MissingEvidence = append(result.MissingEvidence, "release_set_members")
			}
			for _, raw := range kinds {
				kind, ok := raw.(string)
				if !ok || !observed[kind] {
					result.ConsumersConverged = false
					result.MissingEvidence = append(result.MissingEvidence, "release_set_member_convergence")
				}
			}
		}
	}
	// The producer may publish while the evidence reads are in flight. Re-read
	// the selected channel's pointer and discard results if it changes.
	// This is a bounded consistency check, not a transaction across channels
	// or live consumer observations; authoritative failures remain failures.
	finalCurrent, finalErr := client.GetPlatformStateArtifact(artifact.ArtifactKind, artifact.ScopeKey, channel)
	if finalErr != nil {
		result.MissingEvidence = append(result.MissingEvidence, "final_current_release")
		return result, "", false, nil
	}
	if requireCurrent && (finalCurrent.Artifact == nil || finalCurrent.Release == nil || finalCurrent.Release.ArtifactID != finalCurrent.Artifact.ID) {
		result.MissingEvidence = append(result.MissingEvidence, "final_current_release_binding")
		return result, "", false, nil
	}
	if requireCurrent && finalCurrent.Artifact.ID != artifact.ID {
		return result, finalCurrent.Artifact.ID, false, nil
	}
	if !samePlatformStateSnapshot(current, finalCurrent) {
		nextID := artifact.ID
		if requireCurrent && finalCurrent.Artifact != nil && finalCurrent.Artifact.ID != "" {
			nextID = finalCurrent.Artifact.ID
		}
		return result, nextID, false, nil
	}
	result.Current = finalCurrent
	result.ObservedAt = time.Now().UTC()
	if finalCurrent.Artifact != nil && finalCurrent.Release != nil && finalCurrent.LKG != nil {
		lkg := finalCurrent.LKG
		result.VerifiedLKG = finalCurrent.Artifact.ID == artifact.ID && finalCurrent.Release.ArtifactID == artifact.ID && finalCurrent.Release.Status == model.PlatformArtifactReleaseStatusActive && finalCurrent.Release.VerificationState == model.PlatformArtifactVerificationStateVerified && lkg.ArtifactID == artifact.ID && lkg.ContentHash == artifact.ContentHash && lkg.Generation == artifact.Generation && lkg.ExpiresAt.After(result.ObservedAt) && lkg.GenerationSequence > 0 && lkg.VerifiedByReleaseID == finalCurrent.Release.ID && lkg.VerificationEvidenceHash != "" && lkg.SnapshotProvenance.Signature != "" && lkg.ArtifactProvenance.Signature != ""
	}
	return result, artifact.ID, true, nil
}

func samePlatformStateSnapshot(a, b platformStateArtifactEnvelope) bool {
	if (a.Artifact == nil) != (b.Artifact == nil) || (a.Release == nil) != (b.Release == nil) || (a.LKG == nil) != (b.LKG == nil) {
		return false
	}
	if a.Artifact != nil && (a.Artifact.ID != b.Artifact.ID || a.Artifact.Generation != b.Artifact.Generation || a.Artifact.ContentHash != b.Artifact.ContentHash || a.Artifact.GenerationSequence != b.Artifact.GenerationSequence || a.Artifact.Status != b.Artifact.Status || !reflect.DeepEqual(a.Artifact.Provenance, b.Artifact.Provenance)) {
		return false
	}
	// These are bounded metadata, including revocation, expiry and proof
	// fields. Comparing IDs alone misses changes to the same release/LKG.
	return reflect.DeepEqual(a.Release, b.Release) && reflect.DeepEqual(a.LKG, b.LKG)
}
func (c *CLI) newAdminStateCommand() *cobra.Command {
	var kind, scope, channel string
	cmd := &cobra.Command{Use: "state", Short: "Inspect platform configuration artifacts and runtime evidence independently of code releases"}
	show := &cobra.Command{Use: "show", Aliases: []string{"explain"}, Short: "Show the active artifact, LKG and consumer evidence for an explicit scope", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if kind == "" {
			return fmt.Errorf("--kind is required")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		current, err := client.GetPlatformStateArtifact(kind, scope, channel)
		if err != nil {
			return err
		}
		if current.Artifact == nil {
			return c.renderResourceResult(map[string]any{"kind": kind, "scope": scope, "state": "empty", "verified_lkg": false})
		}
		view, err := c.loadCurrentArtifactState(client, current.Artifact.ID, channel)
		if err != nil {
			return err
		}
		return c.renderResourceResult(view)
	}}
	show.Flags().StringVar(&kind, "kind", "", "Artifact kind")
	show.Flags().StringVar(&scope, "scope", "global", "Scope key")
	show.Flags().StringVar(&channel, "channel", "full", "Release channel")
	show.Example = "fugue admin state show --kind " + model.PlatformArtifactKindEdgeRouteBundle + " --scope global --channel full"
	_ = show.MarkFlagRequired("kind")
	cmd.AddCommand(show)
	return cmd
}
func (c *CLI) newAdminArtifactPlanCommand() *cobra.Command {
	var channel string
	cmd := &cobra.Command{Use: "plan <artifact-id-or-generation>", Short: "Validate and compare an artifact with its current scope without publishing", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		view, err := c.loadArtifactState(client, args[0], channel)
		if err != nil {
			return err
		}
		validation, err := client.ValidatePlatformArtifact(view.Artifact.ID, true)
		if err != nil {
			return err
		}
		result := map[string]any{"schema_version": 1, "dry_run": true, "state": view, "validation": validation, "publish_eligibility": "requires_server_release_checks", "next_command": "fugue admin artifact release " + shellSingleQuote(view.Artifact.ID) + " --channel " + shellSingleQuote(channel)}
		if view.Current.Artifact != nil {
			result["diff"] = platformArtifactSafeDiff(*view.Current.Artifact, view.Artifact)
		}
		if err := c.renderResourceResult(result); err != nil {
			return err
		}
		if !validation.Pass {
			return withExitCode(fmt.Errorf("artifact validation failed"), ExitCodeUserInput)
		}
		return nil
	}}
	cmd.Flags().StringVar(&channel, "channel", "full", "Channel to compare; this command never publishes")
	return cmd
}
func (c *CLI) newAdminArtifactWaitCommand() *cobra.Command {
	var condition, channel string
	var timeout, interval time.Duration
	cmd := &cobra.Command{Use: "wait <artifact-id-or-generation>", Short: "Wait for consumer convergence or server-verified LKG; never fabricate verification evidence", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if condition != "verified" && condition != "consumers" {
			return fmt.Errorf("--for must be verified or consumers")
		}
		if timeout <= 0 || interval <= 0 {
			return fmt.Errorf("--timeout and --interval must be positive")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
		defer cancel()
		scoped := *client
		scoped.context = ctx
		var last artifactStateView
		var runErr error
		for {
			view, err := c.loadArtifactState(&scoped, args[0], channel)
			if err == nil {
				last = view
				if (condition == "verified" && view.VerifiedLKG) || (condition == "consumers" && view.ConsumersConverged) {
					break
				}
			} else if code := ExitCodeForError(err); code == 3 || code == 4 || code == 2 {
				runErr = err
				break
			}
			if err := waitRequestRetry(ctx, interval); err != nil {
				runErr = withExitCode(fmt.Errorf("artifact condition %s not established: %w", condition, err), ExitCodeIndeterminate)
				break
			}
		}
		if err := c.renderResourceResult(last); err != nil {
			return err
		}
		return runErr
	}}
	cmd.Flags().StringVar(&condition, "for", "verified", "Required condition: verified or consumers")
	cmd.Flags().StringVar(&channel, "channel", "full", "Release channel")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Minute, "Maximum local wait")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "Poll interval")
	return cmd
}
