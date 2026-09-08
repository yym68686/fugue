package cli

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"github.com/spf13/cobra"
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

func (c *CLI) loadArtifactState(client *Client, id, channel string) (artifactStateView, error) {
	result := artifactStateView{ObservedAt: time.Now().UTC(), Authority: "control_plane", Consumers: []model.PlatformConsumerInstance{}, Convergence: []model.PlatformConsumerConvergenceStatus{}, MissingEvidence: []string{}}
	artifact, err := client.GetPlatformArtifact(id)
	if err != nil {
		return result, err
	}
	result.Artifact = artifact
	current, err := client.GetPlatformStateArtifact(artifact.ArtifactKind, artifact.ScopeKey, channel)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "current_release")
	} else {
		result.Current = current
	}
	consumers, err := client.ListPlatformArtifactConsumers(artifact.ID)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "consumers")
	} else {
		result.Consumers = consumers
	}
	filter := model.PlatformExpectedConsumerSetFilter{ArtifactKind: artifact.ArtifactKind, ScopeKey: artifact.ScopeKey, Limit: 100}
	if current.Release != nil {
		filter.ArtifactReleaseID = current.Release.ID
	}
	sets, err := client.ListPlatformExpectedConsumerSets(filter)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "expected_consumer_sets")
	} else {
		result.ConsumersConverged = true
		for _, set := range sets {
			if set.ExpectedGeneration != artifact.Generation {
				continue
			}
			assessment := platformcontrol.EvaluateConsumerConvergence(set, consumers, result.ObservedAt)
			result.Convergence = append(result.Convergence, assessment)
			if !assessment.Pass {
				result.ConsumersConverged = false
			}
		}
		if len(result.Convergence) == 0 {
			result.ConsumersConverged = false
			result.MissingEvidence = append(result.MissingEvidence, "matching_expected_consumer_set")
		}
	}
	if current.Artifact != nil && current.Release != nil && current.LKG != nil {
		lkg := current.LKG
		result.VerifiedLKG = current.Artifact.ID == artifact.ID && current.Release.ArtifactID == artifact.ID && current.Release.Status == model.PlatformArtifactReleaseStatusActive && current.Release.VerificationState == model.PlatformArtifactVerificationStateVerified && lkg.ArtifactID == artifact.ID && lkg.ContentHash == artifact.ContentHash && lkg.Generation == artifact.Generation && lkg.ExpiresAt.After(result.ObservedAt) && lkg.GenerationSequence > 0 && lkg.VerifiedByReleaseID == current.Release.ID && lkg.VerificationEvidenceHash != "" && lkg.SnapshotProvenance.Signature != "" && lkg.ArtifactProvenance.Signature != ""
	}
	return result, nil
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
		view, err := c.loadArtifactState(client, current.Artifact.ID, channel)
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
