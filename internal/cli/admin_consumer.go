package cli

import (
	"fmt"
	"io"
	"text/tabwriter"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAdminConsumerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consumer",
		Short: "Inspect expected and observed platform consumers",
	}
	cmd.AddCommand(c.newAdminConsumerExpectedCommand())
	return cmd
}

func (c *CLI) newAdminConsumerExpectedCommand() *cobra.Command {
	filter := model.PlatformExpectedConsumerSetFilter{Limit: 50}
	cmd := &cobra.Command{
		Use:   "expected",
		Short: "List fixed expected consumer topology snapshots",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if filter.Limit < 1 || filter.Limit > 200 {
				return fmt.Errorf("--limit must be between 1 and 200")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			sets, err := client.ListPlatformExpectedConsumerSets(filter)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"expected_consumer_sets": sets})
			}
			return writePlatformExpectedConsumerSets(c.stdout, sets)
		},
	}
	cmd.Flags().StringVar(&filter.ReleaseSetID, "release-set", "", "Filter by platform release set ID")
	cmd.Flags().StringVar(&filter.ArtifactReleaseID, "artifact-release", "", "Filter by platform artifact release ID")
	cmd.Flags().StringVar(&filter.ArtifactKind, "kind", "", "Filter by platform artifact kind")
	cmd.Flags().StringVar(&filter.ScopeKey, "scope", "", "Filter by artifact scope key")
	cmd.Flags().IntVar(&filter.Limit, "limit", filter.Limit, "Maximum expected consumer sets")
	return cmd
}

func writePlatformExpectedConsumerSets(w io.Writer, sets []model.PlatformExpectedConsumerSet) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SET ID\tRELEASE SET\tARTIFACT KIND\tSCOPE\tGENERATION\tREVISION\tREQUIRED\tOPTIONAL"); err != nil {
		return err
	}
	for _, set := range sets {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\n",
			set.ID,
			set.ReleaseSetID,
			set.ArtifactKind,
			set.ScopeKey,
			set.ExpectedGeneration,
			set.Revision,
			set.RequiredCardinality,
			set.OptionalCardinality,
		); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, set := range sets {
		if len(set.Consumers) == 0 {
			continue
		}
		if _, err := fmt.Fprintf(w, "\nConsumers for %s\n", set.ID); err != nil {
			return err
		}
		consumerWriter := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(consumerWriter, "REQUIRED\tCOMPONENT\tNODE\tFAILURE DOMAIN\tCOHORT\tPROTOCOL\tSCHEMA"); err != nil {
			return err
		}
		for _, consumer := range set.Consumers {
			if _, err := fmt.Fprintf(consumerWriter, "%t\t%s\t%s\t%s\t%s\t%s\t%s\n",
				consumer.Required,
				consumer.Component,
				consumer.NodeID,
				consumer.FailureDomain,
				consumer.Cohort,
				consumer.ExpectedProtocolVersion,
				consumer.ExpectedSchemaVersion,
			); err != nil {
				return err
			}
		}
		if err := consumerWriter.Flush(); err != nil {
			return err
		}
	}
	return nil
}
