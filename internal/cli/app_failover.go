package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	failoverpkg "fugue/internal/failover"
	"fugue/internal/model"

	"github.com/spf13/cobra"
)

type appFailoverResult struct {
	App        model.App                 `json:"app"`
	Assessment failoverpkg.AppAssessment `json:"assessment"`
}

func (c *CLI) newAppFailoverCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "failover [app]",
		Aliases: []string{"ha", "dr"},
		Short:   "Audit failover readiness for apps",
		Args:    cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}

			runtimes, err := client.ListRuntimes()
			if err != nil {
				c.progressf("warning=runtime inventory unavailable: %v", err)
			}
			runtimeByID := mapRuntimesByID(runtimes)

			if len(args) == 1 {
				app, err := c.resolveNamedApp(client, args[0])
				if err != nil {
					return err
				}
				app, err = client.GetApp(app.ID)
				if err != nil {
					return err
				}
				result := buildAppFailoverResult(app, runtimeByID)
				if c.wantsJSON() {
					return writeJSON(c.stdout, result)
				}
				return writeAppFailoverStatus(c.stdout, result)
			}

			tenantID, projectID, err := c.resolveFilterSelections(client)
			if err != nil {
				return err
			}
			apps, err := client.ListApps()
			if err != nil {
				return err
			}
			results := buildAppFailoverResults(filterApps(apps, tenantID, projectID), runtimeByID)
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"assessments": results})
			}
			return writeAppFailoverTable(c.stdout, results)
		},
	}
}

func buildAppFailoverResults(apps []model.App, runtimeByID map[string]*model.Runtime) []appFailoverResult {
	results := make([]appFailoverResult, 0, len(apps))
	for _, app := range apps {
		results = append(results, buildAppFailoverResult(app, runtimeByID))
	}
	sort.Slice(results, func(i, j int) bool {
		left := results[i]
		right := results[j]
		leftSeverity := failoverSeverity(left.Assessment.Classification)
		rightSeverity := failoverSeverity(right.Assessment.Classification)
		if leftSeverity != rightSeverity {
			return leftSeverity < rightSeverity
		}
		return strings.Compare(left.App.Name, right.App.Name) < 0
	})
	return results
}

func buildAppFailoverResult(app model.App, runtimeByID map[string]*model.Runtime) appFailoverResult {
	runtime := runtimeByID[appRuntimeID(app)]
	return appFailoverResult{
		App:        app,
		Assessment: failoverpkg.AssessApp(app, runtime),
	}
}

func mapRuntimesByID(runtimes []model.Runtime) map[string]*model.Runtime {
	out := make(map[string]*model.Runtime, len(runtimes))
	for index := range runtimes {
		runtime := &runtimes[index]
		out[runtime.ID] = runtime
	}
	return out
}

func appRuntimeID(app model.App) string {
	runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID)
	if runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

func failoverSeverity(classification string) int {
	switch strings.TrimSpace(strings.ToLower(classification)) {
	case failoverpkg.AppClassificationBlocked:
		return 0
	case failoverpkg.AppClassificationCaution:
		return 1
	default:
		return 2
	}
}

func writeAppFailoverTable(w io.Writer, results []appFailoverResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tCLASS\tREPLICAS\tRUNTIME\tNOTES"); err != nil {
		return err
	}
	for _, result := range results {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d/%d\t%s\t%s\n",
			result.App.Name,
			result.Assessment.Classification,
			result.App.Status.CurrentReplicas,
			result.App.Spec.Replicas,
			formatFailoverRuntime(result.Assessment),
			result.Assessment.Summary,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeAppFailoverStatus(w io.Writer, result appFailoverResult) error {
	pairs := []kvPair{
		{Key: "app_id", Value: result.App.ID},
		{Key: "name", Value: result.App.Name},
		{Key: "classification", Value: result.Assessment.Classification},
		{Key: "summary", Value: result.Assessment.Summary},
		{Key: "desired_replicas", Value: fmt.Sprintf("%d", result.App.Spec.Replicas)},
		{Key: "current_replicas", Value: fmt.Sprintf("%d", result.App.Status.CurrentReplicas)},
		{Key: "runtime_id", Value: result.Assessment.RuntimeID},
		{Key: "runtime_type", Value: result.Assessment.RuntimeType},
		{Key: "runtime_status", Value: result.Assessment.RuntimeStatus},
		{Key: "blockers", Value: strings.Join(result.Assessment.Blockers, "; ")},
		{Key: "warnings", Value: strings.Join(result.Assessment.Warnings, "; ")},
	}
	return writeKeyValues(w, pairs...)
}

func formatFailoverRuntime(assessment failoverpkg.AppAssessment) string {
	if strings.TrimSpace(assessment.RuntimeID) == "" {
		return ""
	}
	if strings.TrimSpace(assessment.RuntimeType) == "" {
		return assessment.RuntimeID
	}
	return assessment.RuntimeType + ":" + assessment.RuntimeID
}
