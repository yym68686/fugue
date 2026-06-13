package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	cliterminal "fugue/internal/cli/terminal"
	"fugue/internal/cli/ui"
	"fugue/internal/cli/viewmodel"

	xterm "golang.org/x/term"
)

func (c *CLI) shouldUseRichText() bool {
	if c.wantsJSON() {
		return false
	}
	if !richTextExperimentEnabled() {
		return false
	}
	mode, err := cliterminal.ParseMode(c.root.Interactive)
	if err != nil || mode == cliterminal.ModeNever {
		return false
	}
	if mode == cliterminal.ModeAlways {
		return true
	}
	file, ok := c.stdout.(*os.File)
	return ok && xterm.IsTerminal(int(file.Fd()))
}

func (c *CLI) richRenderer() ui.Renderer {
	mode, err := cliterminal.ParseMode(c.root.Color)
	if err != nil {
		mode = cliterminal.ModeAuto
	}
	width := envIntDefault("COLUMNS", ui.DefaultWidth)
	level := cliterminal.DetectColorLevel(mode, true, os.LookupEnv)
	return ui.NewRenderer(width, cliterminal.Palette{Level: level})
}

func richTextExperimentEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("FUGUE_CLI_RICH_TEXT")))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func envIntDefault(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func (c *CLI) renderRichAppHealth(view viewmodel.AppHealthView) error {
	renderer := c.richRenderer()
	route := []ui.RouteSegment{
		{Label: "source", Tone: viewmodel.ToneNeutral},
		{Label: "build", Tone: viewmodel.ToneNeutral},
		{Label: firstNonEmptyTrimmed(view.RuntimeID, "runtime"), Tone: viewmodel.ToneForGenericStatus(view.Phase)},
	}
	if view.Route.PublicURL != "" {
		route = append(route, ui.RouteSegment{Label: view.Route.PublicURL, Tone: view.Route.Tone})
	}
	body := strings.Join([]string{
		"status " + renderer.StatusChip(view.Phase, view.Tone) + " " + renderer.MetricBar("replicas", view.CurrentReplicas, maxInt(view.DesiredReplicas, view.CurrentReplicas), 10),
		"route  " + renderer.RouteChain(route),
		"",
		renderer.OperationTimeline(view.Operations),
		renderer.CopyBlock("next", fmt.Sprintf("fugue app logs runtime %s --follow", firstNonEmptyTrimmed(view.Name, view.ID))),
	}, "\n")
	_, err := fmt.Fprint(c.stdout, renderer.Panel("App "+firstNonEmptyTrimmed(view.Name, view.ID), body))
	return err
}

func (c *CLI) renderRichDiagnosis(view viewmodel.DiagnosisEvidenceView) error {
	renderer := c.richRenderer()
	_, err := fmt.Fprint(c.stdout, renderer.Panel("Diagnosis "+firstNonEmptyTrimmed(view.Category, string(view.State.Kind)), renderer.ErrorBlock(view)))
	return err
}

func (c *CLI) renderRichOperationExplain(operation viewmodel.OperationTimelineView, diagnosis viewmodel.DiagnosisEvidenceView) error {
	renderer := c.richRenderer()
	body := strings.Join([]string{
		"operation",
		renderer.OperationTimeline(operation),
		"diagnosis",
		renderer.ErrorBlock(diagnosis),
	}, "\n")
	_, err := fmt.Fprint(c.stdout, renderer.Panel("Operation "+firstNonEmptyTrimmed(operation.LatestID, "explain"), body))
	return err
}

func (c *CLI) renderRichProjectWorkbench(view viewmodel.ProjectWorkbenchView, diagnoses []viewmodel.DiagnosisEvidenceView) error {
	renderer := c.richRenderer()
	rows := make([][]string, 0, len(view.Apps))
	for _, app := range view.Apps {
		rows = append(rows, []string{
			firstNonEmptyTrimmed(app.Name, app.ID),
			app.Phase,
			fmt.Sprintf("%d/%d", app.CurrentReplicas, maxInt(app.DesiredReplicas, app.CurrentReplicas)),
			firstNonEmptyTrimmed(app.RuntimeID, "-"),
			firstNonEmptyTrimmed(app.URL, "-"),
		})
	}
	bodyParts := []string{
		fmt.Sprintf("apps=%d services=%d operations=%d", view.AppCount, view.ServiceCount, view.OperationCount),
		renderer.TableWithTitle("apps", []string{"APP", "PHASE", "REPLICAS", "RUNTIME", "URL"}, rows),
	}
	if len(view.Operations.Steps) > 0 {
		bodyParts = append(bodyParts, "operations", renderer.OperationTimeline(view.Operations))
	}
	if len(diagnoses) > 0 {
		bodyParts = append(bodyParts, "diagnosis", renderer.ErrorBlock(diagnoses[0]))
	}
	_, err := fmt.Fprint(c.stdout, renderer.Panel("Project "+firstNonEmptyTrimmed(view.Name, view.ID), strings.Join(bodyParts, "\n")))
	return err
}
