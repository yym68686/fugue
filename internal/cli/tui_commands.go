package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"fugue/internal/cli/terminal"
	"fugue/internal/tui"

	"github.com/spf13/cobra"
)

type tuiFlags struct {
	Interval                   time.Duration
	Window, Theme, Graph, Mode string
	Plain, Once, Mouse         bool
	Filter, Search, Sort       string
}

func defaultTUIFlags() tuiFlags {
	return tuiFlags{Interval: 3 * time.Second, Window: "15m", Mouse: true}
}
func bindTUIFlags(cmd *cobra.Command, opts *tuiFlags) {
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Status refresh interval (1s to 1m)")
	bindTUIAppearanceFlags(cmd, opts)
	cmd.Flags().BoolVar(&opts.Plain, "plain", false, "Print a plain snapshot without terminal controls")
	cmd.Flags().BoolVar(&opts.Once, "once", false, "Render one snapshot and exit")
}
func bindTUIAppearanceFlags(cmd *cobra.Command, opts *tuiFlags) {
	cmd.Flags().StringVar(&opts.Window, "window", opts.Window, "Metric time window: 5m, 15m or 1h")
	cmd.Flags().StringVar(&opts.Theme, "theme", "", "Terminal theme: carbon, light or terminal")
	cmd.Flags().StringVar(&opts.Graph, "graph", "", "Graph glyphs: braille, block, line or ascii")
	cmd.Flags().StringVar(&opts.Mode, "screen-mode", "", "Screen mode: fullscreen or compact")
	cmd.Flags().BoolVar(&opts.Mouse, "mouse", true, "Enable mouse clicks, wheel and log selection")
}
func (c *CLI) newAppTopCommand() *cobra.Command     { return c.newTUITopCommand("app") }
func (c *CLI) newProjectTopCommand() *cobra.Command { return c.newTUITopCommand("project") }
func (c *CLI) newTUITopCommand(kind string) *cobra.Command {
	opts := defaultTUIFlags()
	cmd := &cobra.Command{Use: "top <" + kind + ">", Short: "Open an interactive " + kind + " dashboard with live metrics", Args: cobra.ExactArgs(1), Example: "fugue " + kind + " top my-" + kind, RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := applyTUIFlags(cmd, tui.DefaultPreferences(), opts); err != nil {
			return err
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		target := tui.Target{Kind: kind, Name: args[0], TenantID: c.effectiveTenantID(), ProjectID: c.effectiveProjectID()}
		provider := &tuiProvider{cli: c, client: client}
		if c.wantsJSON() || opts.Plain || opts.Once || !c.shouldUseInteractiveMonitor(false) {
			ctx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
			defer cancel()
			snapshot, err := provider.Load(ctx, tui.Request{Target: target, Section: ""})
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return c.writeJSON(snapshot)
			}
			return renderTUIPlain(c, snapshot)
		}
		return c.runTUI(cmd, provider, target, opts)
	}}
	bindTUIFlags(cmd, &opts)
	return cmd
}
func (c *CLI) runTUI(cmd *cobra.Command, provider *tuiProvider, target tui.Target, flags tuiFlags) error {
	path := filepath.Join(filepath.Dir(authConfigPath()), "tui.toml")
	if err := provider.loadReceipts(filepath.Dir(path)); err != nil {
		return err
	}
	prefs, err := tui.LoadPreferences(path)
	if err != nil {
		return err
	}
	prefs, err = applyTUIFlags(cmd, prefs, flags)
	if err != nil {
		return err
	}
	interval, _ := time.ParseDuration(prefs.Interval)
	window, _ := time.ParseDuration(prefs.Window)
	mode, _ := terminal.ParseMode(c.root.Color)
	color := terminal.DetectColorLevel(mode, true, os.LookupEnv) != terminal.ColorNone
	output := c.stdout
	// Bubble Tea needs the terminal descriptor to discover size and receive
	// resize events. The JSON payload writer intentionally hides that descriptor.
	if file, ok := terminalFile(output); ok {
		output = file
	}
	return tui.Run(cmd.Context(), provider, tui.Request{Target: target, Window: window}, tui.Options{InitialFilter: flags.Filter, InitialSearch: flags.Search, InitialSort: flags.Sort, Interval: interval, Window: window, Mouse: prefs.Mouse, Color: color, AltScreen: prefs.Mode == "fullscreen", Input: cmd.InOrStdin(), Output: output, Preferences: prefs, SavePreferences: func(p tui.Preferences) error { return tui.SavePreferences(path, p) }})
}
func applyTUIFlags(cmd *cobra.Command, prefs tui.Preferences, flags tuiFlags) (tui.Preferences, error) {
	if cmd.Flags().Changed("interval") {
		prefs.Interval = flags.Interval.String()
	}
	if cmd.Flags().Changed("window") {
		prefs.Window = flags.Window
	}
	if flags.Theme != "" {
		prefs.Theme = flags.Theme
	}
	if flags.Graph != "" {
		prefs.Graph = flags.Graph
	}
	if flags.Mode != "" {
		prefs.Mode = flags.Mode
	}
	if cmd.Flags().Changed("mouse") {
		prefs.Mouse = flags.Mouse
	}
	return prefs, prefs.Validate()
}
func renderTUIPlain(c *CLI, s tui.Snapshot) error {
	if _, err := fmt.Fprintf(c.stdout, "%s  %s\n", tui.Plain(s.Title), tui.Plain(s.Status)); err != nil {
		return err
	}
	for _, field := range s.Fields {
		if _, err := fmt.Fprintf(c.stdout, "%s: %s\n", tui.Plain(field.Label), tui.Plain(field.Value)); err != nil {
			return err
		}
	}
	for _, table := range s.Tables {
		if _, err := fmt.Fprintf(c.stdout, "\n%s\n", tui.Plain(table.Title)); err != nil {
			return err
		}
		for _, row := range table.Rows {
			for i, value := range row.Cells {
				if i > 0 {
					fmt.Fprint(c.stdout, "\t")
				}
				if _, err := fmt.Fprint(c.stdout, tui.Plain(value)); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(c.stdout); err != nil {
				return err
			}
		}
	}
	return nil
}
