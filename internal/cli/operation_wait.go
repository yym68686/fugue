package cli

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

// Waiting is a business assertion, independent of terminal rendering.
func (c *CLI) newOperationWaitCommand() *cobra.Command {
	var timeout, interval time.Duration
	var showSecrets bool
	var appRef string
	cmd := &cobra.Command{
		Use: "wait [operation]", Short: "Wait for an operation to succeed, with a bounded deadline", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if timeout <= 0 || interval <= 0 {
				return withExitCode(fmt.Errorf("--timeout and --interval must be positive"), ExitCodeUserInput)
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			client.context = ctx
			id := ""
			if len(args) == 1 {
				id = args[0]
			}
			if id != "" && appRef != "" {
				return withExitCode(fmt.Errorf("operation and --app cannot be used together"), ExitCodeUserInput)
			}
			if id == "" && appRef != "" {
				appID, err := c.resolveOpsAppID(client, appRef)
				if err != nil {
					return err
				}
				operations, err := client.ListOperations(appID)
				if err != nil {
					return err
				}
				latest, err := latestOperation(operations)
				if err != nil {
					return err
				}
				id = latest.ID
			}
			if id == "" {
				return withExitCode(fmt.Errorf("operation id or --app is required"), ExitCodeUserInput)
			}
			op, waitErr := waitOperation(ctx, client, id, interval)
			if !showSecrets {
				op = redactOperationForOutput(op)
			}
			outcome := "succeeded"
			if waitErr != nil {
				outcome = "unknown"
				if op.Status == model.OperationStatusFailed || op.Status == "cancelled" || op.Status == "canceled" || op.Status == "superseded" {
					outcome = "failed"
				}
			}
			next := []string{"fugue operation show " + shellSingleQuote(id), "fugue operation wait " + shellSingleQuote(id)}
			if c.wantsJSON() {
				payload := map[string]any{"schema_version": 1, "operation": op, "outcome": outcome, "next_commands": next}
				if waitErr != nil {
					payload["error"] = describeCommandError(waitErr)
				}
				if err := c.writeJSON(payload); err != nil {
					return err
				}
			} else {
				if err := writeKeyValues(c.stdout, kvPair{Key: "operation_id", Value: id}, kvPair{Key: "status", Value: op.Status}, kvPair{Key: "outcome", Value: outcome}); err != nil {
					return err
				}
				if waitErr != nil {
					c.progressf("resume=%s", next[1])
				}
			}
			return waitErr
		},
	}
	cmd.Flags().StringVar(&appRef, "app", "", "Wait for the latest operation of an app")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Minute, "Maximum wait; timing out does not cancel the server operation")
	cmd.Flags().DurationVar(&interval, "interval", 2*time.Second, "Polling interval")
	cmd.Flags().BoolVar(&showSecrets, "show-secrets", false, "Show sensitive operation fields")
	return cmd
}

func waitOperation(ctx context.Context, client *Client, id string, interval time.Duration) (model.Operation, error) {
	scoped := *client
	scoped.context = ctx
	last := model.Operation{ID: id}
	for {
		op, err := scoped.GetOperation(id)
		if err == nil {
			last = op
			switch strings.ToLower(strings.TrimSpace(op.Status)) {
			case model.OperationStatusCompleted:
				return last, nil
			case model.OperationStatusFailed, "canceled", "cancelled", "superseded":
				return last, withExitCode(fmt.Errorf("operation %s %s: %s", id, op.Status, redactDiagnosticString(op.ErrorMessage)), ExitCodeSystemFault)
			}
		} else {
			if ctx.Err() != nil {
				return last, withExitCode(fmt.Errorf("waiting for operation %s: %w (server operation was not cancelled)", id, ctx.Err()), ExitCodeIndeterminate)
			}
			if code := ExitCodeForError(err); code == ExitCodePermissionDenied || code == ExitCodeNotFound || code == ExitCodeUserInput {
				return last, err
			}
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, withExitCode(fmt.Errorf("waiting for operation %s: %w (server operation was not cancelled)", id, ctx.Err()), ExitCodeIndeterminate)
		case <-timer.C:
		}
	}
}
