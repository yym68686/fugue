package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"fugue/internal/model"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"
)

func (c *CLI) newAppDatabaseImportCommand() *cobra.Command {
	opts := struct {
		DumpPath string
		Label    string
		Format   string
		Clean    bool
		Wait     bool
	}{Format: model.AppDatabaseImportFormatAuto, Wait: true}

	cmd := &cobra.Command{
		Use:   "import <app>",
		Short: "Import a dump into the app managed Postgres database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			dumpBytes, err := os.ReadFile(opts.DumpPath)
			if err != nil {
				return fmt.Errorf("read dump: %w", err)
			}
			response, err := client.ImportAppDatabase(app.ID, model.AppDatabaseImportRequest{
				Label:  strings.TrimSpace(opts.Label),
				Clean:  opts.Clean,
				Format: normalizeCLIAppDatabaseImportFormat(opts.Format),
			}, filepath.Base(opts.DumpPath), dumpBytes)
			if err != nil {
				return err
			}
			job := response.Job
			if opts.Wait && job != nil {
				job, err = c.waitForAppDatabaseImportJob(client, app.ID, job.ID)
				if err != nil {
					return err
				}
			}
			return c.renderAppDatabaseImportStatus(response.App, job, "import")
		},
	}
	cmd.Flags().StringVar(&opts.DumpPath, "dump", "", "Dump file to import")
	cmd.Flags().StringVar(&opts.Label, "label", "", "Import label")
	cmd.Flags().StringVar(&opts.Format, "format", model.AppDatabaseImportFormatAuto, "Dump format: auto, sql, or custom")
	cmd.Flags().BoolVar(&opts.Clean, "clean", false, "Ask the importer to clean existing objects before restoring")
	cmd.Flags().BoolVar(&opts.Wait, "wait", true, "Wait for the import job to finish")
	_ = cmd.MarkFlagRequired("dump")

	statusCmd := &cobra.Command{
		Use:   "status <app>",
		Short: "Show the latest app database import job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.GetAppDatabaseImport(app.ID)
			if err != nil {
				return err
			}
			return c.renderAppDatabaseImportStatus(response.App, response.Job, "status")
		},
	}

	retryCmd := &cobra.Command{
		Use:   "retry <app>",
		Short: "Retry the latest failed app database import job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.RetryAppDatabaseImport(app.ID, model.AppDatabaseImportRetryRequest{})
			if err != nil {
				return err
			}
			return c.renderAppDatabaseImportStatus(response.App, response.Job, "retry")
		},
	}

	verifyCmd := &cobra.Command{
		Use:   "verify <app>",
		Short: "Verify the app database is reachable after import",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			jobResp, jobErr := client.GetAppDatabaseImport(app.ID)
			if jobErr == nil && jobResp.Job != nil {
				if jobResp.Job.Status != model.OperationStatusCompleted {
					return fmt.Errorf("latest import job is %s", jobResp.Job.Status)
				}
			}
			response, err := client.QueryAppDatabase(app.ID, `select 1 as ok`, 1, 10*time.Second)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				payload := map[string]any{
					"app":    app,
					"result": response,
				}
				return writeJSON(c.stdout, payload)
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "database", Value: response.Database},
				kvPair{Key: "host", Value: response.Host},
				kvPair{Key: "row_count", Value: fmt.Sprintf("%d", response.RowCount)},
			)
		},
	}

	cmd.AddCommand(statusCmd, retryCmd, verifyCmd)
	return cmd
}

func (c *CLI) newAppDatabaseAccessCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access",
		Short: "Manage external access grants for the app managed Postgres database",
	}
	cmd.AddCommand(c.newAppDatabaseAccessShowCommand(), c.newAppDatabaseAccessCreateCommand(), c.newAppDatabaseAccessRevokeCommand(), c.newAppDatabaseAccessTunnelCommand())
	return cmd
}

func (c *CLI) newAppDatabaseAccessShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <app>",
		Short: "Show app database access grants",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.ListAppDatabaseAccessGrants(app.ID)
			if err != nil {
				return err
			}
			return c.renderAppDatabaseAccessResponse(response.App, response.Grants)
		},
	}
	return cmd
}

func (c *CLI) newAppDatabaseAccessCreateCommand() *cobra.Command {
	opts := struct {
		Label          string
		Mode           string
		ExpiresMinutes int
		Listen         string
	}{Mode: model.AppDatabaseAccessModeReadWrite, Listen: "127.0.0.1:15432"}
	cmd := &cobra.Command{
		Use:   "create <app>",
		Short: "Create an app database access grant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			mode := normalizeCLIAppDatabaseAccessMode(opts.Mode)
			if mode == "" {
				return fmt.Errorf("mode must be read-write")
			}
			response, err := client.CreateAppDatabaseAccessGrant(app.ID, model.AppDatabaseAccessGrantCreateRequest{
				Label:            strings.TrimSpace(opts.Label),
				Mode:             mode,
				ExpiresInMinutes: opts.ExpiresMinutes,
			})
			if err != nil {
				return err
			}
			return c.renderAppDatabaseAccessGrantCreate(app, response, opts.Listen)
		},
	}
	cmd.Flags().StringVar(&opts.Label, "label", "", "Grant label")
	cmd.Flags().StringVar(&opts.Mode, "mode", opts.Mode, "Access mode: read-write")
	cmd.Flags().IntVar(&opts.ExpiresMinutes, "expires-in-minutes", 0, "Grant expiration in minutes")
	cmd.Flags().StringVar(&opts.Listen, "listen", opts.Listen, "Local listen address to print in the tunnel command")
	return cmd
}

func (c *CLI) newAppDatabaseAccessRevokeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke <app> <grant-id>",
		Short: "Revoke an app database access grant",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			response, err := client.RevokeAppDatabaseAccessGrant(app.ID, args[1])
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{
					"app":      redactAppForOutput(app),
					"removed":  response.Removed,
					"grant_id": strings.TrimSpace(args[1]),
				})
			}
			return writeKeyValues(c.stdout,
				kvPair{Key: "app_id", Value: app.ID},
				kvPair{Key: "grant_id", Value: strings.TrimSpace(args[1])},
				kvPair{Key: "removed", Value: fmt.Sprintf("%t", response.Removed)},
			)
		},
	}
	return cmd
}

func (c *CLI) newAppDatabaseAccessTunnelCommand() *cobra.Command {
	opts := struct {
		GrantID string
		Token   string
		Listen  string
	}{Listen: "127.0.0.1:15432"}
	cmd := &cobra.Command{
		Use:   "tunnel <app>",
		Short: "Start a local tunnel to the app managed Postgres database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			if strings.TrimSpace(opts.GrantID) == "" || strings.TrimSpace(opts.Token) == "" {
				return fmt.Errorf("--grant-id and --token are required")
			}
			return c.serveAppDatabaseTunnel(cmd.Context(), client, app.ID, strings.TrimSpace(opts.GrantID), strings.TrimSpace(opts.Token), strings.TrimSpace(opts.Listen))
		},
	}
	cmd.Flags().StringVar(&opts.GrantID, "grant-id", "", "Access grant ID")
	cmd.Flags().StringVar(&opts.Token, "token", "", "Access grant secret")
	cmd.Flags().StringVar(&opts.Listen, "listen", opts.Listen, "Local listen address")
	_ = cmd.MarkFlagRequired("grant-id")
	_ = cmd.MarkFlagRequired("token")
	return cmd
}

func (c *CLI) waitForAppDatabaseImportJob(client *Client, appID, jobID string) (*model.AppDatabaseImportJob, error) {
	deadline := time.Now().Add(30 * time.Minute)
	for {
		response, err := client.GetAppDatabaseImport(appID)
		if err != nil {
			return nil, err
		}
		if response.Job != nil && response.Job.ID == jobID {
			switch response.Job.Status {
			case model.OperationStatusCompleted, model.OperationStatusFailed:
				return response.Job, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for database import job %s", jobID)
		}
		time.Sleep(3 * time.Second)
	}
}

func (c *CLI) renderAppDatabaseImportStatus(app model.App, job *model.AppDatabaseImportJob, action string) error {
	if c.wantsJSON() {
		payload := map[string]any{
			"app":    redactAppForOutput(app),
			"job":    job,
			"action": action,
		}
		return writeJSON(c.stdout, payload)
	}
	pairs := []kvPair{
		{Key: "app_id", Value: app.ID},
		{Key: "action", Value: action},
	}
	if job != nil {
		pairs = append(pairs,
			kvPair{Key: "job_id", Value: job.ID},
			kvPair{Key: "status", Value: job.Status},
			kvPair{Key: "format", Value: job.Format},
			kvPair{Key: "source_upload_id", Value: job.SourceUploadID},
			kvPair{Key: "retry_count", Value: fmt.Sprintf("%d", job.RetryCount)},
			kvPair{Key: "message", Value: job.ResultMessage},
			kvPair{Key: "error", Value: job.ErrorMessage},
		)
	}
	return writeKeyValues(c.stdout, pairs...)
}

func (c *CLI) renderAppDatabaseAccessResponse(app model.App, grants []model.AppDatabaseAccessGrant) error {
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":    redactAppForOutput(app),
			"grants": grants,
		})
	}
	pairs := []kvPair{
		{Key: "app_id", Value: app.ID},
		{Key: "grant_count", Value: fmt.Sprintf("%d", len(grants))},
	}
	if len(grants) > 0 {
		latest := grants[0]
		pairs = append(pairs,
			kvPair{Key: "grant_id", Value: latest.ID},
			kvPair{Key: "grant_status", Value: latest.Status},
			kvPair{Key: "grant_mode", Value: latest.Mode},
			kvPair{Key: "grant_label", Value: latest.Label},
		)
	}
	return writeKeyValues(c.stdout, pairs...)
}

func (c *CLI) renderAppDatabaseAccessGrantCreate(app model.App, response appDatabaseAccessGrantCreateResponse, listen string) error {
	grant := response.Grant
	tunnelCommand := fmt.Sprintf("fugue app db access tunnel %s --grant-id %s --token %s --listen %s", app.Name, grant.ID, response.Secret, strings.TrimSpace(listen))
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":            redactAppForOutput(app),
			"grant":          grant,
			"secret":         response.Secret,
			"listen":         strings.TrimSpace(listen),
			"tunnel_command": tunnelCommand,
		})
	}
	return writeKeyValues(c.stdout,
		kvPair{Key: "app_id", Value: app.ID},
		kvPair{Key: "grant_id", Value: grant.ID},
		kvPair{Key: "grant_status", Value: grant.Status},
		kvPair{Key: "grant_mode", Value: grant.Mode},
		kvPair{Key: "secret", Value: response.Secret},
		kvPair{Key: "listen", Value: strings.TrimSpace(listen)},
		kvPair{Key: "tunnel_command", Value: tunnelCommand},
	)
}

func (c *CLI) serveAppDatabaseTunnel(ctx context.Context, client *Client, appID, grantID, token, listen string) error {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listen, err)
	}
	defer listener.Close()

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if !c.wantsJSON() {
		_, _ = fmt.Fprintf(c.stderr, "database tunnel listening on %s\n", listen)
	}
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			if errors.Is(acceptErr, net.ErrClosed) || ctx.Err() != nil {
				break
			}
			return fmt.Errorf("accept tunnel connection: %w", acceptErr)
		}
		wg.Add(1)
		go func(localConn net.Conn) {
			defer wg.Done()
			defer localConn.Close()
			if err := c.proxyAppDatabaseConnection(ctx, client, appID, grantID, token, localConn); err != nil && ctx.Err() == nil && !c.wantsJSON() {
				if isExpectedAppDatabaseTunnelClose(err) {
					c.progressf("warning=database tunnel connection closed: %v", err)
				} else {
					c.progressf("warning=database tunnel connection failed: %v", err)
				}
			}
		}(conn)
	}
	wg.Wait()
	return nil
}

func (c *CLI) proxyAppDatabaseConnection(ctx context.Context, client *Client, appID, grantID, token string, localConn net.Conn) error {
	wsURL := client.AppDatabaseTunnelWebSocketURL(appID, grantID, token)
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	wsConn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	defer wsConn.Close()

	errCh := make(chan error, 2)
	go func() {
		buffer := make([]byte, 32*1024)
		for {
			n, readErr := localConn.Read(buffer)
			if n > 0 {
				_ = wsConn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if writeErr := wsConn.WriteMessage(websocket.BinaryMessage, buffer[:n]); writeErr != nil {
					errCh <- writeErr
					return
				}
			}
			if readErr != nil {
				errCh <- readErr
				return
			}
		}
	}()
	go func() {
		for {
			messageType, reader, nextErr := wsConn.NextReader()
			if nextErr != nil {
				if errors.Is(nextErr, io.EOF) {
					errCh <- io.EOF
					return
				}
				errCh <- nextErr
				return
			}
			if messageType != websocket.BinaryMessage {
				continue
			}
			if _, copyErr := io.Copy(localConn, reader); copyErr != nil {
				errCh <- copyErr
				return
			}
		}
	}()
	err = <-errCh
	_ = localConn.SetDeadline(time.Now())
	_ = wsConn.Close()
	return err
}

func isExpectedAppDatabaseTunnelClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		switch closeErr.Code {
		case websocket.CloseNormalClosure,
			websocket.CloseGoingAway,
			websocket.CloseNoStatusReceived,
			websocket.CloseAbnormalClosure:
			return true
		}
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "use of closed network connection") ||
		strings.Contains(message, "connection reset by peer") ||
		strings.Contains(message, "broken pipe")
}

func normalizeCLIAppDatabaseImportFormat(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseImportFormatAuto:
		return model.AppDatabaseImportFormatAuto
	case model.AppDatabaseImportFormatSQL:
		return model.AppDatabaseImportFormatSQL
	case model.AppDatabaseImportFormatCustom:
		return model.AppDatabaseImportFormatCustom
	default:
		return model.AppDatabaseImportFormatAuto
	}
}

func normalizeCLIAppDatabaseAccessMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseAccessModeReadWrite:
		return model.AppDatabaseAccessModeReadWrite
	default:
		return ""
	}
}
