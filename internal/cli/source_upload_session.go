package cli

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

type uploadSessionResponse struct {
	Session model.SourceUploadSession `json:"session"`
}
type uploadSubmitResponse struct {
	Session  model.SourceUploadSession `json:"session"`
	Result   *importUploadResponse     `json:"result,omitempty"`
	Replayed bool                      `json:"replayed,omitempty"`
	Error    any                       `json:"error,omitempty"`
}

func (client *Client) getUploadSession(id string) (model.SourceUploadSession, error) {
	var out uploadSessionResponse
	err := client.doJSON(http.MethodGet, "/v1/source-upload-sessions/"+url.PathEscape(id), nil, &out)
	return out.Session, err
}
func (client *Client) resumeUploadSession(v model.SourceUploadSession, archive []byte) (model.SourceUploadSession, error) {
	sum := fmt.Sprintf("%x", sha256.Sum256(archive))
	if sum != v.SHA256 || int64(len(archive)) != v.SizeBytes {
		return v, withExitCode(fmt.Errorf("archive differs from the frozen request; use the original archive or a new request ID"), ExitCodeUserInput)
	}
	if v.ChunkSize != 4<<20 || len(archive) > maxSourceArchiveUploadBytes {
		return v, fmt.Errorf("unsupported upload session protocol")
	}
	if v.UploadID != "" {
		return v, nil
	}
	for offset, index := 0, 0; offset < len(archive); offset, index = offset+v.ChunkSize, index+1 {
		data := archive[offset:min(offset+v.ChunkSize, len(archive))]
		digest := fmt.Sprintf("%x", sha256.Sum256(data))
		if previous := v.Chunks[index]; previous != "" {
			if previous != digest {
				return v, fmt.Errorf("server chunk %d differs from archive", index)
			}
			continue
		}
		var out uploadSessionResponse
		err := client.doJSON(http.MethodPut, fmt.Sprintf("/v1/source-upload-sessions/%s/chunks/%d", url.PathEscape(v.ID), index), map[string]any{"sha256": digest, "data": data}, &out)
		if err != nil {
			return v, err
		}
		v = out.Session
	}
	var out uploadSessionResponse
	err := client.doJSONWithTimeout(http.MethodPost, "/v1/source-upload-sessions/"+url.PathEscape(v.ID)+"/complete", nil, &out, sourceUploadClientTimeout)
	if err != nil {
		return v, err
	}
	return out.Session, nil
}
func (c *CLI) importUploadResumable(client *Client, requestID string, req importUploadRequest, name string, archive []byte) (importUploadResponse, error) {
	var empty importUploadResponse
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(requestID) {
		return empty, withExitCode(fmt.Errorf("request-id must contain 1–128 letters, digits, underscores, dots or hyphens"), ExitCodeUserInput)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(archive))
	// Persist the exact archive before the first HTTP call. Repacking changed
	// working files must never silently change the meaning of a retry.
	targetHash := fmt.Sprintf("%x", sha256.Sum256([]byte(client.baseURL+"\n"+req.TenantID+"\n"+requestID)))
	dir := filepath.Join(filepath.Dir(authConfigPath()), "source-uploads", targetHash)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return empty, err
	}
	archivePath := filepath.Join(dir, "archive.tgz")
	if existing, err := os.ReadFile(archivePath); err == nil {
		if fmt.Sprintf("%x", sha256.Sum256(existing)) != hash {
			return empty, withExitCode(fmt.Errorf("request-id already has a different local archive; recover it with operation recover --request-id %s", requestID), ExitCodeUserInput)
		}
	} else if !os.IsNotExist(err) {
		return empty, err
	} else if err = os.WriteFile(archivePath, archive, 0600); err != nil {
		return empty, err
	}
	receipt := map[string]any{"schema_version": 1, "request_id": requestID, "api_url": client.baseURL, "tenant_id": req.TenantID, "archive_path": archivePath, "sha256": hash, "created_at": time.Now().UTC()}
	save := func() error {
		raw, err := json.MarshalIndent(receipt, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, "receipt.json"), raw, 0600)
	}
	if err := save(); err != nil {
		return empty, err
	}
	c.progressf("request_id=%s receipt=%s", requestID, filepath.Join(dir, "receipt.json"))
	fail := func(v model.SourceUploadSession, err error) (importUploadResponse, error) {
		followup := fmt.Sprintf("fugue operation recover --request-id %s", requestID)
		if c.wantsJSON() {
			_ = c.writeJSON(map[string]any{"schema_version": 1, "outcome": "unknown", "request_id": requestID, "session": v, "receipt_path": filepath.Join(dir, "receipt.json"), "error": describeCommandError(err), "next_commands": []string{followup}})
		}
		return empty, fmt.Errorf("%w; recovery: %s; original archive: %s", err, followup, archivePath)
	}
	var out uploadSessionResponse
	err := client.doJSON(http.MethodPost, "/v1/source-upload-sessions", map[string]any{"request_id": requestID, "tenant_id": req.TenantID, "filename": name, "size_bytes": len(archive), "sha256": hash}, &out)
	if err != nil {
		return fail(out.Session, err)
	}
	receipt["session_id"] = out.Session.ID
	if err = save(); err != nil {
		return fail(out.Session, err)
	}
	v, err := client.resumeUploadSession(out.Session, archive)
	if err != nil {
		return fail(v, err)
	}
	submitted, err := client.submitUploadSession(v.ID, req)
	if err != nil {
		return fail(v, err)
	}
	if submitted.Result != nil {
		return *submitted.Result, nil
	}
	if submitted.Session.State != "submitted" {
		return fail(submitted.Session, withExitCode(fmt.Errorf("request state is %s; inspect the receipt before any new submission", submitted.Session.State), ExitCodeIndeterminate))
	}
	// Read the exact durable associations, never all operations for an upload.
	recovered, err := client.recoverUploadResult(submitted.Session)
	if err != nil {
		return fail(submitted.Session, err)
	}
	return recovered, nil
}
func (client *Client) submitUploadSession(id string, req importUploadRequest) (uploadSubmitResponse, error) {
	var out uploadSubmitResponse
	err := client.doJSONWithTimeout(http.MethodPost, "/v1/source-upload-sessions/"+url.PathEscape(id)+"/submit", req, &out, sourceUploadClientTimeout)
	return out, err
}
func (client *Client) recoverUploadResult(v model.SourceUploadSession) (importUploadResponse, error) {
	out := importUploadResponse{}
	for _, id := range v.AppIDs {
		app, err := client.GetApp(id)
		if err != nil {
			return out, err
		}
		out.Apps = append(out.Apps, app)
	}
	for _, id := range v.OperationIDs {
		op, err := client.GetOperation(id)
		if err != nil {
			return out, err
		}
		out.Operations = append(out.Operations, op)
	}
	if len(out.Apps) > 0 {
		out.App = &out.Apps[0]
	}
	if len(out.Operations) > 0 {
		out.Operation = &out.Operations[0]
	}
	return out, nil
}
func (c *CLI) newSourceUploadStatusCommand() *cobra.Command {
	return &cobra.Command{Use: "status <session>", Short: "Inspect durable upload chunks and exact submission effects", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := client.getUploadSession(args[0])
		if err != nil {
			return err
		}
		return c.renderResourceResult(map[string]any{"session": v})
	}}
}
func (c *CLI) newSourceUploadResumeCommand() *cobra.Command {
	return &cobra.Command{Use: "resume <session> <archive.tgz>", Short: "Resume missing chunks from the original archive; does not deploy", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := client.getUploadSession(args[0])
		if err != nil {
			return err
		}
		info, err := os.Stat(args[1])
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > maxSourceArchiveUploadBytes {
			return fmt.Errorf("archive must be a regular file no larger than 128 MiB")
		}
		data, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		v, err = client.resumeUploadSession(v, data)
		if outputErr := c.renderResourceResult(map[string]any{"session": v}); outputErr != nil {
			return outputErr
		}
		return err
	}}
}
func (c *CLI) newOperationRecoverCommand() *cobra.Command {
	var requestID string
	cmd := &cobra.Command{Use: "recover", Short: "Recover operations by durable source request ID without resubmitting", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if requestID == "" {
			return fmt.Errorf("request-id is required")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		tenant, err := c.resolveTenantSelection(client, c.effectiveTenantID(), c.effectiveTenantName())
		if err != nil {
			return err
		}
		var out uploadSessionResponse
		err = client.doJSON(http.MethodGet, "/v1/source-upload-requests/"+url.PathEscape(requestID)+"?tenant_id="+url.QueryEscape(tenant), nil, &out)
		if err != nil {
			return err
		}
		result, readErr := client.recoverUploadResult(out.Session)
		report := map[string]any{"schema_version": 1, "request_id": requestID, "session": out.Session, "result": result, "resubmitted": false}
		if readErr != nil {
			report["error"] = describeCommandError(readErr)
		}
		if err = c.renderResourceResult(report); err != nil {
			return err
		}
		if readErr != nil {
			return readErr
		}
		if out.Session.State == "submitting" || out.Session.State == "unknown" {
			return withExitCode(fmt.Errorf("request has incomplete evidence; no request was resubmitted"), ExitCodeIndeterminate)
		}
		return nil
	}}
	cmd.Flags().StringVar(&requestID, "request-id", "", "Exact request ID from deploy --request-id")
	cmd.Example = "fugue operation recover --request-id source-change-001"
	_ = cmd.MarkFlagRequired("request-id")
	return cmd
}
