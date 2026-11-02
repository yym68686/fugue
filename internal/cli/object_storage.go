package cli

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

type objectStoreEnvelope struct {
	Store model.ObjectStore `json:"store"`
}
type objectStoreDeleteResponse = model.ObjectStoreDeleteResponse
type objectStorageConnection struct {
	Credential      model.ObjectStorageCredential `json:"credential"`
	Endpoint        string                        `json:"endpoint"`
	Region          string                        `json:"region"`
	Bucket          string                        `json:"bucket"`
	AccessKeyID     string                        `json:"access_key_id"`
	SecretAccessKey string                        `json:"secret_access_key"`
}

func (c *Client) listObjectStores(tenant, project string) ([]model.ObjectStore, error) {
	var out struct {
		Stores []model.ObjectStore `json:"stores"`
	}
	q := url.Values{"tenant_id": {tenant}, "project_id": {project}}
	err := c.doJSON(http.MethodGet, "/v1/object-stores?"+q.Encode(), nil, &out)
	return out.Stores, err
}
func (c *CLI) findObjectStore(client *Client, ref string) (model.ObjectStore, error) {
	tenant, project, err := c.resolveFilterSelections(client)
	if err != nil {
		return model.ObjectStore{}, err
	}
	all, err := client.listObjectStores(tenant, project)
	if err != nil {
		return model.ObjectStore{}, err
	}
	matches := []model.ObjectStore{}
	for _, v := range all {
		if v.ID == ref || v.Name == ref {
			matches = append(matches, v)
		}
	}
	if len(matches) != 1 {
		return model.ObjectStore{}, fmt.Errorf("object store %q matched %d resources; select --project", ref, len(matches))
	}
	return matches[0], nil
}
func (c *CLI) newObjectStorageCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "s3", Short: "Provision private project object storage and application S3 credentials"}
	var account, tokenFile string
	configure := &cobra.Command{Use: "configure", Short: "Configure platform R2 management (administrator only)", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		token := os.Getenv("FUGUE_S3_CF_API_TOKEN")
		if tokenFile != "" {
			b, err := os.ReadFile(tokenFile)
			if err != nil {
				return err
			}
			token = strings.TrimSpace(string(b))
		}
		if token == "" || account == "" {
			return fmt.Errorf("--account-id and FUGUE_S3_CF_API_TOKEN or --token-file are required")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		var out map[string]any
		if err = client.doJSON(http.MethodPut, "/v1/admin/object-storage", map[string]string{"account_id": account, "api_token": token}, &out); err != nil {
			return err
		}
		return c.writeJSON(out)
	}}
	configure.Flags().StringVar(&account, "account-id", "", "Cloudflare account ID")
	configure.Flags().StringVar(&tokenFile, "token-file", "", "Read management API token from a private file")
	cmd.AddCommand(configure, &cobra.Command{Use: "ls", Aliases: []string{"list"}, Short: "List object stores in the selected account/project", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		tenant, project, err := c.resolveFilterSelections(client)
		if err != nil {
			return err
		}
		items, err := client.listObjectStores(tenant, project)
		if err != nil {
			return err
		}
		return c.writeJSON(map[string]any{"stores": items})
	}})
	var quota int64
	create := &cobra.Command{Use: "create <name>", Short: "Explicitly create a private bucket for the selected project", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		tenant, project, err := c.resolveFilterSelections(client)
		if err != nil {
			return err
		}
		if project == "" {
			return fmt.Errorf("select --project before creating object storage")
		}
		var out objectStoreEnvelope
		err = client.doJSON(http.MethodPost, "/v1/object-stores", map[string]any{"tenant_id": tenant, "project_id": project, "name": args[0], "quota_bytes": quota}, &out)
		if err != nil {
			return err
		}
		return c.writeJSON(out)
	}}
	create.Flags().Int64Var(&quota, "quota-bytes", 0, "Soft storage budget in bytes; 0 disables the budget")
	cmd.AddCommand(create)
	plan := &cobra.Command{Use: "deletion-plan <store>", Short: "Show a read-only, cached deletion plan (no provider calls)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := c.findObjectStore(client, args[0])
		if err != nil {
			return err
		}
		var out objectStoreDeleteResponse
		if err = client.doJSON(http.MethodPost, "/v1/object-stores/"+url.PathEscape(v.ID)+"/deletion-plan", nil, &out); err != nil {
			return err
		}
		return c.writeJSON(out)
	}}
	cmd.AddCommand(plan)
	var confirmBucket, planSHA256 string
	var execute, ackExternal, wait bool
	var maxBatches int
	deleteCmd := &cobra.Command{Use: "delete <store>", Short: "Preview deletion; --execute permanently removes all objects and the bucket", Long: "Preview the exact managed bucket and cached usage. Execution deletes ALL objects and unfinished uploads, revokes Fugue grants, removes the provider bucket and finally removes Fugue metadata. Stop external writers first. The plan pins bucket identity, not an object snapshot. Interrupted deletion stays fenced and can be resumed with the same confirmation flags. --wait=false performs one batch; counters are lower bounds after interruptions.", Example: "fugue s3 delete events\nfugue s3 deletion-plan events", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if maxBatches <= 0 {
			return fmt.Errorf("--max-batches must be positive")
		}
		if !execute && (confirmBucket != "" || planSHA256 != "" || ackExternal) {
			return fmt.Errorf("confirmation flags require --execute")
		}
		digest, digestErr := hex.DecodeString(planSHA256)
		if execute && (strings.TrimSpace(confirmBucket) == "" || digestErr != nil || len(digest) != 32) {
			return fmt.Errorf("--confirm-bucket and --plan-sha256 from `s3 deletion-plan` are required")
		}
		if execute && !ackExternal {
			return fmt.Errorf("refusing destructive operation: pass --ack-no-external-writers after stopping every external writer")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := c.findObjectStore(client, args[0])
		if err != nil {
			return err
		}
		if !execute {
			var preview objectStoreDeleteResponse
			if err = client.doJSON(http.MethodPost, "/v1/object-stores/"+url.PathEscape(v.ID)+"/deletion-plan", nil, &preview); err != nil {
				return err
			}
			return c.writeJSON(preview)
		}
		if confirmBucket != v.Bucket {
			return fmt.Errorf("--confirm-bucket does not match the selected store")
		}
		request := map[string]any{"confirm_bucket": confirmBucket, "plan_sha256": strings.ToLower(planSHA256), "ack_no_external_writers": true}
		var out objectStoreDeleteResponse
		for batch := 0; ; batch++ {
			if err = cmd.Context().Err(); err != nil {
				return err
			}
			if batch >= maxBatches {
				return fmt.Errorf("deletion exceeded --max-batches=%d; rerun to resume", maxBatches)
			}
			out = objectStoreDeleteResponse{}
			if err = client.doJSONWithTimeout(http.MethodDelete, "/v1/object-stores/"+url.PathEscape(v.ID), request, &out, 60*time.Second); err != nil {
				return fmt.Errorf("deletion stopped; inspect `s3 show` and rerun the same confirmed command to resume: %w", err)
			}
			if out.Store.ID != v.ID || out.Store.Bucket != v.Bucket {
				return fmt.Errorf("invalid deletion response target; refusing to continue")
			}
			if out.Deleted || !wait {
				break
			}
			if !c.wantsJSON() && out.Store.Deletion != nil {
				fmt.Fprintf(c.stderr, "Deletion in progress: at least %d objects and %d unfinished uploads removed.\n", out.Store.Deletion.DeletedObjects, out.Store.Deletion.AbortedMultipartUploads)
			}
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		return c.writeJSON(out)
	}}
	deleteCmd.Flags().StringVar(&confirmBucket, "confirm-bucket", "", "Exact bucket from the deletion plan")
	deleteCmd.Flags().StringVar(&planSHA256, "plan-sha256", "", "Digest from the deletion plan")
	deleteCmd.Flags().BoolVar(&ackExternal, "ack-no-external-writers", false, "Confirm all external writers have been stopped")
	deleteCmd.Flags().BoolVar(&execute, "execute", false, "Required acknowledgement for permanent deletion")
	deleteCmd.Flags().BoolVar(&wait, "wait", true, "Continue bounded deletion batches until the bucket is removed")
	deleteCmd.Flags().IntVar(&maxBatches, "max-batches", 10000, "Maximum resumable deletion batches")
	cmd.AddCommand(deleteCmd)
	for _, action := range []string{"show", "enable", "disable", "usage"} {
		action := action
		cmd.AddCommand(&cobra.Command{Use: action + " <store>", Short: map[string]string{"show": "Show connection metadata without secrets", "enable": "Enable issuing new credentials", "disable": "Revoke all credentials and retain objects", "usage": "Measure bucket size and object count"}[action], Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			v, err := c.findObjectStore(client, args[0])
			if err != nil {
				return err
			}
			if action == "show" {
				return c.writeJSON(objectStoreEnvelope{v})
			}
			path := "/v1/object-stores/" + url.PathEscape(v.ID)
			method := http.MethodPatch
			var body any = map[string]bool{"enabled": action == "enable"}
			if action == "usage" {
				method = http.MethodPost
				path += "/usage"
				body = nil
			}
			var out objectStoreEnvelope
			if err = client.doJSON(method, path, body, &out); err != nil {
				return err
			}
			return c.writeJSON(out)
		}})
	}
	var appRef, credentialName, permission, envFile string
	var bind bool
	credential := &cobra.Command{Use: "credential <store>", Short: "Issue an application credential, saving secrets to a private env file or binding to the app", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if envFile == "" && !bind {
			return fmt.Errorf("use --env-file or --bind; secrets are never printed")
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := c.findObjectStore(client, args[0])
		if err != nil {
			return err
		}
		app, err := resolveAppReference(client, appRef, "", v.TenantID)
		if err != nil {
			return err
		}
		if credentialName == "" {
			return fmt.Errorf("--name required; reuse the same name to recover a lost response")
		}
		var out objectStorageConnection
		err = client.doJSON(http.MethodPost, "/v1/object-stores/"+url.PathEscape(v.ID)+"/credentials", map[string]string{"name": credentialName, "app_id": app.ID, "permission": permission}, &out)
		if err != nil {
			return err
		}
		env := map[string]string{"S3_ENDPOINT": out.Endpoint, "S3_BUCKET": out.Bucket, "AWS_REGION": out.Region, "AWS_ACCESS_KEY_ID": out.AccessKeyID, "AWS_SECRET_ACCESS_KEY": out.SecretAccessKey}
		if envFile != "" {
			if err = writePrivateObjectStorageEnv(envFile, env); err != nil {
				return err
			}
		}
		if bind {
			if _, err = client.PatchAppEnv(app.ID, env, nil); err != nil {
				return err
			}
		}
		return c.writeJSON(map[string]any{"credential": out.Credential, "env_file": envFile, "bound": bind})
	}}
	credential.Flags().StringVar(&appRef, "app", "", "Application name or ID receiving this grant")
	credential.Flags().StringVar(&credentialName, "name", "", "Unique credential name within the bucket")
	credential.Flags().StringVar(&permission, "permission", "read-only", "read-only or read-write")
	credential.Flags().StringVar(&envFile, "env-file", "", "Create a private env file (never overwrite an existing file)")
	credential.Flags().BoolVar(&bind, "bind", false, "Merge credentials into the selected application's environment")
	cmd.AddCommand(credential, &cobra.Command{Use: "credentials <store>", Short: "List issued credentials without secrets", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := c.findObjectStore(client, args[0])
		if err != nil {
			return err
		}
		var out map[string]any
		if err = client.doJSON(http.MethodGet, "/v1/object-stores/"+url.PathEscape(v.ID)+"/credentials", nil, &out); err != nil {
			return err
		}
		return c.writeJSON(out)
	}}, &cobra.Command{Use: "revoke <store> <credential-id>", Short: "Revoke one application credential at the storage provider", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		client, err := c.newClient()
		if err != nil {
			return err
		}
		v, err := c.findObjectStore(client, args[0])
		if err != nil {
			return err
		}
		var out map[string]any
		if err = client.doJSON(http.MethodDelete, "/v1/object-stores/"+url.PathEscape(v.ID)+"/credentials/"+url.PathEscape(args[1]), nil, &out); err != nil {
			return err
		}
		return c.writeJSON(out)
	}})
	return cmd
}
func writePrivateObjectStorageEnv(path string, env map[string]string) error {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v := env[k]
		if strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("invalid storage connection value")
		}
		fmt.Fprintf(&b, "%s=%s\n", k, v)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.WriteString(b.String()); err != nil {
		_ = os.Remove(path)
		return err
	}
	return f.Sync()
}
