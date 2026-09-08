package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/spf13/cobra"
	"sort"
	"time"
)

func (c *CLI) newCapabilitiesCommand() *cobra.Command {
	var local bool
	cmd := &cobra.Command{Use: "capabilities", Short: "Report local commands, advertised server API operations and current principal scopes separately", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		result := map[string]any{"schema_version": 1, "cli": currentCLIBuildInfo(), "authorization": "enforced_by_server", "feature_enablement": "unknown"}
		if local {
			result["commands"] = commandCatalog(cmd.Root(), false)
			return c.renderResourceResult(result)
		}
		client, err := c.newClient()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
		defer cancel()
		client.context = ctx
		principal, err := client.GetAuthContext()
		if err != nil {
			return err
		}
		result["principal"] = principal.Principal
		var contract struct {
			Info  map[string]any                        `json:"info"`
			Paths map[string]map[string]json.RawMessage `json:"paths"`
		}
		body, err := client.doJSONRaw("GET", "/openapi.json", nil)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(body, &contract); err != nil {
			return err
		}
		ops := []string{}
		for _, methods := range contract.Paths {
			for method, raw := range methods {
				if method != "get" && method != "post" && method != "put" && method != "patch" && method != "delete" && method != "head" && method != "options" {
					continue
				}
				var op struct {
					ID string `json:"operationId"`
				}
				if err := json.Unmarshal(raw, &op); err == nil && op.ID != "" {
					ops = append(ops, op.ID)
				}
			}
		}
		sort.Strings(ops)
		digest := sha256.Sum256(body)
		result["server"] = map[string]any{"contract_info": contract.Info, "contract_sha256": hex.EncodeToString(digest[:]), "advertised_operations": ops, "retrieved_at": time.Now().UTC()}
		return c.renderResourceResult(result)
	}}
	cmd.Flags().BoolVar(&local, "local", false, "Only inspect local CLI support; makes no API requests")
	return cmd
}
