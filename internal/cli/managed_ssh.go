package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"fugue/internal/model"
	"github.com/spf13/cobra"
)

func (client *Client) GetManagedSSHIntent(appID string) (model.ManagedSSHIntent, error) {
	var response struct {
		Intent model.ManagedSSHIntent `json:"intent"`
	}
	err := client.doJSON(http.MethodGet, path.Join("/v1/apps", appID, "ssh", "managed"), nil, &response)
	return response.Intent, err
}

func (client *Client) PutManagedSSHIntent(appID string, expected uint64, spec json.RawMessage) (model.ManagedSSHIntent, error) {
	var response struct {
		Intent model.ManagedSSHIntent `json:"intent"`
	}
	err := client.doJSON(http.MethodPut, path.Join("/v1/apps", appID, "ssh", "managed"), map[string]any{"expected_generation": expected, "spec": spec}, &response)
	return response.Intent, err
}

func (cli *CLI) newAppSSHManagedCommand() *cobra.Command {
	command := &cobra.Command{Use: "managed", Short: "Manage platform SSH configuration independently of application releases"}
	command.AddCommand(&cobra.Command{Use: "show <app>", Short: "Read managed SSH intent", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, arguments []string) error {
		client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
		if err != nil {
			return err
		}
		intent, err := client.GetManagedSSHIntent(app.ID)
		if err != nil {
			return err
		}
		return cli.writeJSON(map[string]any{"intent": intent})
	}})
	var filename string
	var expected uint64
	apply := &cobra.Command{Use: "apply <app>", Short: "Save a reviewed intent file with an explicit generation precondition", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, arguments []string) error {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
		if err != nil {
			return err
		}
		if len(raw) > 64<<10 {
			return fmt.Errorf("managed SSH intent file exceeds 64 KiB")
		}
		var spec model.ManagedSSHIntentSpec
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&spec); err != nil {
			return err
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return fmt.Errorf("expected one managed SSH spec JSON object")
		}
		client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
		if err != nil {
			return err
		}
		intent, err := client.PutManagedSSHIntent(app.ID, expected, json.RawMessage(raw))
		if err != nil {
			return err
		}
		return cli.writeJSON(map[string]any{"intent": intent})
	}}
	apply.Flags().StringVar(&filename, "file", "", "ManagedSSHIntentSpec JSON file")
	apply.Flags().Uint64Var(&expected, "expected-generation", 0, "Expected stored generation; zero creates initial intent")
	_ = apply.MarkFlagRequired("file")
	_ = apply.MarkFlagRequired("expected-generation")
	command.AddCommand(apply)
	command.AddCommand(cli.newManagedSSHStatusCommand())
	for _, action := range []string{"binding", "activate", "rollback"} {
		command.AddCommand(cli.newManagedSSHReviewedCommand(action))
	}
	return command
}

func (cli *CLI) newManagedSSHStatusCommand() *cobra.Command {
	return &cobra.Command{Use: "status <app>", Short: "Read independent serving state, candidates, and verification evidence", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, arguments []string) error {
		client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
		if err != nil {
			return err
		}
		var response json.RawMessage
		if err := client.doJSON(http.MethodGet, path.Join("/v1/apps", app.ID, "ssh/managed/status"), nil, &response); err != nil {
			return err
		}
		return cli.writeJSON(response)
	}}
}

func (cli *CLI) newManagedSSHReviewedCommand(action string) *cobra.Command {
	var filename string
	command := &cobra.Command{Use: action + " <app>", Short: "Submit an explicitly reviewed managed SSH " + action + " request", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, arguments []string) error {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		raw, err := io.ReadAll(io.LimitReader(file, 8193))
		if err != nil {
			return err
		}
		if len(raw) > 8192 || !json.Valid(raw) {
			return fmt.Errorf("expected one reviewed JSON request of at most 8 KiB")
		}
		client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
		if err != nil {
			return err
		}
		var response json.RawMessage
		if err := client.doJSON(http.MethodPost, path.Join("/v1/apps", app.ID, "ssh/managed", action), json.RawMessage(raw), &response); err != nil {
			return err
		}
		return cli.writeJSON(response)
	}}
	command.Flags().StringVar(&filename, "file", "", "Reviewed API request JSON including exact preconditions")
	_ = command.MarkFlagRequired("file")
	return command
}
