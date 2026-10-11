package cli

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func (client *Client) GetAppSSHIdentity(appID string) (model.AppSSHIdentity, error) {
	return client.requestAppSSHIdentity(http.MethodGet, appID)
}

func (client *Client) PrepareAppSSHIdentity(appID string) (model.AppSSHIdentity, error) {
	return client.requestAppSSHIdentity(http.MethodPost, appID)
}

func (client *Client) requestAppSSHIdentity(method, appID string) (model.AppSSHIdentity, error) {
	var response struct {
		Identity model.AppSSHIdentity `json:"identity"`
	}
	if err := client.doJSON(method, path.Join("/v1/apps", appID, "ssh", "identity"), nil, &response); err != nil {
		return model.AppSSHIdentity{}, err
	}
	return response.Identity, nil
}

func (cli *CLI) newAppSSHIdentityCommand() *cobra.Command {
	command := &cobra.Command{Use: "identity", Short: "Inspect or prepare a managed SSH host identity"}
	for _, operation := range []struct {
		name, description string
		prepare           bool
	}{
		{"show", "Show the prepared public SSH identity; does not imply activation", false},
		{"prepare", "Prepare a durable SSH identity without changing routing or workloads", true},
	} {
		command.AddCommand(&cobra.Command{
			Use:   operation.name + " <app>",
			Short: operation.description,
			Args:  cobra.ExactArgs(1),
			RunE: func(command *cobra.Command, arguments []string) error {
				client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
				if err != nil {
					return err
				}
				var identity model.AppSSHIdentity
				if operation.prepare {
					identity, err = client.PrepareAppSSHIdentity(app.ID)
				} else {
					identity, err = client.GetAppSSHIdentity(app.ID)
				}
				if err != nil {
					return err
				}
				if cli.wantsJSON() {
					return cli.writeJSON(map[string]any{"identity": identity})
				}
				return writeKeyValues(cli.stdout,
					kvPair{Key: "app", Value: app.Name},
					kvPair{Key: "identity", Value: identity.ID},
					kvPair{Key: "generation", Value: strconv.FormatUint(identity.Generation, 10)},
					kvPair{Key: "fingerprint", Value: identity.Fingerprint},
					kvPair{Key: "public_key", Value: identity.PublicKey},
					kvPair{Key: "scope", Value: "prepared identity; activation and client trust require separate verification"},
				)
			},
		})
	}
	command.AddCommand(&cobra.Command{
		Use:   "observe <app>",
		Short: "Verify the public endpoint against its prepared identity without logging in",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
			if err != nil {
				return err
			}
			identity, err := client.GetAppSSHIdentity(app.ID)
			if err != nil {
				return err
			}
			var response struct {
				Identity    model.AppSSHIdentity    `json:"identity"`
				Observation model.AppSSHObservation `json:"observation"`
			}
			err = client.doJSON(http.MethodPost, path.Join("/v1/apps", app.ID, "ssh", "identity", "observe"), map[string]string{"identity_id": identity.ID}, &response)
			if err != nil {
				return err
			}
			if cli.wantsJSON() {
				return cli.writeJSON(response)
			}
			return writeKeyValues(cli.stdout,
				kvPair{Key: "identity", Value: response.Identity.ID},
				kvPair{Key: "expected_fingerprint", Value: response.Identity.Fingerprint},
				kvPair{Key: "observed_fingerprint", Value: response.Observation.Fingerprint},
				kvPair{Key: "identity_matched", Value: fmt.Sprint(response.Observation.IdentityMatched)},
				kvPair{Key: "reason", Value: response.Observation.Reason},
				kvPair{Key: "scope", Value: response.Observation.Scope},
				kvPair{Key: "observer", Value: response.Observation.Observer},
				kvPair{Key: "expires_at", Value: formatTime(response.Observation.ExpiresAt)},
			)
		},
	})
	command.AddCommand(cli.newSSHTrustPlanCommand())
	return command
}

func (cli *CLI) newSSHTrustPlanCommand() *cobra.Command {
	var knownHostsPath string
	command := &cobra.Command{
		Use: "trust-plan <app>", Short: "Review an observed SSH identity against local trust without editing it", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, arguments []string) error {
			client, app, err := cli.resolveAppSSHClientAndApp(arguments[0])
			if err != nil {
				return err
			}
			identity, err := client.GetAppSSHIdentity(app.ID)
			if err != nil {
				return err
			}
			response, err := client.GetAppSSH(app.ID)
			if err != nil {
				return err
			}
			if !response.SSH.IdentityVerified || !response.SSH.Ready || response.SSH.IdentityID != identity.ID || response.SSH.Observation == nil || !response.SSH.Observation.ExpiresAt.After(time.Now()) || response.SSH.HostKeyFingerprint != identity.Fingerprint {
				return fmt.Errorf("a fresh matching public endpoint observation is required; run app ssh identity observe first")
			}
			if knownHostsPath == "" {
				directory, err := os.UserHomeDir()
				if err != nil {
					return err
				}
				knownHostsPath = filepath.Join(directory, ".ssh", "known_hosts")
			}
			publicKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(identity.PublicKey))
			if err != nil {
				return err
			}
			if ssh.FingerprintSHA256(publicKey) != identity.Fingerprint {
				return fmt.Errorf("managed identity public key does not match its fingerprint")
			}
			host := response.SSH.Hostname
			if strings.ContainsAny(host, " \t\r\n,|*?!") {
				return fmt.Errorf("SSH endpoint hostname cannot be represented safely in known_hosts")
			}
			target := net.JoinHostPort(host, strconv.Itoa(response.SSH.PublicPort))
			state := "new_host"
			lines := []int{}
			previous := []string{}
			if _, err := os.Stat(knownHostsPath); err == nil {
				callback, err := knownhosts.New(knownHostsPath)
				if err != nil {
					return err
				}
				err = callback(target, sshTrustAddress(target), publicKey)
				if err == nil {
					state = "already_trusted"
				} else {
					mismatch, ok := err.(*knownhosts.KeyError)
					if !ok {
						return err
					}
					if len(mismatch.Want) > 0 {
						state = "replacement_requires_review"
					}
					for _, wanted := range mismatch.Want {
						lines = append(lines, wanted.Line)
						previous = append(previous, ssh.FingerprintSHA256(wanted.Key))
					}
				}
			} else if !os.IsNotExist(err) {
				return err
			}
			plan := map[string]any{"app_id": app.ID, "identity_id": identity.ID, "endpoint": target, "fingerprint": identity.Fingerprint, "known_hosts_path": knownHostsPath, "state": state, "existing_lines": lines, "existing_fingerprints": previous, "proposed_entry": knownhosts.Line([]string{target}, publicKey), "modified": false, "observation": response.SSH.Observation}
			if cli.wantsJSON() {
				return cli.writeJSON(plan)
			}
			return writeKeyValues(cli.stdout, kvPair{Key: "identity", Value: identity.ID}, kvPair{Key: "endpoint", Value: target}, kvPair{Key: "fingerprint", Value: identity.Fingerprint}, kvPair{Key: "state", Value: state}, kvPair{Key: "known_hosts", Value: knownHostsPath}, kvPair{Key: "existing_lines", Value: fmt.Sprint(lines)}, kvPair{Key: "proposed_entry", Value: knownhosts.Line([]string{target}, publicKey)}, kvPair{Key: "modified", Value: "false"})
		},
	}
	command.Flags().StringVar(&knownHostsPath, "known-hosts", "", "Known hosts file to inspect without modifying")
	return command
}

type sshTrustAddress string

func (address sshTrustAddress) Network() string { return "tcp" }
func (address sshTrustAddress) String() string  { return string(address) }
