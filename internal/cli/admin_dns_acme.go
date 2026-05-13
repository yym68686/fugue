package cli

import (
	"fmt"
	"io"
	"net"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"

	miekgdns "github.com/miekg/dns"
	"github.com/spf13/cobra"
)

func (c *CLI) newAdminDNSACMECommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acme",
		Short: "Manage temporary DNS-01 ACME TXT challenges",
	}
	cmd.AddCommand(
		c.newAdminDNSACMEListCommand(),
		c.newAdminDNSACMEPresentCommand(),
		c.newAdminDNSACMECleanupCommand(),
	)
	return cmd
}

func (c *CLI) newAdminDNSACMEListCommand() *cobra.Command {
	opts := struct {
		Zone           string
		IncludeExpired bool
	}{
		Zone: "fugue.pro",
	}
	cmd := &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List DNS-01 ACME TXT challenges",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.ListDNSACMEChallenges(opts.Zone, opts.IncludeExpired)
			if err != nil {
				return err
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeDNSACMEChallengeTable(c.stdout, response.Challenges)
		},
	}
	cmd.Flags().StringVar(&opts.Zone, "zone", opts.Zone, "DNS zone to list")
	cmd.Flags().BoolVar(&opts.IncludeExpired, "include-expired", false, "Include expired challenges")
	return cmd
}

func (c *CLI) newAdminDNSACMEPresentCommand() *cobra.Command {
	opts := struct {
		Zone         string
		TTL          int
		ExpiresIn    time.Duration
		Wait         bool
		WaitTimeout  time.Duration
		WaitInterval time.Duration
	}{
		Zone:         "fugue.pro",
		TTL:          60,
		ExpiresIn:    time.Hour,
		Wait:         true,
		WaitTimeout:  2 * time.Minute,
		WaitInterval: 2 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "present <name> <txt-value>",
		Short: "Create or refresh a DNS-01 ACME TXT challenge",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.ExpiresIn <= 0 {
				return fmt.Errorf("--expires-in must be positive")
			}
			expiresInSeconds := int(opts.ExpiresIn.Seconds())
			if expiresInSeconds <= 0 {
				expiresInSeconds = 1
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			response, err := client.UpsertDNSACMEChallenge(upsertDNSACMEChallengeClientRequest{
				Zone:             opts.Zone,
				Name:             args[0],
				Value:            args[1],
				TTL:              opts.TTL,
				ExpiresInSeconds: expiresInSeconds,
			})
			if err != nil {
				return err
			}
			if opts.Wait {
				if err := waitForDNSACMEChallenge(client, opts.Zone, response.Challenge.Name, response.Challenge.Value, opts.WaitTimeout, opts.WaitInterval); err != nil {
					return err
				}
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, response)
			}
			return writeDNSACMEChallenge(c.stdout, response.Challenge)
		},
	}
	cmd.Flags().StringVar(&opts.Zone, "zone", opts.Zone, "DNS zone containing the challenge")
	cmd.Flags().IntVar(&opts.TTL, "ttl", opts.TTL, "TXT record TTL in seconds")
	cmd.Flags().DurationVar(&opts.ExpiresIn, "expires-in", opts.ExpiresIn, "Challenge lifetime")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait until healthy DNS nodes answer the TXT record")
	cmd.Flags().DurationVar(&opts.WaitTimeout, "wait-timeout", opts.WaitTimeout, "Maximum time to wait for DNS visibility")
	cmd.Flags().DurationVar(&opts.WaitInterval, "wait-interval", opts.WaitInterval, "Interval between DNS visibility probes")
	return cmd
}

func (c *CLI) newAdminDNSACMECleanupCommand() *cobra.Command {
	opts := struct {
		Zone  string
		Name  string
		Value string
	}{
		Zone: "fugue.pro",
	}
	cmd := &cobra.Command{
		Use:   "cleanup [challenge-id]",
		Short: "Delete a DNS-01 ACME TXT challenge",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if len(args) == 1 {
				response, err := client.DeleteDNSACMEChallenge(args[0])
				if err != nil {
					return err
				}
				if c.wantsJSON() {
					return writeJSON(c.stdout, response)
				}
				return writeDNSACMEChallenge(c.stdout, response.Challenge)
			}
			if strings.TrimSpace(opts.Name) == "" || strings.TrimSpace(opts.Value) == "" {
				return fmt.Errorf("pass a challenge id, or pass both --name and --value")
			}
			response, err := client.ListDNSACMEChallenges(opts.Zone, true)
			if err != nil {
				return err
			}
			var deleted []model.DNSACMEChallenge
			for _, challenge := range response.Challenges {
				if sameDNSName(challenge.Name, opts.Name) && strings.TrimSpace(challenge.Value) == strings.TrimSpace(opts.Value) {
					deletedResponse, err := client.DeleteDNSACMEChallenge(challenge.ID)
					if err != nil {
						return err
					}
					deleted = append(deleted, deletedResponse.Challenge)
				}
			}
			if len(deleted) == 0 {
				return fmt.Errorf("no matching ACME challenge found")
			}
			if c.wantsJSON() {
				return writeJSON(c.stdout, map[string]any{"deleted": true, "challenges": deleted})
			}
			return writeDNSACMEChallengeTable(c.stdout, deleted)
		},
	}
	cmd.Flags().StringVar(&opts.Zone, "zone", opts.Zone, "DNS zone containing the challenge")
	cmd.Flags().StringVar(&opts.Name, "name", "", "Challenge hostname to match when no id is provided")
	cmd.Flags().StringVar(&opts.Value, "value", "", "TXT value to match when no id is provided")
	return cmd
}

func writeDNSACMEChallengeTable(w io.Writer, challenges []model.DNSACMEChallenge) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "ID\tZONE\tNAME\tTTL\tEXPIRES_AT\tUPDATED_AT"); err != nil {
		return err
	}
	for _, challenge := range challenges {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\n",
			challenge.ID,
			challenge.Zone,
			challenge.Name,
			challenge.TTL,
			formatTime(challenge.ExpiresAt),
			formatTime(challenge.UpdatedAt),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDNSACMEChallenge(w io.Writer, challenge model.DNSACMEChallenge) error {
	return writeKeyValues(w,
		kvPair{Key: "id", Value: challenge.ID},
		kvPair{Key: "zone", Value: challenge.Zone},
		kvPair{Key: "name", Value: challenge.Name},
		kvPair{Key: "ttl", Value: fmt.Sprintf("%d", challenge.TTL)},
		kvPair{Key: "expires_at", Value: formatTime(challenge.ExpiresAt)},
		kvPair{Key: "updated_at", Value: formatTime(challenge.UpdatedAt)},
	)
}

func sameDNSName(left, right string) bool {
	return strings.EqualFold(strings.TrimSuffix(strings.TrimSpace(left), "."), strings.TrimSuffix(strings.TrimSpace(right), "."))
}

func waitForDNSACMEChallenge(client *Client, zone, name, value string, timeout, interval time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("--wait-timeout must be positive")
	}
	if interval <= 0 {
		interval = time.Second
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		nodes, err := client.ListDNSNodes("")
		if err != nil {
			lastErr = err
		} else {
			endpoints := dnsACMEChallengeEndpoints(nodes.Nodes, zone)
			if len(endpoints) == 0 {
				lastErr = fmt.Errorf("no healthy DNS node with a public endpoint for zone %s", zone)
			} else {
				missing := []string{}
				for _, endpoint := range endpoints {
					visible, err := queryDNSACMETXT(endpoint, name, value)
					if err != nil {
						lastErr = err
						missing = append(missing, endpoint)
						continue
					}
					if !visible {
						missing = append(missing, endpoint)
					}
				}
				if len(missing) == 0 {
					return nil
				}
				lastErr = fmt.Errorf("ACME TXT not visible on DNS nodes: %s", strings.Join(missing, ","))
			}
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return fmt.Errorf("wait for ACME TXT visibility: %w", lastErr)
			}
			return fmt.Errorf("wait for ACME TXT visibility timed out")
		}
		time.Sleep(interval)
	}
}

func dnsACMEChallengeEndpoints(nodes []model.DNSNode, zone string) []string {
	zone = normalizeCLIName(zone)
	seen := map[string]struct{}{}
	endpoints := []string{}
	for _, node := range nodes {
		if !node.Healthy {
			continue
		}
		if nodeZone := normalizeCLIName(node.Zone); zone != "" && nodeZone != "" && nodeZone != zone {
			continue
		}
		nodeEndpoints := []string{}
		if strings.TrimSpace(node.PublicIPv4) != "" {
			nodeEndpoints = append(nodeEndpoints, node.PublicIPv4)
		}
		if strings.TrimSpace(node.PublicIPv6) != "" {
			nodeEndpoints = append(nodeEndpoints, node.PublicIPv6)
		}
		if len(nodeEndpoints) == 0 && strings.TrimSpace(node.PublicHostname) != "" {
			nodeEndpoints = append(nodeEndpoints, node.PublicHostname)
		}
		for _, endpoint := range nodeEndpoints {
			endpoint = strings.TrimSpace(endpoint)
			if endpoint == "" {
				continue
			}
			if _, ok := seen[endpoint]; ok {
				continue
			}
			seen[endpoint] = struct{}{}
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func queryDNSACMETXT(endpoint, name, value string) (bool, error) {
	address := dnsACMEEndpointAddress(endpoint)
	msg := new(miekgdns.Msg)
	msg.SetQuestion(miekgdns.Fqdn(normalizeCLIName(name)), miekgdns.TypeTXT)
	msg.RecursionDesired = false
	// Use TCP so local DNS proxies that intercept UDP/53 cannot hide the
	// authoritative answer from the target fugue-dns node.
	client := &miekgdns.Client{Net: "tcp", Timeout: 2 * time.Second}
	response, _, err := client.Exchange(msg, address)
	if err != nil {
		return false, err
	}
	if response == nil {
		return false, fmt.Errorf("empty DNS response from %s", endpoint)
	}
	if response.Rcode != miekgdns.RcodeSuccess {
		return false, fmt.Errorf("DNS response from %s returned rcode=%s", endpoint, miekgdns.RcodeToString[response.Rcode])
	}
	wantName := miekgdns.Fqdn(normalizeCLIName(name))
	for _, answer := range response.Answer {
		txt, ok := answer.(*miekgdns.TXT)
		if !ok || !strings.EqualFold(txt.Hdr.Name, wantName) {
			continue
		}
		if strings.Join(txt.Txt, "") == strings.TrimSpace(value) {
			return true, nil
		}
	}
	return false, nil
}

func dnsACMEEndpointAddress(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if _, _, err := net.SplitHostPort(endpoint); err == nil {
		return endpoint
	}
	return net.JoinHostPort(endpoint, "53")
}

func normalizeCLIName(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), ".")
}
