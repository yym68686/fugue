package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

func (c *CLI) newAppNetworkCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "network",
		Short: "Inspect and update app network behavior",
		Long: strings.TrimSpace(`
Network mode controls whether the app is public, internal, or background-only.
Network policy controls explicit ingress and egress restrictions.
`),
	}
	cmd.AddCommand(
		c.newAppNetworkShowCommand(),
		c.newAppNetworkSetCommand(),
		c.newAppNetworkClearCommand(),
	)
	return cmd
}

func (c *CLI) newAppNetworkShowCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "show <app>",
		Aliases: []string{"get", "status"},
		Short:   "Show the app network mode and policy",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			app, err = client.GetApp(app.ID)
			if err != nil {
				return err
			}
			return c.renderAppNetworkState(app, nil, false)
		},
	}
}

func (c *CLI) newAppNetworkSetCommand() *cobra.Command {
	opts := struct {
		Mode             string
		EgressDNS        string
		EgressPublic     string
		EgressAllowApps  []string
		IngressDNS       string
		IngressPublic    string
		IngressAllowApps []string
		ClearPolicy      bool
		Wait             bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "set <app>",
		Short: "Set app network mode and/or policy",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !flagChanged(cmd, "mode") &&
				!flagChanged(cmd, "egress-dns") &&
				!flagChanged(cmd, "egress-public") &&
				len(opts.EgressAllowApps) == 0 &&
				!flagChanged(cmd, "ingress-dns") &&
				!flagChanged(cmd, "ingress-public") &&
				len(opts.IngressAllowApps) == 0 &&
				!opts.ClearPolicy {
				return fmt.Errorf("at least one network flag must be provided")
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveNamedApp(client, args[0])
			if err != nil {
				return err
			}
			egressPeers, err := c.resolveAppNetworkPeers(client, opts.EgressAllowApps)
			if err != nil {
				return err
			}
			ingressPeers, err := c.resolveAppNetworkPeers(client, opts.IngressAllowApps)
			if err != nil {
				return err
			}
			response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(spec *model.AppSpec) error {
				if flagChanged(cmd, "mode") {
					mode, err := parseAppNetworkMode(opts.Mode)
					if err != nil {
						return err
					}
					spec.NetworkMode = mode
				}
				if opts.ClearPolicy {
					spec.NetworkPolicy = nil
					return nil
				}
				policy := cloneAppNetworkPolicySpec(spec.NetworkPolicy)
				if policy == nil {
					policy = &model.AppNetworkPolicySpec{}
				}
				if flagChanged(cmd, "egress-dns") || flagChanged(cmd, "egress-public") || len(opts.EgressAllowApps) > 0 {
					direction, err := updateAppNetworkPolicyDirection(
						policy.Egress,
						opts.EgressDNS,
						flagChanged(cmd, "egress-dns"),
						opts.EgressPublic,
						flagChanged(cmd, "egress-public"),
						egressPeers,
					)
					if err != nil {
						return err
					}
					policy.Egress = direction
				}
				if flagChanged(cmd, "ingress-dns") || flagChanged(cmd, "ingress-public") || len(opts.IngressAllowApps) > 0 {
					direction, err := updateAppNetworkPolicyDirection(
						policy.Ingress,
						opts.IngressDNS,
						flagChanged(cmd, "ingress-dns"),
						opts.IngressPublic,
						flagChanged(cmd, "ingress-public"),
						ingressPeers,
					)
					if err != nil {
						return err
					}
					policy.Ingress = direction
				}
				if policy.Egress == nil && policy.Ingress == nil {
					spec.NetworkPolicy = nil
				} else {
					spec.NetworkPolicy = policy
				}
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderAppNetworkState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().StringVar(&opts.Mode, "mode", "", "Network mode: public, internal, or background")
	cmd.Flags().StringVar(&opts.EgressDNS, "egress-dns", "", "Allow egress DNS: on or off")
	cmd.Flags().StringVar(&opts.EgressPublic, "egress-public", "", "Allow egress public internet: on or off")
	cmd.Flags().StringArrayVar(&opts.EgressAllowApps, "egress-allow-app", nil, "Allow egress to an app peer: app[:port,port] (repeatable)")
	cmd.Flags().StringVar(&opts.IngressDNS, "ingress-dns", "", "Allow ingress DNS: on or off")
	cmd.Flags().StringVar(&opts.IngressPublic, "ingress-public", "", "Allow ingress public internet: on or off")
	cmd.Flags().StringArrayVar(&opts.IngressAllowApps, "ingress-allow-app", nil, "Allow ingress from an app peer: app[:port,port] (repeatable)")
	cmd.Flags().BoolVar(&opts.ClearPolicy, "clear-policy", false, "Remove the explicit network policy")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) newAppNetworkClearCommand() *cobra.Command {
	opts := struct {
		Wait bool
	}{Wait: true}
	cmd := &cobra.Command{
		Use:   "clear <app>",
		Short: "Clear the app network mode and explicit policy",
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
			response, alreadyCurrent, err := deployUpdatedAppSpec(client, app.ID, func(spec *model.AppSpec) error {
				spec.NetworkMode = ""
				spec.NetworkPolicy = nil
				return nil
			})
			if err != nil {
				return err
			}
			response, err = c.waitForAppSpecMutation(client, app.ID, response, opts.Wait)
			if err != nil {
				return err
			}
			return c.renderAppNetworkState(response.App, response.Operation, alreadyCurrent)
		},
	}
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait for the deploy operation to complete")
	return cmd
}

func (c *CLI) renderAppNetworkState(app model.App, operation *model.Operation, alreadyCurrent bool) error {
	mode := strings.TrimSpace(app.Spec.NetworkMode)
	if mode == "" {
		mode = "public"
	}
	if c.wantsJSON() {
		return writeJSON(c.stdout, map[string]any{
			"app":             app,
			"network_mode":    mode,
			"network_policy":  app.Spec.NetworkPolicy,
			"operation":       operation,
			"already_current": alreadyCurrent,
		})
	}
	pairs := []kvPair{
		{Key: "app", Value: formatDisplayName(app.Name, app.ID, c.showIDs())},
		{Key: "network_mode", Value: mode},
	}
	if operation != nil {
		pairs = append(pairs, kvPair{Key: "operation_id", Value: operation.ID})
	}
	if alreadyCurrent {
		pairs = append(pairs, kvPair{Key: "already_current", Value: "true"})
	}
	if err := writeKeyValues(c.stdout, pairs...); err != nil {
		return err
	}
	if app.Spec.NetworkPolicy == nil {
		return nil
	}
	if _, err := fmt.Fprintln(c.stdout); err != nil {
		return err
	}
	return writeAppNetworkPolicy(c.stdout, *app.Spec.NetworkPolicy)
}

func writeAppNetworkPolicy(w io.Writer, policy model.AppNetworkPolicySpec) error {
	if err := writeAppNetworkDirection(w, "egress", policy.Egress); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return writeAppNetworkDirection(w, "ingress", policy.Ingress)
}

func writeAppNetworkDirection(w io.Writer, label string, direction *model.AppNetworkPolicyDirectionSpec) error {
	if direction == nil {
		return writeKeyValues(w, kvPair{Key: label + "_mode", Value: "off"})
	}
	peerStrings := make([]string, 0, len(direction.AllowApps))
	for _, peer := range direction.AllowApps {
		peerStrings = append(peerStrings, formatAppNetworkPeer(peer))
	}
	return writeKeyValues(w,
		kvPair{Key: label + "_mode", Value: firstNonEmpty(direction.Mode, "restricted")},
		kvPair{Key: label + "_allow_dns", Value: fmt.Sprintf("%t", direction.AllowDNS)},
		kvPair{Key: label + "_allow_public_internet", Value: fmt.Sprintf("%t", direction.AllowPublicInternet)},
		kvPair{Key: label + "_allow_apps", Value: strings.Join(peerStrings, "; ")},
	)
}

func updateAppNetworkPolicyDirection(existing *model.AppNetworkPolicyDirectionSpec, dnsRaw string, dnsChanged bool, publicRaw string, publicChanged bool, allowApps []model.AppNetworkPolicyAppPeer) (*model.AppNetworkPolicyDirectionSpec, error) {
	direction := &model.AppNetworkPolicyDirectionSpec{Mode: model.AppNetworkPolicyModeRestricted}
	if existing != nil {
		direction = &model.AppNetworkPolicyDirectionSpec{
			Mode:                firstNonEmpty(existing.Mode, model.AppNetworkPolicyModeRestricted),
			AllowDNS:            existing.AllowDNS,
			AllowPublicInternet: existing.AllowPublicInternet,
			AllowApps:           append([]model.AppNetworkPolicyAppPeer(nil), existing.AllowApps...),
		}
	}
	if dnsChanged {
		value, err := parseOptionalSemanticBool(dnsRaw)
		if err != nil {
			return nil, fmt.Errorf("egress/ingress DNS: %w", err)
		}
		if value == nil {
			return nil, fmt.Errorf("egress/ingress DNS requires on or off")
		}
		direction.AllowDNS = *value
	}
	if publicChanged {
		value, err := parseOptionalSemanticBool(publicRaw)
		if err != nil {
			return nil, fmt.Errorf("egress/ingress public internet: %w", err)
		}
		if value == nil {
			return nil, fmt.Errorf("egress/ingress public internet requires on or off")
		}
		direction.AllowPublicInternet = *value
	}
	if len(allowApps) > 0 {
		direction.AllowApps = append([]model.AppNetworkPolicyAppPeer(nil), allowApps...)
	}
	if direction.Mode == "" {
		direction.Mode = model.AppNetworkPolicyModeRestricted
	}
	if model.NormalizeAppNetworkPolicyMode(direction.Mode) == "" {
		return nil, fmt.Errorf("network policy mode must be restricted")
	}
	return direction, nil
}

func parseAppNetworkMode(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "public":
		return "", nil
	case model.AppNetworkModeBackground:
		return model.AppNetworkModeBackground, nil
	case model.AppNetworkModeInternal:
		return model.AppNetworkModeInternal, nil
	default:
		return "", fmt.Errorf("network mode must be public, internal, or background")
	}
}

func (c *CLI) resolveAppNetworkPeers(client *Client, raw []string) ([]model.AppNetworkPolicyAppPeer, error) {
	peers, err := parseAppNetworkPeers(raw)
	if err != nil {
		return nil, err
	}
	if len(peers) == 0 {
		return nil, nil
	}
	tenantID, projectID, err := c.resolveFilterSelections(client)
	if err != nil {
		return nil, err
	}
	apps, err := client.ListApps()
	if err != nil {
		return nil, err
	}
	out := make([]model.AppNetworkPolicyAppPeer, 0, len(peers))
	for _, peer := range peers {
		ref := strings.TrimSpace(peer.AppID)
		matches := matchVisibleApps(apps, ref, projectID, tenantID)
		switch len(matches) {
		case 0:
			return nil, fmt.Errorf("network peer app %q not found", ref)
		case 1:
			peer.AppID = matches[0].ID
		default:
			return nil, multipleMatchesError("app", ref, matches, describeAppMatch)
		}
		out = append(out, peer)
	}
	return out, nil
}

func parseAppNetworkPeers(raw []string) ([]model.AppNetworkPolicyAppPeer, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	peers := make([]model.AppNetworkPolicyAppPeer, 0, len(raw))
	for _, spec := range raw {
		value := strings.TrimSpace(spec)
		if value == "" {
			continue
		}
		peer := model.AppNetworkPolicyAppPeer{}
		name, portsRaw, hasPorts := strings.Cut(value, ":")
		peer.AppID = strings.TrimSpace(name)
		if peer.AppID == "" {
			return nil, fmt.Errorf("network peer %q is missing an app id", spec)
		}
		if hasPorts && strings.TrimSpace(portsRaw) != "" {
			for _, rawPort := range strings.Split(portsRaw, ",") {
				port, err := strconv.Atoi(strings.TrimSpace(rawPort))
				if err != nil || port <= 0 || port > 65535 {
					return nil, fmt.Errorf("network peer %q has invalid port %q", spec, rawPort)
				}
				peer.Ports = append(peer.Ports, port)
			}
		}
		peers = append(peers, peer)
	}
	return peers, nil
}

func formatAppNetworkPeer(peer model.AppNetworkPolicyAppPeer) string {
	if len(peer.Ports) == 0 {
		return peer.AppID
	}
	portStrings := make([]string, len(peer.Ports))
	for i, port := range peer.Ports {
		portStrings[i] = strconv.Itoa(port)
	}
	return peer.AppID + ":" + strings.Join(portStrings, ",")
}
