package cli

import (
	"fmt"
	"strings"
	"time"

	"fugue/internal/edgequality"
	"github.com/spf13/cobra"
)

func (c *CLI) newAdminEdgeQualityProbeCommand() *cobra.Command {
	var targets []string
	var vantage, path string
	var rounds int
	var interval, timeout time.Duration
	command := &cobra.Command{Use: "quality-probe <hostname>", Short: "Measure observer-to-edge TCP with TLS nonce proofs; never changes DNS or invokes the application", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			parsed := []edgequality.ClientProbeTarget{}
			for _, target := range targets {
				edgeID, address, present := strings.Cut(target, "=")
				if !present {
					return fmt.Errorf("--target must be physical-edge-id=public-IP")
				}
				parsed = append(parsed, edgequality.ClientProbeTarget{EdgeID: edgeID, Address: address})
			}
			capture, err := edgequality.CaptureClientPath(command.Context(), args[0], path, vantage, parsed, rounds, interval, timeout)
			if err != nil && capture.Schema == "" {
				return err
			}
			if writeErr := c.writeJSON(capture); writeErr != nil {
				return writeErr
			}
			return err
		}}
	command.Flags().StringArrayVar(&targets, "target", nil, "Physical edge ID and public IP, repeat for up to eight distinct edges")
	command.Flags().StringVar(&vantage, "vantage", "", "Operator label for this observer; does not assert country, ASN or terminal identity")
	command.Flags().StringVar(&path, "path", "/", "Route proof path; HEAD is intercepted by the edge without contacting the application")
	command.Flags().IntVar(&rounds, "rounds", 3, "Bounded sample rounds (1-3)")
	command.Flags().DurationVar(&interval, "interval", time.Minute, "Delay between rounds (30s-5m)")
	command.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "Deadline for each TLS nonce HEAD (1s-5s)")
	_ = command.MarkFlagRequired("target")
	_ = command.MarkFlagRequired("vantage")
	return command
}
