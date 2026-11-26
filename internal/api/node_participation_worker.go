package api

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// StartBackgroundNodeParticipation executes the same UID-bound, lease-fenced
// reconciliation as the Actions fallback. No production credentials are sent
// to the external policy origin: this installation queries its local API.
func (s *Server) StartBackgroundNodeParticipation(ctx context.Context, policyFile, bindAddr, observationToken string) {
	if s == nil || s.store == nil || policyFile == "" {
		return
	}
	raw, err := os.ReadFile(policyFile)
	var policy struct {
		Schema, Namespace string
		Members           []json.RawMessage
	}
	origin := participationLoopbackOrigin(bindAddr)
	if err != nil || len(raw) > 128<<10 || json.Unmarshal(raw, &policy) != nil ||
		policy.Schema != "fugue.automatic-node-participation/v1" || policy.Namespace != s.controlPlaneNamespace ||
		len(policy.Members) == 0 || len(policy.Members) > 16 || observationToken == "" || origin == "" {
		if s.log != nil {
			s.log.Print("automatic node participation policy or local observation unavailable")
		}
		return
	}
	previous := make([]string, len(policy.Members))
	for ctx.Err() == nil {
		for member := range policy.Members {
			if ctx.Err() != nil {
				return
			}
			deadline, cancel := context.WithTimeout(ctx, 4*time.Minute)
			_, _ = s.store.WithAdvisoryLock(deadline, "fugue|automatic-node-participation", func() error {
				cmd := exec.CommandContext(deadline, "python3", "-B", "-m", "scripts.reconcile_node_participation",
					policyFile, "--member-index", strconv.Itoa(member), "--local-origin", origin, "--apply")
				cmd.Dir = "/app"
				cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "FUGUE_API_KEY=" + observationToken,
					"KUBERNETES_SERVICE_HOST=" + os.Getenv("KUBERNETES_SERVICE_HOST"),
					"KUBERNETES_SERVICE_PORT=" + os.Getenv("KUBERNETES_SERVICE_PORT")}
				out, err := cmd.Output()
				summary := "no bounded reconciliation result"
				if err == nil && len(out) <= 4096 && json.Valid(out) {
					summary = string(out)
				}
				if summary != previous[member] {
					previous[member] = summary
					if s.log != nil {
						s.log.Printf("automatic node participation: %s", summary)
					}
				}
				if err != nil && deadline.Err() == nil {
					return errors.New("automatic node participation child failed")
				}
				return nil
			})
			cancel()
		}
		if !waitPlatformProducer(ctx, 2*time.Minute) {
			return
		}
	}
}

func participationLoopbackOrigin(bind string) string {
	host, port, err := net.SplitHostPort(bind)
	p, invalid := strconv.Atoi(port)
	if err != nil || invalid != nil || p < 1 || p > 65535 {
		return ""
	}
	switch host {
	case "", "0.0.0.0", "127.0.0.1":
		host = "127.0.0.1"
	case "::", "::1":
		host = "::1"
	default:
		return ""
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(p))
}
