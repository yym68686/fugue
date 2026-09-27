package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/agentedge"
)

func (s *AgentService) startEdgeControl(ctx context.Context) error {
	if s.Config.EdgeTrustFile == "" {
		return nil
	}
	path := s.Config.EdgeCheckpointFile
	if path == "" {
		path = filepath.Join(s.Config.WorkDir, "edge-checkpoint.json")
	}
	checkpoint, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve Agent Edge checkpoint: %w", err)
	}
	control, err := agentedge.NewControl(agentedge.ControlOptions{Audience: s.Config.RuntimeID, Origin: strings.TrimRight(s.Config.ServerURL, "/"), RuntimeKey: s.Config.RuntimeKey, TrustFile: s.Config.EdgeTrustFile, CheckpointFile: checkpoint})
	if err != nil {
		return fmt.Errorf("initialize Agent Edge selection: %w", err)
	}
	s.edgeControl = control
	last := ""
	observe := func() {
		err := control.Step(ctx)
		status, _ := json.Marshal(control.Status())
		result := string(status)
		if err != nil {
			result += " error=" + err.Error()
		}
		if result != last {
			s.logf("agent_edge_selection %s", result)
			last = result
		}
	}
	observe()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				observe()
			}
		}
	}()
	return nil
}
