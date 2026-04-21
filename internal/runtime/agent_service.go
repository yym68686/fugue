package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

type AgentService struct {
	Config     config.AgentConfig
	HTTPClient *http.Client
	Renderer   Renderer
	Logger     *log.Logger
}

type agentState struct {
	RuntimeID  string `json:"runtime_id"`
	RuntimeKey string `json:"runtime_key"`
}

func NewAgentService(cfg config.AgentConfig, logger *log.Logger) *AgentService {
	return &AgentService{
		Config: cfg,
		HTTPClient: &http.Client{
			Timeout: 20 * time.Second,
		},
		Renderer: Renderer{BaseDir: cfg.WorkDir},
		Logger:   logger,
	}
}

func (s *AgentService) Run(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = log.Default()
	}
	if err := os.MkdirAll(filepath.Dir(s.Config.StateFile), 0o755); err != nil {
		return fmt.Errorf("create agent state directory: %w", err)
	}
	if err := os.MkdirAll(s.Config.WorkDir, 0o755); err != nil {
		return fmt.Errorf("create agent work directory: %w", err)
	}

	state, err := s.loadState()
	if err != nil {
		return err
	}
	if state.RuntimeID != "" && s.Config.RuntimeID == "" {
		s.Config.RuntimeID = state.RuntimeID
	}
	if state.RuntimeKey != "" && s.Config.RuntimeKey == "" {
		s.Config.RuntimeKey = state.RuntimeKey
	}

	if s.Config.RuntimeKey == "" {
		if err := s.bootstrapOrEnroll(); err != nil {
			return err
		}
	}

	s.Logger.Printf("agent started; runtime_id=%s server=%s kubectl_apply=%v", s.Config.RuntimeID, s.Config.ServerURL, s.Config.ApplyWithKubectl)

	heartbeatTicker := time.NewTicker(s.Config.HeartbeatEvery)
	defer heartbeatTicker.Stop()
	pollTicker := time.NewTicker(s.Config.PollInterval)
	defer pollTicker.Stop()

	if err := s.sendHeartbeat(ctx); err != nil {
		s.Logger.Printf("initial heartbeat failed: %v", err)
	}
	if err := s.pollAndProcess(ctx); err != nil {
		s.Logger.Printf("initial poll failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			if err := s.sendHeartbeat(ctx); err != nil {
				s.Logger.Printf("heartbeat failed: %v", err)
			}
		case <-pollTicker.C:
			if err := s.pollAndProcess(ctx); err != nil {
				s.Logger.Printf("poll failed: %v", err)
			}
		}
	}
}

func (s *AgentService) bootstrapOrEnroll() error {
	if s.Config.NodeKey != "" {
		return s.bootstrapNode()
	}
	return s.enroll()
}

func (s *AgentService) bootstrapNode() error {
	reqBody := map[string]any{
		"node_key":            s.Config.NodeKey,
		"node_name":           s.Config.RuntimeName,
		"machine_name":        s.Config.MachineName,
		"machine_fingerprint": s.Config.MachineFingerprint,
		"endpoint":            s.Config.RuntimeEndpoint,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal node bootstrap request: %w", err)
	}

	respBody, err := s.doJSONRequest(context.Background(), http.MethodPost, "/v1/nodes/bootstrap", "", payload)
	if err != nil {
		return err
	}

	var response struct {
		Node       model.Runtime `json:"node"`
		RuntimeKey string        `json:"runtime_key"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return fmt.Errorf("decode node bootstrap response: %w", err)
	}
	s.Config.RuntimeID = response.Node.ID
	s.Config.RuntimeKey = response.RuntimeKey
	return s.persistState()
}

func (s *AgentService) enroll() error {
	if s.Config.EnrollToken == "" {
		return fmt.Errorf("runtime key is empty and neither node key nor enroll token was provided")
	}

	reqBody := map[string]any{
		"enroll_token":        s.Config.EnrollToken,
		"runtime_name":        s.Config.RuntimeName,
		"machine_name":        s.Config.MachineName,
		"machine_fingerprint": s.Config.MachineFingerprint,
		"endpoint":            s.Config.RuntimeEndpoint,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal enroll request: %w", err)
	}

	respBody, err := s.doJSONRequest(context.Background(), http.MethodPost, "/v1/agent/enroll", "", payload)
	if err != nil {
		return err
	}

	var response struct {
		Runtime    model.Runtime `json:"runtime"`
		RuntimeKey string        `json:"runtime_key"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return fmt.Errorf("decode enroll response: %w", err)
	}
	s.Config.RuntimeID = response.Runtime.ID
	s.Config.RuntimeKey = response.RuntimeKey
	return s.persistState()
}

func (s *AgentService) sendHeartbeat(ctx context.Context) error {
	reqBody := map[string]any{
		"endpoint": s.Config.RuntimeEndpoint,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal heartbeat request: %w", err)
	}
	_, err = s.doJSONRequest(ctx, http.MethodPost, "/v1/agent/heartbeat", s.Config.RuntimeKey, payload)
	return err
}

func (s *AgentService) pollAndProcess(ctx context.Context) error {
	respBody, err := s.doJSONRequest(ctx, http.MethodGet, "/v1/agent/operations", s.Config.RuntimeKey, nil)
	if err != nil {
		return err
	}

	var response struct {
		Tasks []AgentTask `json:"tasks"`
	}
	if err := json.Unmarshal(respBody, &response); err != nil {
		return fmt.Errorf("decode operations response: %w", err)
	}
	for _, task := range response.Tasks {
		if err := s.processTask(ctx, task); err != nil {
			s.Logger.Printf("task %s failed locally: %v", task.Operation.ID, err)
		}
	}
	return nil
}

func (s *AgentService) processTask(ctx context.Context, task AgentTask) error {
	app := task.App
	switch task.Operation.Type {
	case model.OperationTypeDeploy:
		if task.Operation.DesiredSpec == nil {
			return fmt.Errorf("deploy task missing desired spec")
		}
		app.Spec = *task.Operation.DesiredSpec
	case model.OperationTypeScale:
		if task.Operation.DesiredReplicas == nil {
			return fmt.Errorf("scale task missing desired replicas")
		}
		app.Spec.Replicas = *task.Operation.DesiredReplicas
	case model.OperationTypeDelete:
	case model.OperationTypeMigrate:
		if task.Operation.DesiredSpec != nil {
			app.Spec = *task.Operation.DesiredSpec
		} else {
			if task.Operation.TargetRuntimeID == "" {
				return fmt.Errorf("migrate task missing target runtime")
			}
			app.Spec.RuntimeID = task.Operation.TargetRuntimeID
		}
		buildSource := model.AppBuildSource(app)
		if task.Operation.DesiredSource != nil {
			buildSource = model.CloneAppSource(task.Operation.DesiredSource)
		}
		originSource := model.AppOriginSource(app)
		if task.Operation.DesiredOriginSource != nil {
			originSource = model.CloneAppSource(task.Operation.DesiredOriginSource)
		}
		model.SetAppSourceState(&app, originSource, buildSource)
	default:
		return fmt.Errorf("unsupported task type %s", task.Operation.Type)
	}

	bundle, err := s.Renderer.RenderAppBundle(app)
	if err != nil {
		return fmt.Errorf("render bundle: %w", err)
	}
	if s.Config.ApplyWithKubectl {
		switch task.Operation.Type {
		case model.OperationTypeDelete:
			if err := DeleteKubectl(bundle.ManifestPath); err != nil {
				return fmt.Errorf("kubectl delete: %w", err)
			}
		default:
			if err := ApplyKubectl(bundle.ManifestPath); err != nil {
				return fmt.Errorf("kubectl apply: %w", err)
			}
		}
	}

	message := fmt.Sprintf("external runtime applied in namespace %s", bundle.TenantNamespace)
	if task.Operation.Type == model.OperationTypeDelete {
		message = fmt.Sprintf("external runtime deleted app resources in namespace %s", bundle.TenantNamespace)
	}
	reqBody := map[string]any{
		"manifest_path": bundle.ManifestPath,
		"message":       message,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal completion request: %w", err)
	}
	_, err = s.doJSONRequest(ctx, http.MethodPost, "/v1/agent/operations/"+task.Operation.ID+"/complete", s.Config.RuntimeKey, payload)
	return err
}

func (s *AgentService) doJSONRequest(ctx context.Context, method, path, bearer string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.Config.ServerURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build %s %s request: %w", method, path, err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}

	resp, err := s.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s %s response: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: status=%d body=%s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func (s *AgentService) loadState() (agentState, error) {
	data, err := os.ReadFile(s.Config.StateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return agentState{}, nil
		}
		return agentState{}, fmt.Errorf("read agent state: %w", err)
	}
	var state agentState
	if err := json.Unmarshal(data, &state); err != nil {
		return agentState{}, fmt.Errorf("decode agent state: %w", err)
	}
	return state, nil
}

func (s *AgentService) persistState() error {
	state := agentState{
		RuntimeID:  s.Config.RuntimeID,
		RuntimeKey: s.Config.RuntimeKey,
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal agent state: %w", err)
	}
	if err := os.WriteFile(s.Config.StateFile, data, 0o600); err != nil {
		return fmt.Errorf("write agent state: %w", err)
	}
	return nil
}
