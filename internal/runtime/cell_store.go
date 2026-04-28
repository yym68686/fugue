package runtime

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

const (
	cellStoreVersion = 1

	CellOutboxEventAgentCompletion = "agent.operation.complete"

	cellDesiredTaskStatusCached     = "cached"
	cellDesiredTaskStatusApplied    = "applied"
	cellDesiredTaskStatusDelivered  = "delivered"
	cellDesiredTaskStatusDiscarded  = "discarded"
	defaultCompletionReplayLimit    = 16
	defaultCompletionReplayBaseBack = 5 * time.Second
	defaultCompletionReplayMaxBack  = 5 * time.Minute
)

// CellStore is the local survival substrate for fugue-agent. It stores the
// last control-plane tasks that reached this cell plus outbound events that
// must survive control-plane outages.
type CellStore struct {
	path string
	now  func() time.Time

	mu    sync.Mutex
	state cellStoreState
}

type cellStoreState struct {
	Version          int                            `json:"version"`
	Desired          map[string]CellDesiredTask     `json:"desired_tasks"`
	Routes           map[string]CellRoute           `json:"routes"`
	Outbox           []CellOutboxEvent              `json:"outbox"`
	Snapshot         *CellSnapshot                  `json:"snapshot,omitempty"`
	PeerObservations map[string]CellPeerObservation `json:"peer_observations,omitempty"`
	NextOutbox       int64                          `json:"next_outbox_id"`
	LastUpdated      time.Time                      `json:"last_updated_at"`
	Metadata         map[string]string              `json:"metadata,omitempty"`
}

type CellDesiredTask struct {
	OperationID   string    `json:"operation_id"`
	AppID         string    `json:"app_id"`
	TenantID      string    `json:"tenant_id,omitempty"`
	RuntimeID     string    `json:"runtime_id,omitempty"`
	OperationType string    `json:"operation_type"`
	Status        string    `json:"status"`
	ManifestPath  string    `json:"manifest_path,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type CellOutboxEvent struct {
	ID            int64     `json:"id"`
	EventType     string    `json:"event_type"`
	OperationID   string    `json:"operation_id"`
	ManifestPath  string    `json:"manifest_path,omitempty"`
	Message       string    `json:"message,omitempty"`
	Attempts      int       `json:"attempts"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastError     string    `json:"last_error,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func OpenCellStore(path string) (*CellStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("cell store path is empty")
	}
	store := &CellStore{
		path: path,
		now:  func() time.Time { return time.Now().UTC() },
		state: cellStoreState{
			Version:          cellStoreVersion,
			Desired:          map[string]CellDesiredTask{},
			Routes:           map[string]CellRoute{},
			Outbox:           []CellOutboxEvent{},
			PeerObservations: map[string]CellPeerObservation{},
			NextOutbox:       1,
			Metadata:         map[string]string{},
		},
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *CellStore) RecordDesiredTask(task AgentTask) error {
	if s == nil {
		return nil
	}
	operationID := strings.TrimSpace(task.Operation.ID)
	if operationID == "" {
		return fmt.Errorf("cell desired task missing operation id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()

	now := s.now().UTC()
	record := s.state.Desired[operationID]
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	record.OperationID = operationID
	record.AppID = strings.TrimSpace(task.Operation.AppID)
	record.TenantID = strings.TrimSpace(task.Operation.TenantID)
	record.RuntimeID = strings.TrimSpace(firstNonEmpty(task.Operation.AssignedRuntimeID, task.Operation.TargetRuntimeID, task.App.Spec.RuntimeID))
	record.OperationType = strings.TrimSpace(task.Operation.Type)
	record.Status = cellDesiredTaskStatusCached
	record.UpdatedAt = now
	s.state.Desired[operationID] = record
	s.updateRouteCacheForTaskLocked(task, now)
	return s.persistLocked()
}

func (s *CellStore) MarkDesiredTaskApplied(operationID, manifestPath string) error {
	return s.markDesiredTask(operationID, cellDesiredTaskStatusApplied, manifestPath)
}

func (s *CellStore) MarkDesiredTaskDelivered(operationID string) error {
	return s.markDesiredTask(operationID, cellDesiredTaskStatusDelivered, "")
}

func (s *CellStore) MarkDesiredTaskDiscarded(operationID string) error {
	return s.markDesiredTask(operationID, cellDesiredTaskStatusDiscarded, "")
}

func (s *CellStore) HasPendingCompletion(operationID string) (bool, error) {
	if s == nil {
		return false, nil
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.state.Outbox {
		if event.EventType == CellOutboxEventAgentCompletion && strings.TrimSpace(event.OperationID) == operationID {
			return true, nil
		}
	}
	return false, nil
}

func (s *CellStore) EnqueueCompletion(operationID, manifestPath, message string) error {
	if s == nil {
		return nil
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return fmt.Errorf("completion outbox event missing operation id")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()

	now := s.now().UTC()
	for idx := range s.state.Outbox {
		event := &s.state.Outbox[idx]
		if event.EventType == CellOutboxEventAgentCompletion && strings.TrimSpace(event.OperationID) == operationID {
			event.ManifestPath = strings.TrimSpace(manifestPath)
			event.Message = strings.TrimSpace(message)
			event.UpdatedAt = now
			if event.NextAttemptAt.IsZero() {
				event.NextAttemptAt = now
			}
			return s.persistLocked()
		}
	}

	id := s.state.NextOutbox
	if id <= 0 {
		id = 1
	}
	s.state.NextOutbox = id + 1
	s.state.Outbox = append(s.state.Outbox, CellOutboxEvent{
		ID:            id,
		EventType:     CellOutboxEventAgentCompletion,
		OperationID:   operationID,
		ManifestPath:  strings.TrimSpace(manifestPath),
		Message:       strings.TrimSpace(message),
		NextAttemptAt: now,
		CreatedAt:     now,
		UpdatedAt:     now,
	})
	return s.persistLocked()
}

func (s *CellStore) ListDueCompletions(now time.Time, limit int) ([]CellOutboxEvent, error) {
	if s == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = defaultCompletionReplayLimit
	}
	now = now.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]CellOutboxEvent, 0, len(s.state.Outbox))
	for _, event := range s.state.Outbox {
		if event.EventType != CellOutboxEventAgentCompletion {
			continue
		}
		if !event.NextAttemptAt.IsZero() && event.NextAttemptAt.After(now) {
			continue
		}
		out = append(out, event)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *CellStore) MarkOutboxDelivered(id int64) (string, error) {
	if s == nil {
		return "", nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for idx, event := range s.state.Outbox {
		if event.ID != id {
			continue
		}
		operationID := strings.TrimSpace(event.OperationID)
		s.state.Outbox = append(s.state.Outbox[:idx], s.state.Outbox[idx+1:]...)
		return operationID, s.persistLocked()
	}
	return "", nil
}

func (s *CellStore) MarkOutboxAttempt(id int64, message string) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for idx := range s.state.Outbox {
		event := &s.state.Outbox[idx]
		if event.ID != id {
			continue
		}
		event.Attempts++
		event.LastError = strings.TrimSpace(message)
		event.NextAttemptAt = now.Add(completionReplayBackoff(event.Attempts))
		event.UpdatedAt = now
		return s.persistLocked()
	}
	return nil
}

func (s *CellStore) CountPendingCompletions() (int, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, event := range s.state.Outbox {
		if event.EventType == CellOutboxEventAgentCompletion {
			count++
		}
	}
	return count, nil
}

func (s *CellStore) CountRoutes() (int, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.state.Routes), nil
}

func (s *CellStore) CountPeerObservations() (int, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, observation := range s.state.PeerObservations {
		if observation.Snapshot != nil && observation.LastError == "" {
			count++
		}
	}
	return count, nil
}

func (s *CellStore) ListRoutes() ([]CellRoute, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CellRoute, 0, len(s.state.Routes))
	for _, route := range s.state.Routes {
		out = append(out, route)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Hostname < out[j].Hostname
	})
	return out, nil
}

func (s *CellStore) ListPeerObservations() ([]CellPeerObservation, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CellPeerObservation, 0, len(s.state.PeerObservations))
	for _, observation := range s.state.PeerObservations {
		copied := observation
		if observation.Snapshot != nil {
			snapshot := *observation.Snapshot
			snapshot.Peers = append([]CellPeer(nil), observation.Snapshot.Peers...)
			copied.Snapshot = &snapshot
		}
		out = append(out, copied)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Peer.Hostname == out[j].Peer.Hostname {
			return out[i].Peer.IP < out[j].Peer.IP
		}
		return out[i].Peer.Hostname < out[j].Peer.Hostname
	})
	return out, nil
}

func (s *CellStore) SaveSnapshot(snapshot CellSnapshot) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()
	copied := snapshot
	copied.Peers = append([]CellPeer(nil), snapshot.Peers...)
	s.state.Snapshot = &copied
	return s.persistLocked()
}

func (s *CellStore) Snapshot() (CellSnapshot, bool, error) {
	if s == nil {
		return CellSnapshot{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.Snapshot == nil {
		return CellSnapshot{}, false, nil
	}
	copied := *s.state.Snapshot
	copied.Peers = append([]CellPeer(nil), s.state.Snapshot.Peers...)
	return copied, true, nil
}

func (s *CellStore) SavePeerObservation(peer CellPeer, snapshot *CellSnapshot, source, errMessage string) error {
	if s == nil {
		return nil
	}
	key := cellPeerObservationKey(peer)
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()

	now := s.now().UTC()
	observation := s.state.PeerObservations[key]
	observation.Key = key
	observation.Peer = peer
	observation.Source = strings.TrimSpace(source)
	observation.LastAttemptAt = now
	observation.LastError = strings.TrimSpace(errMessage)
	if snapshot != nil {
		copied := *snapshot
		copied.Peers = append([]CellPeer(nil), snapshot.Peers...)
		observation.Snapshot = &copied
		observation.LastSeenAt = &now
	}
	s.state.PeerObservations[key] = observation
	return s.persistLocked()
}

func (s *CellStore) markDesiredTask(operationID, status, manifestPath string) error {
	if s == nil {
		return nil
	}
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureInitializedLocked()

	record := s.state.Desired[operationID]
	if record.OperationID == "" {
		record.OperationID = operationID
		record.CreatedAt = s.now().UTC()
	}
	record.Status = strings.TrimSpace(status)
	if strings.TrimSpace(manifestPath) != "" {
		record.ManifestPath = strings.TrimSpace(manifestPath)
	}
	record.UpdatedAt = s.now().UTC()
	s.state.Desired[operationID] = record
	return s.persistLocked()
}

func (s *CellStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read cell store: %w", err)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &s.state); err != nil {
		return fmt.Errorf("decode cell store: %w", err)
	}
	s.ensureInitializedLocked()
	return nil
}

func (s *CellStore) persistLocked() error {
	s.ensureInitializedLocked()
	s.state.LastUpdated = s.now().UTC()
	data, err := json.MarshalIndent(s.state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal cell store: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create cell store directory: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write cell store temp file: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace cell store: %w", err)
	}
	return nil
}

func (s *CellStore) ensureInitializedLocked() {
	if s.state.Version <= 0 {
		s.state.Version = cellStoreVersion
	}
	if s.state.Desired == nil {
		s.state.Desired = map[string]CellDesiredTask{}
	}
	if s.state.Routes == nil {
		s.state.Routes = map[string]CellRoute{}
	}
	if s.state.Outbox == nil {
		s.state.Outbox = []CellOutboxEvent{}
	}
	if s.state.PeerObservations == nil {
		s.state.PeerObservations = map[string]CellPeerObservation{}
	}
	if s.state.NextOutbox <= 0 {
		var maxID int64
		for _, event := range s.state.Outbox {
			if event.ID > maxID {
				maxID = event.ID
			}
		}
		s.state.NextOutbox = maxID + 1
	}
	if s.state.Metadata == nil {
		s.state.Metadata = map[string]string{}
	}
}

func cellPeerObservationKey(peer CellPeer) string {
	if ip := strings.TrimSpace(peer.IP); ip != "" {
		return strings.ToLower(ip)
	}
	return strings.ToLower(strings.TrimSpace(peer.Hostname))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *CellStore) updateRouteCacheForTaskLocked(task AgentTask, now time.Time) {
	app := task.App
	operationID := strings.TrimSpace(task.Operation.ID)
	if operationID == "" {
		return
	}
	if task.Operation.Type == model.OperationTypeDelete {
		for hostname, route := range s.state.Routes {
			if strings.TrimSpace(route.AppID) == strings.TrimSpace(app.ID) {
				delete(s.state.Routes, hostname)
			}
		}
		return
	}
	if task.Operation.DesiredSpec != nil {
		app.Spec = *task.Operation.DesiredSpec
	}
	if app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "" {
		return
	}
	hostname := strings.TrimSpace(strings.ToLower(app.Route.Hostname))
	s.state.Routes[hostname] = CellRoute{
		Hostname:    hostname,
		AppID:       strings.TrimSpace(app.ID),
		TenantID:    strings.TrimSpace(app.TenantID),
		RuntimeID:   strings.TrimSpace(app.Spec.RuntimeID),
		ServicePort: app.Route.ServicePort,
		UpdatedAt:   now.UTC(),
	}
}

func completionReplayBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		return defaultCompletionReplayBaseBack
	}
	delay := defaultCompletionReplayBaseBack
	for i := 1; i < attempts && delay < defaultCompletionReplayMaxBack; i++ {
		delay *= 2
	}
	if delay > defaultCompletionReplayMaxBack {
		return defaultCompletionReplayMaxBack
	}
	return delay
}
