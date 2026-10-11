package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"syscall"
	"time"

	"fugue/internal/model"
)

func (stateStore *Store) RecordEdgeSSHAcknowledgement(ack model.EdgeSSHAcknowledgement) error {
	now := time.Now().UTC()
	if !validEdgeSSHAcknowledgement(ack) || ack.LoadedAt.After(now.Add(5*time.Second)) || ack.LoadedAt.Before(now.Add(-2*time.Minute)) || ack.ObservedAt.After(now.Add(5*time.Second)) || ack.ObservedAt.Before(now.Add(-time.Minute)) {
		return ErrInvalidInput
	}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		raw, err := json.Marshal(ack)
		if err != nil {
			return err
		}
		result, err := stateStore.db.ExecContext(ctx, `INSERT INTO fugue_edge_ssh_acknowledgements(edge_id,instance_uid,loaded_at,expires_at,body) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT(edge_id,instance_uid) DO UPDATE SET loaded_at=EXCLUDED.loaded_at,expires_at=EXCLUDED.expires_at,body=EXCLUDED.body WHERE fugue_edge_ssh_acknowledgements.loaded_at<EXCLUDED.loaded_at`, ack.EdgeID, ack.InstanceUID, ack.LoadedAt, ack.ExpiresAt, string(raw))
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return ErrConflict
		}
		return nil
	}
	return stateStore.withEdgeSSHAcknowledgements(true, func(records map[string]model.EdgeSSHAcknowledgement) error {
		key := ack.EdgeID + "\x00" + ack.InstanceUID
		if previous, exists := records[key]; exists && !ack.LoadedAt.After(previous.LoadedAt) {
			return ErrConflict
		}
		records[key] = ack
		for identifier, previous := range records {
			if previous.ExpiresAt.Before(now.Add(-24 * time.Hour)) {
				delete(records, identifier)
			}
		}
		return nil
	})
}

func (stateStore *Store) GetEdgeSSHAcknowledgement(edgeID, instanceUID string) (model.EdgeSSHAcknowledgement, error) {
	if edgeID == "" || instanceUID == "" {
		return model.EdgeSSHAcknowledgement{}, ErrInvalidInput
	}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var raw []byte
		if err := stateStore.db.QueryRowContext(ctx, `SELECT body FROM fugue_edge_ssh_acknowledgements WHERE edge_id=$1 AND instance_uid=$2`, edgeID, instanceUID).Scan(&raw); err != nil {
			return model.EdgeSSHAcknowledgement{}, mapDBErr(err)
		}
		var ack model.EdgeSSHAcknowledgement
		if err := json.Unmarshal(raw, &ack); err != nil {
			return ack, err
		}
		if !validEdgeSSHAcknowledgement(ack) || ack.EdgeID != edgeID || ack.InstanceUID != instanceUID {
			return model.EdgeSSHAcknowledgement{}, ErrConflict
		}
		return ack, nil
	}
	var ack model.EdgeSSHAcknowledgement
	err := stateStore.withEdgeSSHAcknowledgements(false, func(records map[string]model.EdgeSSHAcknowledgement) error {
		var exists bool
		ack, exists = records[edgeID+"\x00"+instanceUID]
		if !exists {
			return ErrNotFound
		}
		return nil
	})
	return ack, err
}

func validEdgeSSHAcknowledgement(ack model.EdgeSSHAcknowledgement) bool {
	for _, identifier := range []string{ack.EdgeID, ack.EdgeGroupID, ack.InstanceUID, ack.BundleVersion} {
		if identifier == "" || len(identifier) > 256 || strings.ContainsAny(identifier, "\x00\r\n\t ") {
			return false
		}
	}
	if ack.Connections == nil || len(ack.Connections) > 20000 {
		return false
	}
	seen := map[model.EdgeSSHRouteConnections]bool{}
	for _, connections := range ack.Connections {
		if connections.Count < 1 || connections.AppID == "" || len(connections.AppID) > 256 || connections.TargetHost == "" || len(connections.TargetHost) > 253 || connections.PublicPort < 1 || connections.PublicPort > 65535 || connections.TargetPort < 1 || connections.TargetPort > 65535 {
			return false
		}
		connections.Count = 0
		if seen[connections] {
			return false
		}
		seen[connections] = true
	}
	return ack.Persisted && len(ack.Routes) <= 10000 && ack.RouteDigest == model.SSHRoutesDigest(ack.Routes) && !ack.LoadedAt.IsZero() && !ack.ObservedAt.IsZero() && ack.ExpiresAt.After(ack.ObservedAt) && !ack.ExpiresAt.After(ack.ObservedAt.Add(2*time.Minute))
}

func (stateStore *Store) withEdgeSSHAcknowledgements(write bool, callback func(map[string]model.EdgeSSHAcknowledgement) error) error {
	path := stateStore.path + ".ssh-front-acks.json"
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	records := map[string]model.EdgeSSHAcknowledgement{}
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &records); err != nil {
			return err
		}
		if records == nil {
			return ErrConflict
		}
		for key, ack := range records {
			if key != ack.EdgeID+"\x00"+ack.InstanceUID || !validEdgeSSHAcknowledgement(ack) {
				return ErrConflict
			}
		}
	}
	if err := callback(records); err != nil {
		return err
	}
	if !write {
		return nil
	}
	raw, err = json.Marshal(records)
	if err != nil {
		return err
	}
	return writeSSHIdentityFile(path, raw)
}
