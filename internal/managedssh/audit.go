package managedssh

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
)

type FileAudit struct {
	file  *os.File
	mutex sync.Mutex
}

func OpenFileAudit(path string) (*FileAudit, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		_ = file.Close()
		return nil, errors.New("managed SSH audit file must be private and regular")
	}
	return &FileAudit{file: file}, nil
}

func (audit *FileAudit) Record(ctx context.Context, event AuditEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	if _, err := audit.file.Write(raw); err != nil {
		return err
	}
	return audit.file.Sync()
}

func (audit *FileAudit) Close() error {
	audit.mutex.Lock()
	defer audit.mutex.Unlock()
	return audit.file.Close()
}
