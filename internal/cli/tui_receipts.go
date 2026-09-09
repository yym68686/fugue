package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"fugue/internal/tui"
	"os"
	"path/filepath"
)

type tuiReceiptRecord struct {
	AppID     string `json:"app_id"`
	RequestID string `json:"request_id"`
}

func (p *tuiProvider) loadReceipts(directory string) error {
	// Partition by server and credential identity, never store the credential.
	key := sha256.Sum256([]byte(p.client.baseURL + "\x00" + p.client.token))
	p.receiptJournal = filepath.Join(directory, "tui-receipts-"+hex.EncodeToString(key[:12])+".json")
	data, err := os.ReadFile(p.receiptJournal)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var records []tuiReceiptRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("invalid TUI receipt journal %s: %w", p.receiptJournal, err)
	}
	p.uncertain = map[string]tui.Plan{}
	for _, record := range records {
		p.uncertain[record.AppID] = tui.Plan{ID: record.RequestID, Request: tui.ActionRequest{Target: tui.Target{Kind: "app", ID: record.AppID}}}
	}
	return nil
}
func (p *tuiProvider) saveReceiptsLocked() error {
	if p.receiptJournal == "" {
		return nil
	}
	records := make([]tuiReceiptRecord, 0, len(p.uncertain))
	for app, plan := range p.uncertain {
		records = append(records, tuiReceiptRecord{AppID: app, RequestID: plan.ID})
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	directory := filepath.Dir(p.receiptJournal)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(directory, ".tui-receipt-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), p.receiptJournal)
}
