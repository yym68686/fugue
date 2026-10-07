package dnsserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/lkgcache"
	"fugue/internal/platformconsumer"
	"github.com/miekg/dns"
)

const dnsDecisionRetention = 64
const dnsDecisionQueue = 32

type DNSDecisionSnapshot struct {
	NodeID            string                      `json:"node_id"`
	ProcessID         string                      `json:"process_id"`
	CapturedAt        time.Time                   `json:"captured_at"`
	Publication       DNSDecisionPublicationState `json:"publication"`
	Receipts          []DNSDecisionReceipt        `json:"receipts"`
	Retained          int                         `json:"retained"`
	Dropped           uint64                      `json:"dropped"`
	Evicted           uint64                      `json:"evicted"`
	PersistenceErrors uint64                      `json:"persistence_errors"`
	RecoveryErrors    uint64                      `json:"recovery_errors"`
	RetentionLimit    int                         `json:"retention_limit"`
	MaxReceiptBytes   int                         `json:"max_receipt_bytes"`
}

type dnsDecisionPending struct {
	receipt DNSDecisionReceipt
	capture *dnsDecisionCapture
}

type dnsDecisionJournal struct {
	nodeID            string
	processID         string
	directory         string
	queue             chan dnsDecisionPending
	sequence          atomic.Uint64
	dropped           atomic.Uint64
	evicted           atomic.Uint64
	persistenceErrors atomic.Uint64
	recoveryErrors    atomic.Uint64
	records           atomic.Pointer[[]DNSDecisionReceipt]
	write             func(string, []byte) error
}

func (s *Service) startDNSDecisionJournal(ctx context.Context) {
	if s.decisionAudit.Load() != nil {
		return
	}
	identity := make([]byte, 16)
	if _, err := rand.Read(identity); err != nil {
		return
	}
	journal := &dnsDecisionJournal{nodeID: s.Config.DNSNodeID, processID: hex.EncodeToString(identity), queue: make(chan dnsDecisionPending, dnsDecisionQueue),
		write: func(filename string, data []byte) error { return lkgcache.AtomicWriteFile(filename, data, 0600) }}
	if s.Config.CachePath != "" {
		journal.directory = s.Config.CachePath + ".decisions"
	}
	empty := []DNSDecisionReceipt{}
	journal.records.Store(&empty)
	if !s.decisionAudit.CompareAndSwap(nil, journal) {
		return
	}
	go journal.run(ctx)
}

func (journal *dnsDecisionJournal) enqueue(request, response *dns.Msg, capture *dnsDecisionCapture, publication DNSDecisionPublicationState, transport string, observedAt time.Time, written bool) {
	if capture == nil || capture.err != nil || len(capture.input.Stages) == 0 && !capture.input.NoServingState || len(request.Question) != 1 {
		journal.dropped.Add(1)
		return
	}
	query := dns.Msg{MsgHdr: request.MsgHdr, Question: append([]dns.Question(nil), request.Question...)}
	query.Extra = dnsDecisionRedactOPT(request.Extra)
	queryWire, err := query.Pack()
	if err != nil {
		journal.dropped.Add(1)
		return
	}
	capture.input.Query = queryWire
	sequence := journal.sequence.Add(1)
	receipt := DNSDecisionReceipt{Schema: DNSDecisionSchema, DecisionID: fmt.Sprintf("%s-%d", journal.processID, sequence),
		ProcessID: journal.processID, NodeID: journal.nodeID, ObservedAt: observedAt,
		Hostname: normalizeName(request.Question[0].Name), QType: request.Question[0].Qtype, QueryID: request.Id, Transport: transport,
		RCode: response.Rcode, RRSet: dnsRRStrings(response.Answer), Authority: dnsRRStrings(response.Ns), Additional: dnsRRStrings(dnsDecisionRedactOPT(response.Extra)),
		WriteSucceeded: written, Publication: publication, AnswerPublication: capture.answerPublication, Records: capture.records}
	select {
	case journal.queue <- dnsDecisionPending{receipt: receipt, capture: capture}:
	default:
		journal.dropped.Add(1)
	}
}

func (journal *dnsDecisionJournal) run(ctx context.Context) {
	journal.recover()
	for {
		select {
		case <-ctx.Done():
			return
		case pending := <-journal.queue:
			journal.append(pending)
		}
	}
}

func (journal *dnsDecisionJournal) append(pending dnsDecisionPending) {
	defer func() {
		if recover() != nil {
			journal.dropped.Add(1)
		}
	}()
	for _, input := range pending.capture.observations {
		pending.capture.describeRecord(input.entry, input.materialized, input.hint, input.at, input.audit, input.failure)
	}
	input, err := json.Marshal(pending.capture.input)
	if err != nil || len(input) > DNSDecisionMaxBytes {
		journal.dropped.Add(1)
		return
	}
	receipt := pending.receipt
	receipt.Records = pending.capture.records
	receipt.ReplayInput = input
	receipt.EvidenceDigest = dnsDecisionDigest(receipt)
	raw, err := json.Marshal(receipt)
	if err != nil || len(raw) > DNSDecisionMaxBytes {
		journal.dropped.Add(1)
		return
	}
	var retained DNSDecisionReceipt
	if json.Unmarshal(raw, &retained) != nil {
		journal.dropped.Add(1)
		return
	}
	previous := journal.records.Load()
	updated := append([]DNSDecisionReceipt{}, (*previous)...)
	updated = append(updated, retained)
	if len(updated) > dnsDecisionRetention {
		journal.evicted.Add(1)
		updated = updated[1:]
	}
	journal.records.Store(&updated)
	if journal.directory == "" {
		journal.persistenceErrors.Add(1)
		return
	}
	slot := journal.sequenceForReceipt(receipt.DecisionID) % dnsDecisionRetention
	filename := filepath.Join(journal.directory, fmt.Sprintf("%02d.json", slot))
	if err := os.MkdirAll(journal.directory, 0700); err != nil {
		journal.persistenceErrors.Add(1)
		return
	}
	if err := journal.write(filename, raw); err != nil {
		journal.persistenceErrors.Add(1)
	}
}

func (journal *dnsDecisionJournal) sequenceForReceipt(identity string) uint64 {
	var sequence uint64
	_, _ = fmt.Sscanf(identity, journal.processID+"-%d", &sequence)
	return sequence
}

func (journal *dnsDecisionJournal) recover() {
	if journal.directory == "" {
		return
	}
	receipts := []DNSDecisionReceipt{}
	for index := 0; index < dnsDecisionRetention; index++ {
		filename := filepath.Join(journal.directory, fmt.Sprintf("%02d.json", index))
		raw, err := platformconsumer.ReadFile(filename, DNSDecisionMaxBytes)
		if os.IsNotExist(err) {
			continue
		}
		var receipt DNSDecisionReceipt
		if err != nil || json.Unmarshal(raw, &receipt) != nil || receipt.Schema != DNSDecisionSchema || receipt.NodeID != journal.nodeID || receipt.EvidenceDigest != dnsDecisionDigest(receipt) {
			journal.recoveryErrors.Add(1)
			continue
		}
		receipts = append(receipts, receipt)
	}
	sort.Slice(receipts, func(first, second int) bool { return receipts[first].ObservedAt.Before(receipts[second].ObservedAt) })
	journal.records.Store(&receipts)
}

func (journal *dnsDecisionJournal) snapshot(hostname, decisionID string, limit int) DNSDecisionSnapshot {
	records := journal.records.Load()
	result := DNSDecisionSnapshot{NodeID: journal.nodeID, ProcessID: journal.processID, CapturedAt: time.Now().UTC(), Receipts: []DNSDecisionReceipt{},
		Retained: len(*records), Dropped: journal.dropped.Load(), Evicted: journal.evicted.Load(), PersistenceErrors: journal.persistenceErrors.Load(),
		RecoveryErrors: journal.recoveryErrors.Load(), RetentionLimit: dnsDecisionRetention, MaxReceiptBytes: DNSDecisionMaxBytes}
	for index := len(*records) - 1; index >= 0 && len(result.Receipts) < limit; index-- {
		receipt := (*records)[index]
		if hostname != "" && normalizeName(hostname) != receipt.Hostname || decisionID != "" && decisionID != receipt.DecisionID {
			continue
		}
		result.Receipts = append(result.Receipts, receipt)
	}
	return result
}

func (s *Service) handleDNSDecisions(writer http.ResponseWriter, request *http.Request) {
	limit := 5
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			httpx.WriteError(writer, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = parsed
	}
	hostname, decisionID := request.URL.Query().Get("hostname"), request.URL.Query().Get("decision_id")
	if err := ValidateDNSDecisionFilter(hostname, decisionID, limit); err != nil {
		httpx.WriteError(writer, http.StatusBadRequest, err.Error())
		return
	}
	journal := s.decisionAudit.Load()
	if journal == nil {
		httpx.WriteError(writer, http.StatusServiceUnavailable, "DNS decision journal unavailable")
		return
	}
	result := journal.snapshot(hostname, decisionID, limit)
	result.Publication = decisionPublicationState(s.platformServing.Load(), s.decisionPublication.Load())
	writer.Header().Set("Cache-Control", "private, no-store")
	httpx.WriteJSON(writer, http.StatusOK, result)
}
