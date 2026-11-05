package inputarchive

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var ErrBudget = errors.New("compiler input copy budget exhausted")

// CopyBudget counts application payload and client operations, not TLS/header
// traffic or storage-device IO. The single-attempt S3 client disables SDK
// retries; ambiguous PUTs still consume their complete attempted body budget.
type CopyBudget struct {
	MaxPutBytes, MaxGetBytes       int64
	MaxPutRequests, MaxGetRequests int64
	BytesPerSecond                 int64
}

type CopyUsage struct {
	PutBytes    int64 `json:"put_payload_bytes"`
	GetBytes    int64 `json:"get_payload_bytes"`
	PutRequests int64 `json:"put_requests"`
	GetRequests int64 `json:"get_requests"`
	SourceBytes int64 `json:"source_raw_bytes"`
}

type BudgetObjects struct {
	reader, writer Objects
	budget         CopyBudget
	mu             sync.Mutex
	usage          CopyUsage
	next           time.Time
}

func NewBudgetObjects(reader, writer Objects, b CopyBudget) (*BudgetObjects, error) {
	if reader == nil || writer == nil || b.MaxPutBytes < 1 || b.MaxGetBytes < 1 || b.MaxPutRequests < 1 || b.MaxGetRequests < 1 || b.BytesPerSecond < 1 {
		return nil, ErrInvalid
	}
	return &BudgetObjects{reader: reader, writer: writer, budget: b}, nil
}

func (b *BudgetObjects) Usage() CopyUsage { b.mu.Lock(); defer b.mu.Unlock(); return b.usage }

// No accumulated idle credit: even the first payload waits its byte interval.
// Source reads, PUT bodies, and GET bodies share the same pace.
func (b *BudgetObjects) pace(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	start := time.Now()
	if b.next.After(start) {
		start = b.next
	}
	b.next = start.Add(time.Duration(int64(n) * int64(time.Second) / b.budget.BytesPerSecond))
	delay := time.Until(b.next)
	b.mu.Unlock()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (b *BudgetObjects) Put(ctx context.Context, key string, body []byte) error {
	b.mu.Lock()
	if b.usage.PutRequests >= b.budget.MaxPutRequests || int64(len(body)) > b.budget.MaxPutBytes-b.usage.PutBytes {
		b.mu.Unlock()
		return ErrBudget
	}
	b.mu.Unlock()
	if err := b.pace(ctx, len(body)); err != nil {
		return err
	}
	b.mu.Lock()
	b.usage.PutRequests++
	b.usage.PutBytes += int64(len(body))
	b.mu.Unlock()
	return b.writer.Put(ctx, key, body)
}

func (b *BudgetObjects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.usage.GetRequests >= b.budget.MaxGetRequests || b.usage.GetBytes >= b.budget.MaxGetBytes {
		b.mu.Unlock()
		return nil, ErrBudget
	}
	b.usage.GetRequests++
	b.mu.Unlock()
	body, err := b.reader.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return &budgetBody{ReadCloser: body, ctx: ctx, b: b}, nil
}

type budgetBody struct {
	io.ReadCloser
	ctx context.Context
	b   *BudgetObjects
}

func (r *budgetBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r.b.mu.Lock()
	left := r.b.budget.MaxGetBytes - r.b.usage.GetBytes
	r.b.mu.Unlock()
	if left <= 0 {
		return 0, ErrBudget
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	next := min(len(p), 64<<10, int(left))
	// Read at most one bounded buffer and charge actual bytes, including
	// bytes transferred before cancellation. Pace before returning payload;
	// short reads and EOF must not pay for unused buffer capacity.
	n, err := r.ReadCloser.Read(p[:next])
	r.b.mu.Lock()
	r.b.usage.GetBytes += int64(n)
	r.b.mu.Unlock()
	if n > 0 {
		if paceErr := r.b.pace(r.ctx, n); paceErr != nil {
			return n, errors.Join(err, paceErr)
		}
	}
	return n, err
}

func (b *BudgetObjects) Source(ctx context.Context, source io.Reader) io.Reader {
	return &pacedSource{ctx, source, b}
}

type pacedSource struct {
	ctx    context.Context
	source io.Reader
	b      *BudgetObjects
}

func (r *pacedSource) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	next := min(len(p), 64<<10)
	n, err := r.source.Read(p[:next])
	r.b.mu.Lock()
	r.b.usage.SourceBytes += int64(n)
	r.b.mu.Unlock()
	if n > 0 {
		if paceErr := r.b.pace(r.ctx, n); paceErr != nil {
			return n, errors.Join(err, paceErr)
		}
	}
	return n, err
}
