package planning

import (
	"context"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

// Budget owns the local per-observer cooldown and finite identity cache.
// The zero value is ready to use.
type Budget struct {
	mu   sync.Mutex
	last map[string]time.Time
}

func ReadyCandidates(ctx context.Context, nodes []model.EdgeNode, probe func(context.Context, model.EdgeNode) (routeprobe.Proof, error)) ([]model.EdgeNode, []routeprobe.Proof) {
	ready, proofs := []model.EdgeNode{}, []routeprobe.Proof{}
	for _, node := range nodes {
		if ctx.Err() != nil {
			break
		}
		proof, err := probe(ctx, node)
		if err != nil || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || proof.State != "" {
			continue
		}
		ready, proofs = append(ready, node), append(proofs, proof)
	}
	return ready, proofs
}

func (b *Budget) Reserve(observer string, now time.Time) bool {
	if !b.mu.TryLock() {
		return false
	}
	defer b.mu.Unlock()
	if b.last == nil {
		b.last = map[string]time.Time{}
	}
	for identity, at := range b.last {
		if now.Sub(at) >= 5*time.Minute {
			delete(b.last, identity)
		}
	}
	if len(b.last) >= 128 || now.Sub(b.last[observer]) < 30*time.Second {
		return false
	}
	b.last[observer] = now
	return true
}
