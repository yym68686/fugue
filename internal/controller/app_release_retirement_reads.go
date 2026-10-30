package controller

import "context"

// A hundred retirement tasks may wait on drain observations concurrently, but
// decoding a complete publication or namespace inventory for every task at once
// can exhaust the controller's memory. This bounds only those read phases; it
// never shares or caches evidence between attempts or final deletion checks.
const appReleaseRetirementReadConcurrency = 4

type releaseRetirementReadLimitKey struct{}

func withReleaseRetirementReadLimit(ctx context.Context) context.Context {
	return context.WithValue(ctx, releaseRetirementReadLimitKey{}, make(chan struct{}, appReleaseRetirementReadConcurrency))
}

func acquireReleaseRetirementRead(ctx context.Context) (func(), error) {
	limit, _ := ctx.Value(releaseRetirementReadLimitKey{}).(chan struct{})
	if limit == nil {
		return func() {}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case limit <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-limit
			return nil, err
		}
		return func() { <-limit }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
