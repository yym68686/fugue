package api

import (
	"context"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
	"time"
)

// Check for a saved reversion before the legacy global compiler's "same
// paths as full" / unchanged-source shortcuts. Failure to withdraw A never
// discards another domain's selected head. No queue item is consumed here:
// a hint is revisited if the independent transaction didn't commit.
func (s *Server) reconcileHostedWithdrawals(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease, projection platformIntentProjectionResponse) {
	if policy.Mode != "serving" || policy.PublicationRole != "" || policy.TargetScope != pc.GlobalScopeKey || policy.InputSource != "business-static-intent" || policy.DNSPolicyArtifactID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	after, _ := s.hostedWithdrawCursor.Load().(string)
	targets, next, e := s.store.HostedWithdrawalHints(ctx, projection.Intent, after)
	warn := func(h string, e error) {
		if s.log != nil {
			s.log.Printf("host withdrawal pending host=%s policy_release=%s error=%v", h, authority.ID, e)
		}
	}
	if e != nil {
		warn("", e)
		return
	}
	if len(targets) == 0 {
		s.hostedWithdrawCursor.Store(next)
		return
	}
	source, e := s.readHostedAddressSource(ctx, policy, authority, projection.hostedAddressHolds)
	if e != nil {
		warn("", e)
		return
	}
	for _, host := range targets {
		if ctx.Err() != nil {
			break
		}
		fence, e := s.store.WithdrawHostedSelection(ctx, source, host, func(in store.ProjectHostRouteInputs, p pc.HostedScopeProposal) (string, string, string, error) {
			b, err := matchHostedProjectIntent(in, p)
			return b.OwnerDigest, b.PathDigest, b.Direction, err
		})
		if e != nil {
			warn(host, e)
			continue
		}
		if fence > 0 && s.log != nil {
			s.log.Printf("host withdrawal selected host=%s host_fence=%d policy_release=%s (not an application receipt)", host, fence, authority.ID)
		}
	}
	s.hostedWithdrawCursor.Store(next)
}
