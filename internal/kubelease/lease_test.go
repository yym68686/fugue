package kubelease

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
)

type memoryLeaseClient struct {
	mu       sync.Mutex
	lease    coordinationv1.Lease
	present  bool
	revision int
}

type releaseRaceClient struct {
	*memoryLeaseClient
	once sync.Once
}

func (c *releaseRaceClient) Update(ctx context.Context, namespace string, lease coordinationv1.Lease) error {
	if leaseHolder(lease) == "" {
		raced := false
		c.once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			holder := "backup/replacement"
			c.revision++
			c.lease.ResourceVersion = strconv.Itoa(c.revision)
			c.lease.Spec.HolderIdentity = &holder
			if c.lease.Annotations == nil {
				c.lease.Annotations = make(map[string]string)
			}
			c.lease.Annotations["fugue.pro/coordination-token"] = "replacement-token"
			raced = true
		})
		if raced {
			return ErrConflict
		}
	}
	return c.memoryLeaseClient.Update(ctx, namespace, lease)
}

func (c *memoryLeaseClient) Get(context.Context, string, string) (coordinationv1.Lease, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.present {
		return coordinationv1.Lease{}, false, nil
	}
	return *c.lease.DeepCopy(), true, nil
}

func (c *memoryLeaseClient) Create(_ context.Context, _ string, lease coordinationv1.Lease) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return errors.New("lease duration must be positive")
	}
	if c.present {
		return ErrConflict
	}
	c.revision++
	lease.ResourceVersion = strconv.Itoa(c.revision)
	c.lease = *lease.DeepCopy()
	c.present = true
	return nil
}

func (c *memoryLeaseClient) Update(_ context.Context, _ string, lease coordinationv1.Lease) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return errors.New("lease duration must be positive")
	}
	if !c.present {
		return ErrNotFound
	}
	if lease.ResourceVersion != c.lease.ResourceVersion {
		return ErrConflict
	}
	c.revision++
	lease.ResourceVersion = strconv.Itoa(c.revision)
	c.lease = *lease.DeepCopy()
	return nil
}

func TestManagerFencesSameHolderExecutionsAndReclaimsExpiredBackup(t *testing.T) {
	t.Parallel()

	client := &memoryLeaseClient{}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	first := newTestManager(client, "backup/run-1", "token-first")
	second := newTestManager(client, "backup/run-1", "token-second")

	held, err := first.TryAcquireOrRenew(context.Background(), now)
	if err != nil || !held {
		t.Fatalf("first acquire: held=%v err=%v", held, err)
	}
	created, found, err := client.Get(context.Background(), "control-plane", "backup")
	if err != nil || !found {
		t.Fatalf("read initial lease: found=%v err=%v", found, err)
	}
	if created.Spec.LeaseTransitions == nil || *created.Spec.LeaseTransitions != 0 {
		t.Fatalf("initial acquisition must have zero holder transitions, got %+v", created.Spec.LeaseTransitions)
	}
	held, err = second.TryAcquireOrRenew(context.Background(), now.Add(time.Minute))
	if err != nil || held {
		t.Fatalf("second execution must be fenced before expiry: held=%v err=%v", held, err)
	}
	held, err = second.TryAcquireOrRenew(context.Background(), now.Add(2*time.Minute))
	if err != nil || !held {
		t.Fatalf("expired backup holder should be recoverable: held=%v err=%v", held, err)
	}
	takenOver, found, err := client.Get(context.Background(), "control-plane", "backup")
	if err != nil || !found {
		t.Fatalf("read taken-over lease: found=%v err=%v", found, err)
	}
	if takenOver.Spec.LeaseTransitions == nil || *takenOver.Spec.LeaseTransitions != 1 {
		t.Fatalf("holder takeover must increment transitions once, got %+v", takenOver.Spec.LeaseTransitions)
	}

	released, err := first.Release(context.Background(), now.Add(2*time.Minute))
	if err != nil || released {
		t.Fatalf("old token must not release new execution: released=%v err=%v", released, err)
	}
	released, err = second.Release(context.Background(), now.Add(2*time.Minute))
	if err != nil || !released {
		t.Fatalf("current execution release: released=%v err=%v", released, err)
	}
	lease, found, err := client.Get(context.Background(), "control-plane", "backup")
	if err != nil || !found {
		t.Fatalf("read released lease: found=%v err=%v", found, err)
	}
	if holder := leaseHolder(lease); holder != "" {
		t.Fatalf("expected an empty holder after release, got %q", holder)
	}
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds != 120 {
		t.Fatalf("expected schema-valid duration after release, got %+v", lease.Spec.LeaseDurationSeconds)
	}
	if got := lease.Annotations["fugue.pro/coordination-token"]; got != "" {
		t.Fatalf("expected release to clear fencing token, got %q", got)
	}
	third := newTestManager(client, "backup/run-2", "token-third")
	held, err = third.TryAcquireOrRenew(context.Background(), now.Add(2*time.Minute))
	if err != nil || !held {
		t.Fatalf("empty released Lease must be immediately reusable: held=%v err=%v", held, err)
	}
}

func TestManagerReleaseDoesNotOverwriteReplacementOwnerAfterConflict(t *testing.T) {
	t.Parallel()

	base := &memoryLeaseClient{}
	client := &releaseRaceClient{memoryLeaseClient: base}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(client, "backup/original", "original-token")
	held, err := manager.TryAcquireOrRenew(context.Background(), now)
	if err != nil || !held {
		t.Fatalf("acquire original Lease: held=%v err=%v", held, err)
	}
	released, err := manager.Release(context.Background(), now.Add(time.Second))
	if err != nil || released {
		t.Fatalf("release must observe replacement owner after conflict: released=%v err=%v", released, err)
	}
	lease, found, err := base.Get(context.Background(), "control-plane", "backup")
	if err != nil || !found {
		t.Fatalf("read replacement Lease: found=%v err=%v", found, err)
	}
	if holder := leaseHolder(lease); holder != "backup/replacement" {
		t.Fatalf("replacement holder was overwritten: %q", holder)
	}
	if got := lease.Annotations["fugue.pro/coordination-token"]; got != "replacement-token" {
		t.Fatalf("replacement token was overwritten: %q", got)
	}
}

func TestManagerNeverTakesExpiredReleaseHolderWhenPolicyRejectsIt(t *testing.T) {
	t.Parallel()

	client := &memoryLeaseClient{}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	release := newTestManager(client, "release/123-1", "release-token")
	held, err := release.TryAcquireOrRenew(context.Background(), now)
	if err != nil || !held {
		t.Fatalf("release acquire: held=%v err=%v", held, err)
	}
	backup := newTestManager(client, "backup/run-2", "backup-token")
	held, err = backup.TryAcquireOrRenew(context.Background(), now.Add(24*time.Hour))
	if err != nil || held {
		t.Fatalf("backup must not take an expired release holder: held=%v err=%v", held, err)
	}
}

func TestManagerConcurrentCreateHasSingleWinner(t *testing.T) {
	t.Parallel()

	client := &memoryLeaseClient{}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	const contenders = 32
	results := make(chan bool, contenders)
	errorsCh := make(chan error, contenders)
	var wg sync.WaitGroup
	for index := 0; index < contenders; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			manager := newTestManager(client, "backup/run-"+strconv.Itoa(index), "token-"+strconv.Itoa(index))
			held, err := manager.TryAcquireOrRenew(context.Background(), now)
			results <- held
			errorsCh <- err
		}(index)
	}
	wg.Wait()
	close(results)
	close(errorsCh)

	winners := 0
	for held := range results {
		if held {
			winners++
		}
	}
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent acquire error: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one Lease winner, got %d", winners)
	}
}

func TestManagerSurfacesClientErrors(t *testing.T) {
	t.Parallel()

	manager := newTestManager(errorLeaseClient{}, "backup/run", "token")
	if _, err := manager.TryAcquireOrRenew(context.Background(), time.Now()); !errors.Is(err, errTestLeaseClient) {
		t.Fatalf("expected client error, got %v", err)
	}
}

func newTestManager(client Client, holder, token string) *Manager {
	return &Manager{
		Client:             client,
		Namespace:          "control-plane",
		Name:               "backup",
		HolderIdentity:     holder,
		Duration:           2 * time.Minute,
		TokenAnnotationKey: "fugue.pro/coordination-token",
		Token:              token,
		CanTakeOverExpired: func(current string) bool {
			return strings.HasPrefix(current, "backup/")
		},
	}
}

var errTestLeaseClient = errors.New("test lease client error")

type errorLeaseClient struct{}

func (errorLeaseClient) Get(context.Context, string, string) (coordinationv1.Lease, bool, error) {
	return coordinationv1.Lease{}, false, errTestLeaseClient
}

func (errorLeaseClient) Create(context.Context, string, coordinationv1.Lease) error {
	return errTestLeaseClient
}

func (errorLeaseClient) Update(context.Context, string, coordinationv1.Lease) error {
	return errTestLeaseClient
}
