package kubelease

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	ErrConflict = errors.New("kubernetes lease update conflict")
	ErrNotFound = errors.New("kubernetes lease not found")
)

// Client is the minimal Kubernetes Lease API used by Manager. Implementations
// must preserve resourceVersion on reads and use it for updates.
type Client interface {
	Get(ctx context.Context, namespace, name string) (coordinationv1.Lease, bool, error)
	Create(ctx context.Context, namespace string, lease coordinationv1.Lease) error
	Update(ctx context.Context, namespace string, lease coordinationv1.Lease) error
}

// TakeoverPolicy decides whether an expired, non-empty holder may be replaced.
// Empty holders are always immediately reusable and do not call this function.
type TakeoverPolicy func(holder string) bool

// Manager performs resourceVersion-fenced acquire, renew, and release updates
// against one coordination.k8s.io/v1 Lease.
type Manager struct {
	Client             Client
	Namespace          string
	Name               string
	HolderIdentity     string
	Duration           time.Duration
	TokenAnnotationKey string
	Token              string
	CanTakeOverExpired TakeoverPolicy
}

func (m *Manager) TryAcquireOrRenew(ctx context.Context, now time.Time) (bool, error) {
	if err := m.validate(); err != nil {
		return false, err
	}
	now = now.UTC()
	current, found, err := m.Client.Get(ctx, m.Namespace, m.Name)
	if err != nil {
		return false, err
	}
	if !found {
		lease := m.newLease(now)
		if err := m.Client.Create(ctx, m.Namespace, lease); err != nil {
			if errors.Is(err, ErrConflict) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	holder := leaseHolder(current)
	if holder == m.HolderIdentity && m.tokenMatches(current) {
		m.applyRenewal(&current, now, false)
		if err := m.Client.Update(ctx, m.Namespace, current); err != nil {
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	if holder != "" {
		if !Expired(current, now) || m.CanTakeOverExpired == nil || !m.CanTakeOverExpired(holder) {
			return false, nil
		}
	}
	m.applyRenewal(&current, now, true)
	if err := m.Client.Update(ctx, m.Namespace, current); err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Release clears the holder using holder, token, and resourceVersion fencing.
// A false result means the Lease no longer belongs to this Manager.
func (m *Manager) Release(ctx context.Context, now time.Time) (bool, error) {
	if err := m.validate(); err != nil {
		return false, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		current, found, err := m.Client.Get(ctx, m.Namespace, m.Name)
		if err != nil {
			return false, err
		}
		if !found || leaseHolder(current) != m.HolderIdentity || !m.tokenMatches(current) {
			return false, nil
		}
		empty := ""
		durationSeconds := int32(m.Duration / time.Second)
		renewTime := metav1.NewMicroTime(now.UTC())
		current.Spec.HolderIdentity = &empty
		// coordination.k8s.io/v1 rejects non-positive Lease durations. An empty
		// holder is immediately reusable regardless of RenewTime, so retain the
		// configured positive duration while clearing ownership.
		current.Spec.LeaseDurationSeconds = &durationSeconds
		current.Spec.RenewTime = &renewTime
		if current.Annotations != nil && m.TokenAnnotationKey != "" {
			delete(current.Annotations, m.TokenAnnotationKey)
		}
		if err := m.Client.Update(ctx, m.Namespace, current); err != nil {
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				continue
			}
			return false, err
		}
		return true, nil
	}
	return false, ErrConflict
}

func (m *Manager) validate() error {
	if m == nil || m.Client == nil {
		return fmt.Errorf("kubernetes lease client is not configured")
	}
	if strings.TrimSpace(m.Namespace) == "" {
		return fmt.Errorf("kubernetes lease namespace is empty")
	}
	if strings.TrimSpace(m.Name) == "" {
		return fmt.Errorf("kubernetes lease name is empty")
	}
	if strings.TrimSpace(m.HolderIdentity) == "" {
		return fmt.Errorf("kubernetes lease holder identity is empty")
	}
	if m.Duration < time.Second {
		return fmt.Errorf("kubernetes lease duration must be at least one second")
	}
	if m.Duration/time.Second > time.Duration(^uint32(0)>>1) {
		return fmt.Errorf("kubernetes lease duration is too large")
	}
	if (m.TokenAnnotationKey == "") != (m.Token == "") {
		return fmt.Errorf("kubernetes lease token annotation and token must be configured together")
	}
	return nil
}

func (m *Manager) newLease(now time.Time) coordinationv1.Lease {
	holder := m.HolderIdentity
	durationSeconds := int32(m.Duration / time.Second)
	acquireTime := metav1.NewMicroTime(now)
	renewTime := metav1.NewMicroTime(now)
	// LeaseTransitions counts changes between holder identities. The initial
	// acquisition has not transitioned from another holder yet.
	transitions := int32(0)
	lease := coordinationv1.Lease{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coordinationv1.SchemeGroupVersion.String(),
			Kind:       "Lease",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.Name,
			Namespace: m.Namespace,
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &durationSeconds,
			AcquireTime:          &acquireTime,
			RenewTime:            &renewTime,
			LeaseTransitions:     &transitions,
		},
	}
	m.applyToken(&lease)
	return lease
}

func (m *Manager) applyRenewal(lease *coordinationv1.Lease, now time.Time, transition bool) {
	holder := m.HolderIdentity
	durationSeconds := int32(m.Duration / time.Second)
	renewTime := metav1.NewMicroTime(now)
	lease.Spec.HolderIdentity = &holder
	lease.Spec.LeaseDurationSeconds = &durationSeconds
	lease.Spec.RenewTime = &renewTime
	if transition || lease.Spec.AcquireTime == nil {
		acquireTime := metav1.NewMicroTime(now)
		lease.Spec.AcquireTime = &acquireTime
	}
	if transition {
		transitions := int32(1)
		if lease.Spec.LeaseTransitions != nil {
			transitions = *lease.Spec.LeaseTransitions + 1
		}
		lease.Spec.LeaseTransitions = &transitions
	}
	m.applyToken(lease)
}

func (m *Manager) applyToken(lease *coordinationv1.Lease) {
	if m.TokenAnnotationKey == "" {
		return
	}
	if lease.Annotations == nil {
		lease.Annotations = make(map[string]string)
	}
	lease.Annotations[m.TokenAnnotationKey] = m.Token
}

func (m *Manager) tokenMatches(lease coordinationv1.Lease) bool {
	if m.TokenAnnotationKey == "" {
		return true
	}
	return lease.Annotations != nil && lease.Annotations[m.TokenAnnotationKey] == m.Token
}

func leaseHolder(lease coordinationv1.Lease) string {
	if lease.Spec.HolderIdentity == nil {
		return ""
	}
	return strings.TrimSpace(*lease.Spec.HolderIdentity)
}

func Expired(lease coordinationv1.Lease, now time.Time) bool {
	var reference time.Time
	if lease.Spec.RenewTime != nil {
		reference = lease.Spec.RenewTime.Time
	} else if lease.Spec.AcquireTime != nil {
		reference = lease.Spec.AcquireTime.Time
	}
	if reference.IsZero() || lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return true
	}
	expiresAt := reference.UTC().Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	return !now.UTC().Before(expiresAt)
}
