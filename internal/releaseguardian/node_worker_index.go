package releaseguardian

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const nodeWorkerIndexKind = "node-worker-index"
const nodeWorkerIndexKey = "index.json"
const nodeWorkerIndexSchema = "edge-node-worker-index/v1"
const nodeWorkerIndexLimit = 512 << 10

// NodeWorkerIndex is a control-plane pointer, not a release receipt. Readers
// still validate the exact immutable journal and terminal receipt it names.
type NodeWorkerIndex struct {
	Schema string                         `json:"schema"`
	Group  string                         `json:"group"`
	Nodes  map[string]NodeWorkerIndexHead `json:"nodes"`
}

type NodeWorkerIndexHead struct {
	JournalDigest string `json:"journal_digest"`
	Generation    uint64 `json:"generation"`
}

func NodeWorkerIndexName(group string) string { return "fugue-node-worker-index-" + group }

func NodeWorkerArchiveName(digest string) string { return nodeWorkerInputName("archive", digest) }

// Archives use the release ledger's canonical JSON (sorted object keys),
// unlike the live projection, which uses the Front's typed wire layout.
func ReadArchivedNodeWorkerJournal(raw []byte) (edgegroupfront.NodeWorkerJournal, error) {
	var journal edgegroupfront.NodeWorkerJournal
	if len(raw) == 0 || len(raw) > 16<<10 || decodeStrict(raw, &journal) != nil {
		return journal, errors.New("invalid Guardian node Worker archive encoding")
	}
	canonical, err := declarativerelease.CanonicalJSON(journal)
	if err != nil || !bytes.Equal(canonical, raw) {
		return journal, errors.New("Guardian node Worker archive is not canonical")
	}
	sealed, err := journal.Seal()
	if err != nil || sealed.Digest != journal.Digest {
		return journal, errors.New("Guardian node Worker archive digest or scope differs")
	}
	return journal, nil
}

func ReadNodeWorkerIndex(cm *corev1.ConfigMap, group string) (NodeWorkerIndex, error) {
	var index NodeWorkerIndex
	if cm == nil || cm.Name != NodeWorkerIndexName(group) || cm.UID == "" || cm.ResourceVersion == "" ||
		cm.DeletionTimestamp != nil || cm.Immutable != nil && *cm.Immutable || len(cm.BinaryData) != 0 ||
		cm.Labels["fugue.pro/authority-kind"] != nodeWorkerIndexKind ||
		cm.Labels["fugue.pro/group"] != group || cm.Labels["fugue.pro/authority-store"] != "true" ||
		cm.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" ||
		len(cm.Data) != 1 || len(cm.Data[nodeWorkerIndexKey]) > nodeWorkerIndexLimit ||
		decodeStrict([]byte(cm.Data[nodeWorkerIndexKey]), &index) != nil ||
		index.Schema != nodeWorkerIndexSchema || index.Group != group || len(index.Nodes) == 0 {
		return NodeWorkerIndex{}, errors.New("invalid Guardian node Worker index")
	}
	for uid, head := range index.Nodes {
		if uid == "" || !digestPattern.MatchString(head.JournalDigest) || head.Generation == 0 {
			return NodeWorkerIndex{}, errors.New("invalid Guardian node Worker index head")
		}
	}
	return index, nil
}

func (index *NodeWorkerIndex) advance(j edgegroupfront.NodeWorkerJournal, r NodeWorkerResult) error {
	if r.Outcome == "expired_without_cutover" {
		return nil
	}
	next := NodeWorkerIndexHead{JournalDigest: j.Digest, Generation: r.Activation.Generation}
	old := index.Nodes[j.NodeUID]
	if old.Generation == next.Generation && old != next {
		return errors.New("conflicting Guardian Worker results at the same node generation")
	}
	if old.Generation < next.Generation {
		index.Nodes[j.NodeUID] = next
	}
	return nil
}

// EnsureNodeWorkerIndex initializes indexes for installations that already
// have completed node-local transactions. The caller holds the group mutation
// Lease. The only actor that lists old journals is their owning Guardian.
func (store *AuthorityStore) EnsureNodeWorkerIndex(ctx context.Context, group string) error {
	found, err := store.HasNodeWorkerIndex(ctx, group)
	if found || err != nil {
		return err
	}
	return store.bootstrapNodeWorkerIndex(ctx, group)
}

func (store *AuthorityStore) HasNodeWorkerIndex(ctx context.Context, group string) (bool, error) {
	if !groupPattern.MatchString(group) {
		return false, errors.New("invalid node Worker index group")
	}
	cm, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, NodeWorkerIndexName(group), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = ReadNodeWorkerIndex(cm, group)
	return err == nil, err
}

func (store *AuthorityStore) bootstrapNodeWorkerIndex(ctx context.Context, group string) error {
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	opts := metav1.ListOptions{LabelSelector: "fugue.pro/group=" + group + ",fugue.pro/authority-kind=node-worker-archive", Limit: 128}
	index := NodeWorkerIndex{Schema: nodeWorkerIndexSchema, Group: group, Nodes: make(map[string]NodeWorkerIndexHead)}
	seen := make(map[string]map[uint64]bool)
	for page := 0; ; page++ {
		if page == 32 {
			return errors.New("Guardian node Worker history exceeds bootstrap bound")
		}
		archives, err := configs.List(ctx, opts)
		if err != nil {
			return err
		}
		for _, a := range archives.Items {
			j, err := ReadArchivedNodeWorkerJournal([]byte(a.Data["journal.json"]))
			if err != nil {
				return fmt.Errorf("invalid sealed Guardian node Worker archive: %w", err)
			}
			if j.Recipient.Group != group || a.Name != NodeWorkerArchiveName(j.Digest) {
				return errors.New("Guardian node Worker archive differs from its sealed name or group")
			}
			if a.UID == "" || a.ResourceVersion == "" || a.DeletionTimestamp != nil ||
				a.Immutable == nil || !*a.Immutable || len(a.Data) != 1 || len(a.BinaryData) != 0 ||
				a.Labels["fugue.pro/group"] != group || a.Labels["fugue.pro/authority-kind"] != "node-worker-archive" ||
				a.Labels["fugue.pro/authority-store"] != "true" || a.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" {
				return errors.New("invalid Guardian node Worker archive during index bootstrap")
			}
			result, found, err := store.ReadNodeWorkerResult(ctx, j)
			if err != nil || !found {
				return errors.Join(errors.New("Guardian node Worker archive has no exact terminal receipt"), err)
			}
			if result.Outcome != "expired_without_cutover" {
				if seen[j.NodeUID] == nil {
					seen[j.NodeUID] = make(map[uint64]bool)
				}
				if seen[j.NodeUID][result.Activation.Generation] {
					return errors.New("ambiguous Guardian node Worker result in archived history")
				}
				seen[j.NodeUID][result.Activation.Generation] = true
			}
			if err := index.advance(j, result); err != nil {
				return err
			}
		}
		if archives.Continue == "" {
			break
		}
		opts.Continue = archives.Continue
	}
	if len(index.Nodes) == 0 {
		return errors.New("Guardian node Worker mode lacks a completed activation")
	}
	labels := authorityLabels(group)
	labels["fugue.pro/authority-kind"] = nodeWorkerIndexKind
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: NodeWorkerIndexName(group), Namespace: store.namespace, Labels: labels}}
	return store.writeNodeWorkerIndex(ctx, object, index)
}

func (store *AuthorityStore) advanceNodeWorkerIndex(ctx context.Context, j edgegroupfront.NodeWorkerJournal, r NodeWorkerResult) error {
	group := j.Recipient.Group
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, NodeWorkerIndexName(group), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return store.bootstrapNodeWorkerIndex(ctx, group)
	}
	if err != nil {
		return err
	}
	index, err := ReadNodeWorkerIndex(object, group)
	if err != nil {
		return err
	}
	old := index.Nodes[j.NodeUID]
	if old.Generation > r.Activation.Generation {
		return errors.New("Guardian node Worker result would regress the committed head")
	}
	if err := index.advance(j, r); err != nil || old == index.Nodes[j.NodeUID] {
		return err
	}
	return store.writeNodeWorkerIndex(ctx, object, index)
}

func (store *AuthorityStore) writeNodeWorkerIndex(ctx context.Context, object *corev1.ConfigMap, index NodeWorkerIndex) error {
	raw, err := json.Marshal(index)
	if err != nil || len(raw) > nodeWorkerIndexLimit {
		return fmt.Errorf("cannot encode bounded Guardian node Worker index: %v", err)
	}
	next := object.DeepCopy()
	next.Data = map[string]string{nodeWorkerIndexKey: string(raw)}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	if next.ResourceVersion == "" {
		_, err = configs.Create(ctx, next, metav1.CreateOptions{})
	} else {
		_, err = configs.Update(ctx, next, metav1.UpdateOptions{})
	}
	return err
}
