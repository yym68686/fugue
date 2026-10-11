package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"fugue/internal/store"
)

type inventoryPageRef struct {
	Key  string
	Hash string
}
type inventoryPagesEnvelope struct {
	Version    int
	Checkpoint backupInventoryCheckpoint
	Maps       map[string][]inventoryPageRef
}

// Stable hash partitions keep a changed observation page from shifting every
// later page. Generation keys let Current become Complete without rewriting
// the complete inventory again. Only changed pages are written by the store.
func encodeBackupInventoryPages(cp backupInventoryCheckpoint) (store.ObservationPages, error) {
	envelope := inventoryPagesEnvelope{Version: 2, Checkpoint: cp, Maps: map[string][]inventoryPageRef{}}
	out := store.ObservationPages{Pages: map[string][]byte{}}
	for _, ptr := range []**backupInventoryScan{&envelope.Checkpoint.Current, &envelope.Checkpoint.Complete} {
		if *ptr == nil {
			continue
		}
		scan := **ptr
		*ptr = &scan
		if scan.Generation == "" || len(scan.Generation) > 128 || strings.Contains(scan.Generation, "/") {
			return out, fmt.Errorf("invalid inventory generation")
		}
		for _, field := range []struct {
			name  string
			value any
		}{{"wanted", scan.Wanted}, {"references", scan.References}, {"totals", scan.Totals}} {
			key := scan.Generation + "/" + field.name
			if _, ok := envelope.Maps[key]; ok {
				continue
			}
			refs, err := encodeInventoryMapPages(key, reflect.ValueOf(field.value), out.Pages)
			if err != nil {
				return out, err
			}
			envelope.Maps[key] = refs
		}
		scan.Wanted = nil
		scan.References = nil
		scan.Totals = nil
	}
	var err error
	out.Metadata, err = json.Marshal(envelope)
	return out, err
}

func encodeInventoryMapPages(prefix string, value reflect.Value, pages map[string][]byte) ([]inventoryPageRef, error) {
	groups := map[string]map[string]json.RawMessage{}
	iter := value.MapRange()
	for iter.Next() {
		key := iter.Key().String()
		raw, err := json.Marshal(iter.Value().Interface())
		if err != nil {
			return nil, err
		}
		h := sha256.Sum256([]byte(key))
		bucket := hex.EncodeToString(h[:1])
		if groups[bucket] == nil {
			groups[bucket] = map[string]json.RawMessage{}
		}
		groups[bucket][key] = raw
	}
	refs := []inventoryPageRef{}
	var split func(string, map[string]json.RawMessage) error
	split = func(bucket string, items map[string]json.RawMessage) error {
		raw, err := json.Marshal(items)
		if err != nil {
			return err
		}
		if len(raw) > 64<<10 {
			if len(bucket) >= 64 || len(items) < 2 {
				return fmt.Errorf("individual inventory observation exceeds page budget")
			}
			children := map[string]map[string]json.RawMessage{}
			for key, v := range items {
				h := sha256.Sum256([]byte(key))
				child := hex.EncodeToString(h[:])[:len(bucket)+1]
				if children[child] == nil {
					children[child] = map[string]json.RawMessage{}
				}
				children[child][key] = v
			}
			for child, values := range children {
				if err := split(child, values); err != nil {
					return err
				}
			}
			return nil
		}
		name := prefix + "/" + bucket
		h := sha256.Sum256(raw)
		pages[name] = raw
		refs = append(refs, inventoryPageRef{Key: name, Hash: hex.EncodeToString(h[:])})
		return nil
	}
	for bucket, items := range groups {
		if err := split(bucket, items); err != nil {
			return nil, err
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Key < refs[j].Key })
	return refs, nil
}

func decodeBackupInventoryPages(pages store.ObservationPages, legacy []byte) (backupInventoryCheckpoint, error) {
	cp := backupInventoryCheckpoint{Version: 1}
	if pages.Metadata == nil {
		if len(legacy) > 0 {
			if err := json.Unmarshal(legacy, &cp); err != nil {
				return cp, err
			}
		}
		if cp.Version != 1 {
			return cp, fmt.Errorf("unsupported backup inventory checkpoint")
		}
		return cp, nil
	}
	var envelope inventoryPagesEnvelope
	if err := json.Unmarshal(pages.Metadata, &envelope); err != nil {
		return cp, err
	}
	if envelope.Version != 2 || envelope.Checkpoint.Version != 1 {
		return cp, fmt.Errorf("unsupported paged inventory checkpoint")
	}
	cp = envelope.Checkpoint
	used := map[string]bool{}
	for _, scan := range []*backupInventoryScan{cp.Current, cp.Complete} {
		if scan == nil {
			continue
		}
		scan.Wanted = map[string]bool{}
		scan.References = map[string]dataObjectInfo{}
		scan.Totals = map[string]backupInventoryTotals{}
		for _, field := range []struct {
			name   string
			target any
		}{{"wanted", &scan.Wanted}, {"references", &scan.References}, {"totals", &scan.Totals}} {
			prefix := scan.Generation + "/" + field.name
			refs, ok := envelope.Maps[prefix]
			if !ok {
				return cp, fmt.Errorf("inventory map manifest missing")
			}
			seen := map[string]bool{}
			for _, ref := range refs {
				raw, ok := pages.Pages[ref.Key]
				h := sha256.Sum256(raw)
				if !ok || !strings.HasPrefix(ref.Key, prefix+"/") || hex.EncodeToString(h[:]) != ref.Hash || len(raw) > 64<<10 {
					return cp, fmt.Errorf("inventory page missing or digest differs")
				}
				var values map[string]json.RawMessage
				if err := json.Unmarshal(raw, &values); err != nil {
					return cp, err
				}
				for key := range values {
					if seen[key] {
						return cp, fmt.Errorf("duplicate inventory page entry")
					}
					seen[key] = true
				}
				if err := json.Unmarshal(raw, field.target); err != nil {
					return cp, err
				}
				used[ref.Key] = true
			}
		}
	}
	if len(used) != len(pages.Pages) {
		return cp, fmt.Errorf("inventory pages outside metadata manifest")
	}
	return cp, nil
}
