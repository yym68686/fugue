package platformconfig

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

// DecodeRuntimeSnapshotReader bounds decoder buffering by an array element
// instead of a complete snapshot. The decoded snapshot itself remains O(N).
func DecodeRuntimeSnapshotReader(r io.Reader) (RuntimeSnapshot, error) {
	var snapshot RuntimeSnapshot
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return snapshot, fmt.Errorf("runtime snapshot must be an object: %v", err)
	}
	v := reflect.ValueOf(&snapshot).Elem()
	typ := v.Type()
	fields := map[string]int{}
	for i := 0; i < typ.NumField(); i++ {
		fields[strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]] = i
	}
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return snapshot, err
		}
		name, ok := token.(string)
		index, exists := fields[name]
		if !ok || !exists || seen[name] {
			return snapshot, fmt.Errorf("unknown or duplicate runtime snapshot field")
		}
		seen[name] = true
		field := v.Field(index)
		if field.Kind() != reflect.Slice {
			if err = d.Decode(field.Addr().Interface()); err != nil {
				return snapshot, err
			}
			continue
		}
		token, err = d.Token()
		if err != nil {
			return snapshot, err
		}
		if token == nil {
			continue
		}
		if token != json.Delim('[') {
			return snapshot, fmt.Errorf("runtime snapshot array expected")
		}
		field.Set(reflect.MakeSlice(field.Type(), 0, 0))
		for d.More() {
			item := reflect.New(field.Type().Elem())
			if err = d.Decode(item.Interface()); err != nil {
				return snapshot, err
			}
			field.Set(reflect.Append(field, item.Elem()))
		}
		if _, err = d.Token(); err != nil {
			return snapshot, err
		}
	}
	if _, err = d.Token(); err != nil {
		return snapshot, err
	}
	if _, err = d.Token(); err != io.EOF {
		return snapshot, fmt.Errorf("runtime snapshot has trailing JSON")
	}
	return snapshot, nil
}

// WriteRuntimeSnapshot emits the existing v1 canonical JSON one array element
// at a time. Hashing/persistence need not allocate another snapshot-sized byte
// buffer. Individual values still use encoding/json, retaining its escaping,
// numbers and custom marshaler semantics. Their maximum size still matters.
// Writers implementing ObserveRuntimeSnapshotDNSSelection receive exact byte
// ranges after each successfully encoded observation; this does not change v1 bytes.
func WriteRuntimeSnapshot(w io.Writer, snapshot RuntimeSnapshot) error {
	s := snapshotJSONWriter{w: w}
	s.text("{")
	fields := []struct {
		name  string
		value any
		omit  bool
	}{
		{"dns_selections", snapshot.DNSSelections, len(snapshot.DNSSelections) == 0},
		{"dns_edge_endpoints", snapshot.DNSEdgeEndpoints, len(snapshot.DNSEdgeEndpoints) == 0},
		{"dns_consumers", snapshot.DNSConsumers, len(snapshot.DNSConsumers) == 0},
		{"tls_domains", snapshot.TLSDomains, len(snapshot.TLSDomains) == 0},
		{"dns_placements", snapshot.DNSPlacements, len(snapshot.DNSPlacements) == 0},
		{"dns_flatten", snapshot.DNSFlatten, len(snapshot.DNSFlatten) == 0},
		{"intent_generation", snapshot.IntentGeneration, false},
		{"policy_generation", snapshot.PolicyGeneration, false},
		{"facts", snapshot.Facts, len(snapshot.Facts) == 0},
		{"captured_at", snapshot.CapturedAt, snapshot.CapturedAt == nil},
		{"origins", snapshot.Origins, len(snapshot.Origins) == 0},
		{"releases", snapshot.Releases, len(snapshot.Releases) == 0},
	}
	first := true
	for _, field := range fields {
		if field.omit || s.err != nil {
			continue
		}
		if !first {
			s.text(",")
		}
		first = false
		s.value(field.name)
		s.text(":")
		switch value := field.value.(type) {
		case []DNSSelectionObservation:
			s.text("[")
			for i, fact := range value {
				if i > 0 {
					s.text(",")
				}
				if len(fact.PhysicalEvidence) > 0 {
					canonical, err := canonicalPhysicalEvidence(fact.PhysicalEvidence)
					if err != nil {
						return err
					}
					fact.PhysicalEvidence = canonical
				}
				start := s.bytes
				s.value(fact)
				if observer, ok := w.(interface {
					ObserveRuntimeSnapshotDNSSelection(int64, int64, DNSSelectionObservation)
				}); ok && s.err == nil {
					observer.ObserveRuntimeSnapshotDNSSelection(start, s.bytes-start, fact)
				}
				if s.err != nil {
					return s.err
				}
			}
			s.text("]")
		case map[string]any:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			s.text("{")
			for i, key := range keys {
				if i > 0 {
					s.text(",")
				}
				s.value(key)
				s.text(":")
				s.value(value[key])
				if s.err != nil {
					return s.err
				}
			}
			s.text("}")
		default:
			v := reflect.ValueOf(value)
			if v.Kind() == reflect.Slice {
				s.text("[")
				for i := 0; i < v.Len(); i++ {
					if i > 0 {
						s.text(",")
					}
					s.value(v.Index(i).Interface())
					if s.err != nil {
						return s.err
					}
				}
				s.text("]")
			} else {
				s.value(value)
			}
		}
	}
	s.text("}")
	return s.err
}

type snapshotJSONWriter struct {
	w     io.Writer
	err   error
	bytes int64
}

func (s *snapshotJSONWriter) text(value string) {
	if s.err == nil {
		var n int
		n, s.err = io.WriteString(s.w, value)
		s.bytes += int64(n)
		if s.err == nil && n != len(value) {
			s.err = io.ErrShortWrite
		}
	}
}
func (s *snapshotJSONWriter) value(value any) {
	if s.err != nil {
		return
	}
	var raw []byte
	raw, s.err = json.Marshal(value)
	if s.err == nil {
		var n int
		n, s.err = s.w.Write(raw)
		s.bytes += int64(n)
		if s.err == nil && n != len(raw) {
			s.err = io.ErrShortWrite
		}
	}
}
