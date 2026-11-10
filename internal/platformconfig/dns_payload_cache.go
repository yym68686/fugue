package platformconfig

import (
	"reflect"
	"sync"

	"fugue/internal/model"
	"golang.org/x/sync/singleflight"
)

type dnsPayloadEntry struct {
	payload DNSArtifactPayload
	bytes   int
}
type dnsPayloadCache struct {
	sync.Mutex
	entries map[[32]byte]dnsPayloadEntry
	order   [][32]byte
	bytes   int
	flights singleflight.Group
}

var decodedDNSPayloads dnsPayloadCache

// Only deterministic decoding/validation is reused. The key visits actual
// content and the complete envelope; live signatures, trust, freshness and
// serving assignments are still checked by callers on every read.
func (c *dnsPayloadCache) decode(a model.PlatformArtifact) (DNSArtifactPayload, error) {
	key, size, ok := dnsValidationArtifactKey(a)
	if !ok || a.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || size < 16<<10 || size > 4<<20 {
		return decodeDNSArtifactUncached(a)
	}
	value, err, _ := c.flights.Do(string(key[:]), func() (any, error) {
		c.Lock()
		entry, hit := c.entries[key]
		c.Unlock()
		if hit {
			return entry.payload, nil
		}
		payload, err := decodeDNSArtifactUncached(a)
		if err != nil {
			return payload, err
		}
		// Charge conservatively for decoded maps, slices and strings. Keep
		// entry count and total retained bytes bounded, including nested input.
		cost := 256 + dnsPayloadRetainedBytes(reflect.ValueOf(payload))
		c.Lock()
		defer c.Unlock()
		if c.entries == nil {
			c.entries = make(map[[32]byte]dnsPayloadEntry)
		}
		for len(c.order) > 0 && (len(c.order) >= 16 || c.bytes+cost > 32<<20) {
			old := c.order[0]
			c.order = c.order[1:]
			c.bytes -= c.entries[old].bytes
			delete(c.entries, old)
		}
		if cost <= 32<<20 {
			c.entries[key] = dnsPayloadEntry{payload, cost}
			c.order = append(c.order, key)
			c.bytes += cost
		}
		return payload, nil
	})
	// No mutable part of the cached typed plan escapes (readiness, nested
	// artifacts, policies, records and query projections included).
	return cloneDNSPayloadValue(reflect.ValueOf(value.(DNSArtifactPayload))).Interface().(DNSArtifactPayload), err
}

// Input is a successfully JSON-decoded payload, so it has no cycles or custom
// mutable objects. Unexported time.Time internals are immutable and copied.
func cloneDNSPayloadValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(cloneDNSPayloadValue(v.Elem()))
		return out
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(cloneDNSPayloadValue(v.Elem()))
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		it := v.MapRange()
		for it.Next() {
			out.SetMapIndex(it.Key(), cloneDNSPayloadValue(it.Value()))
		}
		return out
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type()).Elem()
		if v.Kind() == reflect.Slice {
			out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		}
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(cloneDNSPayloadValue(v.Index(i)))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				out.Field(i).Set(cloneDNSPayloadValue(v.Field(i)))
			}
		}
		return out
	default:
		return v
	}
}

func dnsPayloadRetainedBytes(v reflect.Value) int {
	size := int(v.Type().Size())
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !v.IsNil() {
			size += dnsPayloadRetainedBytes(v.Elem())
		}
	case reflect.String:
		size += v.Len()
	case reflect.Map:
		size += 128 + 128*v.Len()
		it := v.MapRange()
		for it.Next() {
			size += dnsPayloadRetainedBytes(it.Key()) + dnsPayloadRetainedBytes(it.Value())
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			size += dnsPayloadRetainedBytes(v.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				size += dnsPayloadRetainedBytes(v.Field(i))
			}
		}
	}
	return size
}
