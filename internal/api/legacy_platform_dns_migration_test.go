package api

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

type platformDNSMigrationComparison struct {
	CapturedAt                 time.Time
	SourceRecordCount          int
	ArtifactRecordCount        int
	MatchingRecordCount        int
	ExpiredCandidateValueCount int
	Equivalent                 bool
	Differences                []platformDNSMigrationDifference
}

type platformDNSMigrationDifference struct {
	Hostname string
	Type     string
	Kind     string
	Fields   []string
}

func comparePlatformDNSRecords(source, candidate []model.EdgeDNSRecord, now time.Time) (platformDNSMigrationComparison, error) {
	result := platformDNSMigrationComparison{CapturedAt: now, SourceRecordCount: len(source), ArtifactRecordCount: len(candidate), Differences: []platformDNSMigrationDifference{}}
	left, _, err := dnsMigrationRecordIndex(source, now)
	if err != nil {
		return result, err
	}
	right, expired, err := dnsMigrationRecordIndex(candidate, now)
	if err != nil {
		return result, err
	}
	result.ExpiredCandidateValueCount = expired
	keys := map[string]bool{}
	for k := range left {
		keys[k] = true
	}
	for k := range right {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		parts := strings.SplitN(key, "\x00", 2)
		difference := platformDNSMigrationDifference{Hostname: parts[0], Type: parts[1], Fields: []string{}}
		l, lok := left[key]
		rr, rok := right[key]
		switch {
		case !rok:
			difference.Kind = "missing_from_artifact"
		case !lok:
			difference.Kind = "extra_in_artifact"
		default:
			fields := map[string]bool{}
			for k := range l {
				fields[k] = true
			}
			for k := range rr {
				fields[k] = true
			}
			for k := range fields {
				if !reflect.DeepEqual(l[k], rr[k]) {
					difference.Fields = append(difference.Fields, k)
				}
			}
			if len(difference.Fields) == 0 {
				result.MatchingRecordCount++
				continue
			}
			sort.Strings(difference.Fields)
			difference.Kind = "changed"
		}
		result.Differences = append(result.Differences, difference)
	}
	result.Equivalent = len(result.Differences) == 0
	return result, nil
}

func dnsMigrationRecordIndex(records []model.EdgeDNSRecord, now time.Time) (map[string]map[string]any, int, error) {
	if now.IsZero() {
		return nil, 0, fmt.Errorf("DNS comparison requires observation time")
	}
	base := make([]platformconfig.DNSIntent, 0, len(records))
	for _, r := range records {
		base = append(base, platformconfig.DNSIntent{Hostname: r.Name, Type: r.Type, Values: r.Values, TTL: r.TTL, ValueExpirations: r.ValueExpirations})
	}
	if err := platformconfig.ValidateDNSIntents(base); err != nil {
		return nil, 0, err
	}
	out := make(map[string]map[string]any, len(records))
	expired := 0
	for i, record := range records {
		if len(record.ValueExpirations) > 0 && (len(record.Candidates) > 0 || len(record.ScopedCandidates) > 0) {
			return nil, 0, fmt.Errorf("leased DNS records cannot contain dynamic candidates")
		}
		active, err := platformconfig.DNSRecordsAt([]platformconfig.DNSIntent{base[i]}, now)
		if err != nil {
			return nil, 0, err
		}
		count := 0
		if len(active) == 0 {
			record.Values = nil
		} else {
			record.Values = append([]string(nil), active[0].Values...)
			record.TTL = active[0].TTL
			count = len(record.Values)
		}
		expired += len(records[i].Values) - count
		sort.Strings(record.Values)
		record.RecordGeneration = ""
		raw, err := json.Marshal(record)
		if err != nil {
			return nil, 0, err
		}
		var fields map[string]any
		if err = json.Unmarshal(raw, &fields); err != nil {
			return nil, 0, err
		}
		delete(fields, "record_generation")
		out[record.Name+"\x00"+record.Type] = fields
	}
	return out, expired, nil
}
