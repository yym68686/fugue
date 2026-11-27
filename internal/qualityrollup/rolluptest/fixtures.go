// Package rolluptest provides synthetic edge observations for rollup and DNS tests.
package rolluptest

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func PostgresStore(b testing.TB, samples []model.EdgePerformanceSample) *store.Store {
	b.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		b.Skip("set FUGUE_TEST_DATABASE_URL to run Postgres rollup benchmark")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		b.Fatal("refusing non-disposable Postgres benchmark target")
	}
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		b.Fatal(err)
	}
	schemaName := strings.ReplaceAll(model.NewID("rollup_benchmark"), "-", "_")
	baseDB, err := sql.Open("pgx", databaseURL)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := baseDB.ExecContext(context.Background(), `CREATE SCHEMA "`+schemaName+`"`); err != nil {
		_ = baseDB.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_, _ = baseDB.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		_ = baseDB.Close()
	})
	query := parsed.Query()
	query.Set("search_path", schemaName)
	parsed.RawQuery = query.Encode()
	stateStore := store.New("", parsed.String())
	if err := stateStore.RecordEdgePerformanceSamples(samples, time.Time{}); err != nil {
		b.Fatal(err)
	}
	return stateStore
}

func Samples(count int, now time.Time, hostnameCount int) []model.EdgePerformanceSample {
	if hostnameCount <= 0 {
		hostnameCount = 1
	}
	samples := make([]model.EdgePerformanceSample, 0, count+2)
	for index := 0; index < count; index++ {
		sampledAt := now.Add(-time.Duration(index%1440+1) * time.Minute)
		samples = append(samples, model.EdgePerformanceSample{
			ID:                        fmt.Sprintf("sample-%06d", index),
			EdgeID:                    fmt.Sprintf("edge-%d", index%6),
			EdgeGroupID:               fmt.Sprintf("edge-group-%d", index%3),
			Hostname:                  fmt.Sprintf("service-%d.example.test", index%hostnameCount),
			PathPrefix:                fmt.Sprintf("/api/v%d/items", index%4),
			Method:                    []string{"GET", "post"}[index%2],
			TrafficClass:              []string{"dynamic_api", "static_cacheable", "large_body_api"}[index%3],
			ClientCountry:             []string{"us", "de", "hk", ""}[index%4],
			ClientRegion:              []string{"ca", "he", "hk", ""}[index%4],
			ClientASN:                 []string{"as64500", "as64501", "", "as64503"}[index%4],
			TTFBMS:                    int64(1 + index%997),
			UpstreamMS:                int64(1 + index%701),
			TotalMS:                   int64(1 + index%1201),
			SampleCount:               1 + index%16,
			CacheHitCount:             index % 13,
			CacheObservationCount:     16,
			ErrorCount:                index % 3,
			UploadEffectiveBPS:        int64(1024 + (index%97)*4096),
			MinWindowBPS:              int64(512 + (index%89)*2048),
			MaxReadGapMS:              int64(1 + index%503),
			BodyReadBlockMS:           int64(1 + index%401),
			BodyIncompleteCount:       index % 2,
			BodyReadErrorCount:        index % 5,
			ResponseWriteMS:           int64(1 + index%607),
			ResponseEgressBPS:         int64(2048 + (index%101)*8192),
			OriginDNSMS:               int64(1 + index%31),
			OriginConnectMS:           int64(1 + index%67),
			OriginRequestWriteMS:      int64(1 + index%83),
			OriginResponseWaitMS:      int64(1 + index%127),
			OriginTTFBMS:              int64(1 + index%181),
			OriginTotalMS:             int64(1 + index%251),
			ClientCancelCount:         index % 2,
			ClientTCPRTTMS:            float64(1+index%211) / 3,
			ClientTCPMinRTTMS:         float64(1+index%173) / 4,
			ClientTCPRTTVarMS:         float64(1+index%109) / 5,
			ClientTCPRetransRate:      float64(index%17) / 100,
			ClientTCPBytesRetransRate: float64(index%19) / 100,
			ClientTCPRTORate:          float64(index%11) / 100,
			ClientTCPDeliveryBPS:      int64(4096 + (index%103)*16384),
			ActiveRequests:            1 + index%23,
			ActiveBodyBuffers:         1 + index%7,
			GoroutineCount:            10 + index%101,
			MemoryAllocBytes:          int64(1024 * (1 + index%1009)),
			SampledAt:                 sampledAt,
		})
	}
	sort.Slice(samples, func(i, j int) bool {
		if !samples[i].SampledAt.Equal(samples[j].SampledAt) {
			return samples[i].SampledAt.Before(samples[j].SampledAt)
		}
		if samples[i].Hostname != samples[j].Hostname {
			return samples[i].Hostname < samples[j].Hostname
		}
		if samples[i].EdgeGroupID != samples[j].EdgeGroupID {
			return samples[i].EdgeGroupID < samples[j].EdgeGroupID
		}
		return samples[i].ID < samples[j].ID
	})
	return samples
}
