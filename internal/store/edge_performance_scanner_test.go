package store

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
	"reflect"
	"testing"
	"time"
)

func TestEdgePerformanceScannerReusePreservesValuesAndNulls(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	columns := make([]string, 64)
	values := make([]driver.Value, 64)
	nulls := make([]driver.Value, 64)
	for i := range columns {
		columns[i] = fmt.Sprint("c", i)
		switch {
		case i < 14 || i == 62:
			values[i] = fmt.Sprint("value-", i)
			nulls[i] = ""
		case i == 63:
			values[i] = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			nulls[i] = values[i]
		case i == 52 || i == 53 || i == 54 || i == 56 || i == 58 || i == 60:
			values[i] = float64(i) + 0.125
		default:
			values[i] = int64(i + 1)
		}
	}
	nulls[18] = int64(0)
	for _, test := range []string{"legacy", "reuse"} {
		mock.ExpectQuery(test).WillReturnRows(sqlmock.NewRows(columns).AddRow(values...).AddRow(nulls...).AddRow(values...))
	}
	read := func(query string, scan func(sqlScanner) (model.EdgePerformanceSample, error)) []model.EdgePerformanceSample {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []model.EdgePerformanceSample
		for rows.Next() {
			x, err := scan(rows)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, x)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	want := read("legacy", legacyEdgePerformanceSampleScan)
	got := read("reuse", newEdgePerformanceSampleScanner().scan)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("cursor changed field values or retained rows: want=%+v got=%+v", want, got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Frozen decoder is an independent oracle for persisted metrics and nulls.

type emptyEdgeSampleRow struct{}

func (emptyEdgeSampleRow) Scan(dest ...any) error { return nil }
func BenchmarkEdgeSampleScanDestinations(b *testing.B) {
	b.Run("per-row", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := legacyEdgePerformanceSampleScan(emptyEdgeSampleRow{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("per-cursor", func(b *testing.B) {
		scanner := newEdgePerformanceSampleScanner()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := scanner.scan(emptyEdgeSampleRow{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
func legacyEdgePerformanceSampleScan(scanner sqlScanner) (model.EdgePerformanceSample, error) {
	var sample model.EdgePerformanceSample
	var tlsHandshake, ttfb, upstream, total sql.NullInt64
	var bodyReadBlock, fileWrite, uploadEffective, minWindow, maxReadGap sql.NullInt64
	var requestBodyBytes, requestBodyReadBytes, responseWrite, responseBytes, responseEgress sql.NullInt64
	var originDNS, originConnect, originEndpointConnect, originWrite, originWait, originTTFB, originTotal sql.NullInt64
	var memoryAlloc sql.NullInt64
	var clientTCPRTT, clientTCPMinRTT, clientTCPRTTVar, clientTCPRetransRate, clientTCPBytesRetransRate, clientTCPRTORate sql.NullFloat64
	var clientTCPTotalRetrans, clientTCPBytesRetrans, clientTCPTotalRTO, clientTCPDeliveryBPS sql.NullInt64
	var sampleCount, cacheHitCount, cacheObservationCount, errorCount sql.NullInt64
	var uploadRequestCount, bodyBufferCount, bodyIncompleteCount, bodyReadErrorCount sql.NullInt64
	var streamingRequestCount, webSocketRequestCount, sseRequestCount, clientCancelCount sql.NullInt64
	var activeRequests, activeBodyBuffers, goroutineCount sql.NullInt64
	if err := scanner.Scan(
		&sample.ID,
		&sample.EdgeID,
		&sample.EdgeGroupID,
		&sample.Hostname,
		&sample.ClientCountry,
		&sample.ClientRegion,
		&sample.ClientASN,
		&sample.RuntimeRegion,
		&sample.PathPrefix,
		&sample.Method,
		&sample.TrafficClass,
		&sample.RouteGeneration,
		&sample.CacheStatus,
		&sample.DNSPolicy,
		&tlsHandshake,
		&ttfb,
		&upstream,
		&total,
		&sample.StatusCode,
		&sampleCount,
		&cacheHitCount,
		&cacheObservationCount,
		&errorCount,
		&uploadRequestCount,
		&bodyBufferCount,
		&bodyReadBlock,
		&fileWrite,
		&uploadEffective,
		&minWindow,
		&maxReadGap,
		&requestBodyBytes,
		&requestBodyReadBytes,
		&bodyIncompleteCount,
		&bodyReadErrorCount,
		&responseWrite,
		&responseBytes,
		&responseEgress,
		&originDNS,
		&originConnect,
		&originEndpointConnect,
		&originWrite,
		&originWait,
		&originTTFB,
		&originTotal,
		&streamingRequestCount,
		&webSocketRequestCount,
		&sseRequestCount,
		&clientCancelCount,
		&activeRequests,
		&activeBodyBuffers,
		&goroutineCount,
		&memoryAlloc,
		&clientTCPRTT,
		&clientTCPMinRTT,
		&clientTCPRTTVar,
		&clientTCPTotalRetrans,
		&clientTCPRetransRate,
		&clientTCPBytesRetrans,
		&clientTCPBytesRetransRate,
		&clientTCPTotalRTO,
		&clientTCPRTORate,
		&clientTCPDeliveryBPS,
		&sample.OriginFailureClass,
		&sample.SampledAt,
	); err != nil {
		return model.EdgePerformanceSample{}, fmt.Errorf("scan edge performance sample: %w", err)
	}
	sample.TLSHandshakeMS = edgePerformanceInt64FromNull(tlsHandshake)
	sample.TTFBMS = edgePerformanceInt64FromNull(ttfb)
	sample.UpstreamMS = edgePerformanceInt64FromNull(upstream)
	sample.TotalMS = edgePerformanceInt64FromNull(total)
	if sampleCount.Valid {
		sample.SampleCount = int(sampleCount.Int64)
	}
	if cacheHitCount.Valid {
		sample.CacheHitCount = int(cacheHitCount.Int64)
	}
	if cacheObservationCount.Valid {
		sample.CacheObservationCount = int(cacheObservationCount.Int64)
	}
	if errorCount.Valid {
		sample.ErrorCount = int(errorCount.Int64)
	}
	sample.BodyReadBlockMS = edgePerformanceInt64FromNull(bodyReadBlock)
	sample.FileWriteMS = edgePerformanceInt64FromNull(fileWrite)
	sample.UploadEffectiveBPS = edgePerformanceInt64FromNull(uploadEffective)
	sample.MinWindowBPS = edgePerformanceInt64FromNull(minWindow)
	sample.MaxReadGapMS = edgePerformanceInt64FromNull(maxReadGap)
	sample.RequestBodyBytes = edgePerformanceInt64FromNull(requestBodyBytes)
	sample.RequestBodyReadBytes = edgePerformanceInt64FromNull(requestBodyReadBytes)
	sample.ResponseWriteMS = edgePerformanceInt64FromNull(responseWrite)
	sample.ResponseBytes = edgePerformanceInt64FromNull(responseBytes)
	sample.ResponseEgressBPS = edgePerformanceInt64FromNull(responseEgress)
	sample.OriginDNSMS = edgePerformanceInt64FromNull(originDNS)
	sample.OriginConnectMS = edgePerformanceInt64FromNull(originConnect)
	sample.OriginEndpointConnectMS = edgePerformanceInt64FromNull(originEndpointConnect)
	sample.OriginRequestWriteMS = edgePerformanceInt64FromNull(originWrite)
	sample.OriginResponseWaitMS = edgePerformanceInt64FromNull(originWait)
	sample.OriginTTFBMS = edgePerformanceInt64FromNull(originTTFB)
	sample.OriginTotalMS = edgePerformanceInt64FromNull(originTotal)
	sample.MemoryAllocBytes = edgePerformanceInt64FromNull(memoryAlloc)
	sample.ClientTCPRTTMS = edgePerformanceFloat64FromNull(clientTCPRTT)
	sample.ClientTCPMinRTTMS = edgePerformanceFloat64FromNull(clientTCPMinRTT)
	sample.ClientTCPRTTVarMS = edgePerformanceFloat64FromNull(clientTCPRTTVar)
	sample.ClientTCPTotalRetrans = edgePerformanceInt64FromNull(clientTCPTotalRetrans)
	sample.ClientTCPRetransRate = edgePerformanceFloat64FromNull(clientTCPRetransRate)
	sample.ClientTCPBytesRetrans = edgePerformanceInt64FromNull(clientTCPBytesRetrans)
	sample.ClientTCPBytesRetransRate = edgePerformanceFloat64FromNull(clientTCPBytesRetransRate)
	sample.ClientTCPTotalRTO = edgePerformanceInt64FromNull(clientTCPTotalRTO)
	sample.ClientTCPRTORate = edgePerformanceFloat64FromNull(clientTCPRTORate)
	sample.ClientTCPDeliveryBPS = edgePerformanceInt64FromNull(clientTCPDeliveryBPS)
	if uploadRequestCount.Valid {
		sample.UploadRequestCount = int(uploadRequestCount.Int64)
	}
	if bodyBufferCount.Valid {
		sample.BodyBufferCount = int(bodyBufferCount.Int64)
	}
	if bodyIncompleteCount.Valid {
		sample.BodyIncompleteCount = int(bodyIncompleteCount.Int64)
	}
	if bodyReadErrorCount.Valid {
		sample.BodyReadErrorCount = int(bodyReadErrorCount.Int64)
	}
	if streamingRequestCount.Valid {
		sample.StreamingRequestCount = int(streamingRequestCount.Int64)
	}
	if webSocketRequestCount.Valid {
		sample.WebSocketRequestCount = int(webSocketRequestCount.Int64)
	}
	if sseRequestCount.Valid {
		sample.SSERequestCount = int(sseRequestCount.Int64)
	}
	if clientCancelCount.Valid {
		sample.ClientCancelCount = int(clientCancelCount.Int64)
	}
	if activeRequests.Valid {
		sample.ActiveRequests = int(activeRequests.Int64)
	}
	if activeBodyBuffers.Valid {
		sample.ActiveBodyBuffers = int(activeBodyBuffers.Int64)
	}
	if goroutineCount.Valid {
		sample.GoroutineCount = int(goroutineCount.Int64)
	}
	return sample, nil
}
