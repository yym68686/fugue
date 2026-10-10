package model

import (
	"testing"
	"time"
)

func deliveryFixture() EdgeClientNetworkSample {
	now := time.Now().UTC()
	return EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: now, TCPInfoAvailable: true,
		DeliveryBaseline: &EdgeClientDeliveryCounters{ObservedAt: now.Add(-time.Second), BytesAcked: 1000, BusyMicroseconds: 100000, DataSegmentsOut: 10},
		Delivery:         &EdgeClientDeliveryCounters{ObservedAt: now, BytesAcked: 101000, BusyMicroseconds: 600000, DataSegmentsOut: 110, RetransmittedSegments: 5, DeliveryRateBytesPerSecond: 250000}}
}

func TestPublicDeliveryExcludesApplicationIdleAndUsesMeasuredDenominator(t *testing.T) {
	sample := deliveryFixture()
	rate, loss := EdgeClientDeliveryMetrics(&sample)
	if rate == nil || *rate != 200000 || loss == nil || *loss != 0.05 {
		t.Fatal(rate, loss)
	}
	sample.DeliveryBaseline.ObservedAt = sample.DeliveryBaseline.ObservedAt.Add(-20 * time.Second)
	rate, loss = EdgeClientDeliveryMetrics(&sample)
	if rate == nil || *rate != 200000 || loss == nil || *loss != 0.05 {
		t.Fatal("idle or model wait contaminated kernel delivery", rate, loss)
	}
}

func TestPublicDeliveryUnknownNeverBecomesZeroCostOrFalseCapacity(t *testing.T) {
	for _, edit := range []func(*EdgeClientNetworkSample){
		func(sample *EdgeClientNetworkSample) { sample.Delivery.ApplicationLimited = true },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.BytesAcked = 1001 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.BusyMicroseconds = 100001 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.LastDataSentMS = 1001 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.DeliveryRateBytesPerSecond = 0 },
		func(sample *EdgeClientNetworkSample) { sample.DeliveryBaseline = nil },
	} {
		sample := deliveryFixture()
		edit(&sample)
		if rate, _ := EdgeClientDeliveryMetrics(&sample); rate != nil {
			t.Fatal("unproved delivery was treated as link capacity", *rate)
		}
	}
	for _, edit := range []func(*EdgeClientNetworkSample){
		func(sample *EdgeClientNetworkSample) { sample.Delivery.BytesAcked = 1 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.DataSegmentsOut = 1 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.BusyMicroseconds = 1 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.BusyMicroseconds = 10000000 },
		func(sample *EdgeClientNetworkSample) { sample.Delivery.ObservedAt = sample.ObservedAt.Add(time.Second) },
		func(sample *EdgeClientNetworkSample) {
			sample.DeliveryBaseline.ObservedAt = sample.ObservedAt.Add(-time.Minute)
		},
		func(sample *EdgeClientNetworkSample) { sample.TCPInfoAvailable = false },
	} {
		sample := deliveryFixture()
		edit(&sample)
		if ValidateEdgeClientNetworkSample(&sample) == nil {
			t.Fatal("accepted counter reset, impossible interval or missing kernel evidence")
		}
	}
}
