// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	grpcMetrics = grpc_prometheus.NewClientMetrics()

	eventFeedErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_error_count",
			Help:      "The number of error return by tikv",
		}, []string{"type"})
	eventFeedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_feed_count",
			Help:      "The number of event feed running",
		})
	scanRegionsDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "scan_regions_duration_seconds",
			Help:      "The time it took to finish a scanRegions call.",
			Buckets:   prometheus.ExponentialBuckets(0.001 /* 1 ms */, 2, 18),
		})
	eventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "event_size_bytes",
			Help:      "Size of KV events.",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 25),
		}, []string{"type"})
	pullEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "pull_event_count",
			Help:      "event count received by this puller",
		}, []string{"type", "namespace", "changefeed"})
	sendEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "send_event_count",
			Help:      "event count sent to event channel by this puller",
		}, []string{"type", "namespace", "changefeed"})
	clientChannelSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "channel_size",
			Help:      "size of each channel in kv client",
		}, []string{"channel"})
	clientRegionTokenSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "region_token",
			Help:      "size of region token in kv client",
		}, []string{"store", "namespace", "changefeed"})
	cachedRegionSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "cached_region",
			Help:      "cached region that has not requested to TiKV in kv client",
		}, []string{"store", "namespace", "changefeed"})
	batchResolvedEventSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "batch_resolved_event_size",
			Help:      "The number of region in one batch resolved ts event",
			Buckets:   prometheus.ExponentialBuckets(2, 2, 16),
		}, []string{"namespace", "changefeed"})
	changefeedResolvedTsLagGauge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kv",
			Name:      "resolved_ts_lag_histogram",
			Help:      "resolved ts lag histogram of changefeeds",
			Buckets: []float64{1, 2, 2.5, 2.8,
				3, 3.2, 3.4, 3.6, 3.8,
				4, 4.2, 4.4, 4.6, 4.8,
				5, 5.2, 5.4, 5.6, 5.8,
				6, 6.2, 6.4, 6.6, 6.8,
				7, 7.2, 7.4, 7.6, 7.8,
				8, 8.2, 8.4, 8.6, 8.8,
				10, 14, 20, 40, 80, 160, 320},
		}, []string{"namespace", "changefeed"})
	grpcPoolStreamGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "grpc_stream_count",
			Help:      "active stream count of each gRPC connection",
		}, []string{"store"})

	regionEventsBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "kvclient",
			Name:      "region_events_batch_size",
			Help:      "region events batch size",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		})
)

// InitMetrics registers all metrics in the kv package
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(eventFeedErrorCounter)
	registry.MustRegister(scanRegionsDuration)
	registry.MustRegister(eventSize)
	registry.MustRegister(eventFeedGauge)
	registry.MustRegister(pullEventCounter)
	registry.MustRegister(sendEventCounter)
	registry.MustRegister(clientChannelSize)
	registry.MustRegister(clientRegionTokenSize)
	registry.MustRegister(cachedRegionSize)
	registry.MustRegister(batchResolvedEventSize)
	registry.MustRegister(changefeedResolvedTsLagGauge)
	registry.MustRegister(grpcPoolStreamGauge)
	registry.MustRegister(regionEventsBatchSize)

	// Register client metrics to registry.
	registry.MustRegister(grpcMetrics)
}
