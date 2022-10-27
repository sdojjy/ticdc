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

package pipeline

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	changefeedResolvedTsLagGauge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sorter",
			Name:      "resolved_ts_lag_histogram",
			Help:      "resolved ts lag histogram of changefeeds",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 60),
		}, []string{"namespace", "changefeed"})

	changefeedReResolvedTsLagGauge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sorter",
			Name:      "recv_resolved_ts_lag_histogram",
			Help:      "resolved ts lag histogram of changefeeds",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 60),
		}, []string{"namespace", "changefeed"})

	changefeedReBarrierTsLagGauge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sorter",
			Name:      "recv_barrier_ts_lag_histogram",
			Help:      "resolved ts lag histogram of changefeeds",
			Buckets:   prometheus.LinearBuckets(0.5, 0.5, 60),
		}, []string{"namespace", "changefeed"})
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(changefeedResolvedTsLagGauge)
	registry.MustRegister(changefeedReResolvedTsLagGauge)
	registry.MustRegister(changefeedReBarrierTsLagGauge)
}
