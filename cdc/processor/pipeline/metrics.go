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
			Buckets: []float64{1, 2, 2.5, 2.8,
				3, 3.2, 3.4, 3.6, 3.8,
				4, 4.2, 4.4, 4.6, 4.8,
				5, 5.2, 5.4, 5.6, 5.8,
				6, 6.2, 6.4, 6.6, 6.8,
				7, 7.2, 7.4, 7.6, 7.8,
				8, 8.2, 8.4, 8.6, 8.8,
				10, 14, 20, 40, 80, 160, 320},
		}, []string{"namespace", "changefeed"})

	changefeedReResolvedTsLagGauge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sorter",
			Name:      "recv_resolved_ts_lag_histogram",
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

	changefeedReBarrierTsLagGauge = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "sorter",
			Name:      "recv_barrier_ts_lag_histogram",
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
)

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(changefeedResolvedTsLagGauge)
	registry.MustRegister(changefeedReResolvedTsLagGauge)
	registry.MustRegister(changefeedReBarrierTsLagGauge)
}
