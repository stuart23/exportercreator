// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// endpointSeries is one series of the observed-endpoints gauge.
type endpointSeries struct {
	observerType string
	endpointType string
}

// endpointTracker counts the endpoints reported across every observer subscription.
//
// The totals are shared rather than kept per subscription because watch_observers may name
// several instances of one type - k8s_observer/cluster-a and k8s_observer/cluster-b, say -
// and they share the observer_type series. A gauge records an absolute value, so each
// subscription reporting its own count would overwrite the others instead of adding to them,
// leaving the gauge showing whichever wrote last.
type endpointTracker struct {
	telemetry *metadata.TelemetryBuilder

	mu sync.Mutex
	// live maps each subscription to the endpoints it currently reports, and each of those to
	// its type. Keyed by subscription because observers report and retract independently and
	// may each report the same endpoint id.
	live map[component.ID]map[observer.EndpointID]string
	// totals is live summed per series. A series stays in the map once seen, at zero, because
	// a gauge only reports what it is given: one that stopped being written would keep
	// exporting the value it last held.
	totals map[endpointSeries]int
}

func newEndpointTracker(telemetry *metadata.TelemetryBuilder) *endpointTracker {
	return &endpointTracker{
		telemetry: telemetry,
		live:      map[component.ID]map[observer.EndpointID]string{},
		totals:    map[endpointSeries]int{},
	}
}

// track records endpoints as currently reported by observerID. An endpoint already counted for
// that observer is not counted again, because OnAdd is also called to resync state. An endpoint
// whose type changed moves between series rather than being counted under both.
func (t *endpointTracker) track(observerID component.ID, endpoints []observer.Endpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()

	observerType := observerID.Type().String()
	live, ok := t.live[observerID]
	if !ok {
		live = map[observer.EndpointID]string{}
		t.live[observerID] = live
	}

	for _, e := range endpoints {
		endpointType := endpointTypeOf(e)
		if previous, ok := live[e.ID]; ok {
			if previous == endpointType {
				continue
			}
			t.totals[endpointSeries{observerType, previous}]--
		}
		live[e.ID] = endpointType
		t.totals[endpointSeries{observerType, endpointType}]++
	}
	t.recordLocked()
}

// untrack drops endpoints observerID no longer reports, ignoring any it never reported.
func (t *endpointTracker) untrack(observerID component.ID, endpoints []observer.Endpoint) {
	t.mu.Lock()
	defer t.mu.Unlock()

	live := t.live[observerID]
	observerType := observerID.Type().String()
	for _, e := range endpoints {
		previous, ok := live[e.ID]
		if !ok {
			continue
		}
		t.totals[endpointSeries{observerType, previous}]--
		delete(live, e.ID)
	}
	t.recordLocked()
}

// recordLocked reports every series, including those now at zero. The caller must hold t.mu.
func (t *endpointTracker) recordLocked() {
	if t.telemetry == nil {
		return
	}
	for series, count := range t.totals {
		t.telemetry.ExporterCreatorObservedEndpoints.Record(context.Background(), int64(count),
			metric.WithAttributes(
				attribute.String("observer_type", series.observerType),
				attribute.String("endpoint_type", series.endpointType),
			))
	}
}
