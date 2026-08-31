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

var _ observer.Notify = (*observerNotify)(nil)

// observerNotify subscribes one observable on behalf of the shared observerHandler and counts
// what that observable reports.
//
// The Notify callbacks carry endpoints and nothing else, so which observer produced an
// endpoint can only be known from the subscription that delivered it. One of these per
// watched observer is what makes the observer_type attribute possible; the endpoint handling
// itself still belongs to the single shared handler, which owns the exporter state.
type observerNotify struct {
	id           observer.NotifyID
	observerType string
	handler      *observerHandler
	telemetry    *metadata.TelemetryBuilder

	mu sync.Mutex
	// live maps every endpoint this observer currently reports to its type. OnAdd is called
	// again to resync state, so an endpoint already counted must not be counted twice.
	live map[observer.EndpointID]string
	// counts is live totalled by endpoint type. A type stays in the map once seen, at zero,
	// because a gauge only reports the series it is given: a type that stopped being written
	// would keep exporting whatever value it last held.
	counts map[string]int
}

// newObserverNotify returns the subscription to register on the observable named by
// observerID. creatorID distinguishes it from the subscriptions of other exporter_creator
// instances, since a NotifyID must be unique for each Notify.
func newObserverNotify(creatorID, observerID component.ID, handler *observerHandler, telemetry *metadata.TelemetryBuilder) *observerNotify {
	return &observerNotify{
		id:           observer.NotifyID(creatorID.String() + "/" + observerID.String()),
		observerType: observerID.Type().String(),
		handler:      handler,
		telemetry:    telemetry,
		live:         map[observer.EndpointID]string{},
		counts:       map[string]int{},
	}
}

// ID implements observer.Notify.
func (n *observerNotify) ID() observer.NotifyID {
	return n.id
}

// OnAdd implements observer.Notify.
func (n *observerNotify) OnAdd(added []observer.Endpoint) {
	n.handler.OnAdd(added)
	n.track(added)
}

// OnRemove implements observer.Notify.
func (n *observerNotify) OnRemove(removed []observer.Endpoint) {
	n.handler.OnRemove(removed)
	n.untrack(removed)
}

// OnChange implements observer.Notify.
func (n *observerNotify) OnChange(changed []observer.Endpoint) {
	n.handler.OnChange(changed)
	// A change keeps the endpoint's identity, so this is not an arrival. It is still tracked,
	// because the details it carries - and so its type - may have changed.
	n.track(changed)
}

// track records endpoints as live, ignoring those already counted.
func (n *observerNotify) track(endpoints []observer.Endpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, e := range endpoints {
		endpointType := endpointTypeOf(e)
		if previous, ok := n.live[e.ID]; ok {
			if previous == endpointType {
				continue
			}
			n.counts[previous]--
		}
		n.live[e.ID] = endpointType
		n.counts[endpointType]++
	}
	n.recordLocked()
}

// untrack drops endpoints that are no longer reported, ignoring those never counted.
func (n *observerNotify) untrack(endpoints []observer.Endpoint) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, e := range endpoints {
		previous, ok := n.live[e.ID]
		if !ok {
			continue
		}
		n.counts[previous]--
		delete(n.live, e.ID)
	}
	n.recordLocked()
}

// recordLocked reports this observer's endpoint count per endpoint type. The caller must hold
// n.mu. Every type seen so far is reported, including those now at zero.
func (n *observerNotify) recordLocked() {
	if n.telemetry == nil {
		return
	}
	for endpointType, count := range n.counts {
		n.telemetry.ExporterCreatorObservedEndpoints.Record(context.Background(), int64(count),
			metric.WithAttributes(
				attribute.String("observer_type", n.observerType),
				attribute.String("endpoint_type", endpointType),
			))
	}
}
