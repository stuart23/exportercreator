// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"go.opentelemetry.io/collector/component"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
)

var _ observer.Notify = (*observerNotify)(nil)

// observerNotify subscribes one observable on behalf of the shared observerHandler and counts
// what that observable reports.
//
// The Notify callbacks carry endpoints and nothing else, so which observer produced an endpoint
// can only be known from the subscription that delivered it. One of these per watched observer
// is what makes the observer_type attribute possible. It changes nothing about how endpoints
// are handled: every callback is passed to the one shared handler, which owns the exporter
// state, exactly as it was when that handler subscribed to each observable itself.
type observerNotify struct {
	id         observer.NotifyID
	observerID component.ID
	handler    *observerHandler
	tracker    *endpointTracker
}

// newObserverNotify returns the subscription to register on the observable named by
// observerID. creatorID distinguishes it from the subscriptions of other exporter_creator
// instances, since a NotifyID must be unique for each Notify.
func newObserverNotify(creatorID, observerID component.ID, handler *observerHandler, tracker *endpointTracker) *observerNotify {
	return &observerNotify{
		id:         observer.NotifyID(creatorID.String() + "/" + observerID.String()),
		observerID: observerID,
		handler:    handler,
		tracker:    tracker,
	}
}

// ID implements observer.Notify.
func (n *observerNotify) ID() observer.NotifyID {
	return n.id
}

// OnAdd implements observer.Notify.
func (n *observerNotify) OnAdd(added []observer.Endpoint) {
	n.handler.OnAdd(added)
	n.tracker.track(n.observerID, added)
}

// OnRemove implements observer.Notify.
func (n *observerNotify) OnRemove(removed []observer.Endpoint) {
	n.handler.OnRemove(removed)
	n.tracker.untrack(n.observerID, removed)
}

// OnChange implements observer.Notify.
func (n *observerNotify) OnChange(changed []observer.Endpoint) {
	n.handler.OnChange(changed)
	// A change keeps the endpoint's identity, so this is not an arrival. It is still tracked,
	// because the details it carries - and so its type - may have changed.
	n.tracker.track(n.observerID, changed)
}
