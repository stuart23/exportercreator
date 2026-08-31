// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
	"github.com/stuart23/exportercreator/internal/metadatatest"
)

func endpointsSeen(observerType string, byType map[string]int64) []metricdata.DataPoint[int64] {
	dps := make([]metricdata.DataPoint[int64], 0, len(byType))
	for endpointType, n := range byType {
		dps = append(dps, metricdata.DataPoint[int64]{
			Attributes: attribute.NewSet(
				attribute.String("observer_type", observerType),
				attribute.String("endpoint_type", endpointType),
			),
			Value: n,
		})
	}
	return dps
}

// newNotifies returns one subscription per observer id, sharing the handler and the counts the
// way exporter_creator wires them, so the tests exercise production's aggregation rather than
// isolated counters. The handler creates no exporters, leaving only the counting visible.
func newNotifies(t *testing.T, tt *componenttest.Telemetry, observerIDs ...component.ID) []*observerNotify {
	t.Helper()
	telemetry, err := metadata.NewTelemetryBuilder(metadatatest.NewSettings(tt).TelemetrySettings)
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	router := newTelemetryRouter(cfg.Routing.Rules, telemetry)
	handler, _ := newObserverHandler(t, cfg, router)
	tracker := newEndpointTracker(telemetry)

	notifies := make([]*observerNotify, 0, len(observerIDs))
	for _, observerID := range observerIDs {
		notifies = append(notifies, newObserverNotify(
			component.MustNewIDWithName("exporter_creator", "test"), observerID, handler, tracker))
	}
	return notifies
}

// newNotify returns a single subscription for an unnamed observer of the given type.
func newNotify(t *testing.T, tt *componenttest.Telemetry, observerType string) *observerNotify {
	t.Helper()
	return newNotifies(t, tt, component.MustNewID(observerType))[0]
}

func TestObserverNotify_CountsEndpointsByType(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{podEndpoint, portEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"pod": 1, "port": 1}),
		metricdatatest.IgnoreTimestamp())
}

// Observers call OnAdd again to resync, so an endpoint already counted must not be counted twice.
func TestObserverNotify_ResyncDoesNotDoubleCount(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newNotify(t, tt, "k8s_observer")

	for i := 0; i < 3; i++ {
		notify.OnAdd([]observer.Endpoint{podEndpoint})
	}

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"pod": 1}),
		metricdatatest.IgnoreTimestamp())
}

// A gauge only reports the series it is written, so a type that drops to zero has to be
// reported as zero rather than left holding its last value.
func TestObserverNotify_ReportsZeroAfterRemoval(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{podEndpoint, portEndpoint})
	notify.OnRemove([]observer.Endpoint{podEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"pod": 0, "port": 1}),
		metricdatatest.IgnoreTimestamp())
}

// Removing something never counted must not push the count negative.
func TestObserverNotify_UnknownRemovalIsIgnored(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{podEndpoint})
	notify.OnRemove([]observer.Endpoint{portEndpoint})
	notify.OnRemove([]observer.Endpoint{podEndpoint})
	notify.OnRemove([]observer.Endpoint{podEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"pod": 0}),
		metricdatatest.IgnoreTimestamp())
}

// Each observer gets its own subscription, which is the only way an endpoint can be attributed
// to the observer that reported it: the Notify callbacks carry endpoints and nothing else.
func TestObserverNotify_AttributesEndpointsToTheirObserver(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notifies := newNotifies(t, tt,
		component.MustNewID("k8s_observer"), component.MustNewID("host_observer"))
	k8s, host := notifies[0], notifies[1]

	k8s.OnAdd([]observer.Endpoint{podEndpoint})
	host.OnAdd([]observer.Endpoint{portEndpoint})

	dps := append(endpointsSeen("k8s_observer", map[string]int64{"pod": 1}),
		endpointsSeen("host_observer", map[string]int64{"port": 1})...)
	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt, dps, metricdatatest.IgnoreTimestamp())
}

// An endpoint keeps its identity across a change but its details, and so its type, may not.
// It has to move between series rather than be counted under both.
func TestObserverNotify_EndpointChangingTypeMovesSeries(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{podEndpoint})

	reclassified := podEndpoint
	reclassified.Details = portEndpoint.Details
	notify.OnChange([]observer.Endpoint{reclassified})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"pod": 0, "port": 1}),
		metricdatatest.IgnoreTimestamp())
}

func TestObserverNotify_IDsAreUniquePerObserver(t *testing.T) {
	tt := componenttest.NewTelemetry()
	assert.NotEqual(t, newNotify(t, tt, "k8s_observer").ID(), newNotify(t, tt, "host_observer").ID())
}

// watch_observers may name several instances of one observer type, and they share the
// observer_type series. Each subscription reporting its own absolute count would overwrite the
// others rather than add to them, leaving the gauge showing whichever wrote last.
func TestObserverNotify_NamedInstancesOfATypeSum(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notifies := newNotifies(t, tt,
		component.MustNewIDWithName("k8s_observer", "cluster-a"),
		component.MustNewIDWithName("k8s_observer", "cluster-b"))
	clusterA, clusterB := notifies[0], notifies[1]

	fromB := portEndpoint
	fromB.ID = observer.EndpointID("port-from-cluster-b")

	clusterA.OnAdd([]observer.Endpoint{portEndpoint})
	clusterB.OnAdd([]observer.Endpoint{fromB})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"port": 2}),
		metricdatatest.IgnoreTimestamp())

	// And they retract independently, without either dropping the other's count.
	clusterA.OnRemove([]observer.Endpoint{portEndpoint})
	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"port": 1}),
		metricdatatest.IgnoreTimestamp())
}

// Two observers may report the same endpoint id. Each has to be able to retract its own
// without cancelling the other's.
func TestObserverNotify_SameEndpointFromTwoObservers(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notifies := newNotifies(t, tt,
		component.MustNewIDWithName("k8s_observer", "cluster-a"),
		component.MustNewIDWithName("k8s_observer", "cluster-b"))
	clusterA, clusterB := notifies[0], notifies[1]

	clusterA.OnAdd([]observer.Endpoint{portEndpoint})
	clusterB.OnAdd([]observer.Endpoint{portEndpoint})
	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"port": 2}),
		metricdatatest.IgnoreTimestamp())

	clusterB.OnRemove([]observer.Endpoint{portEndpoint})
	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt,
		endpointsSeen("k8s_observer", map[string]int64{"port": 1}),
		metricdatatest.IgnoreTimestamp())
}

// Ownership and the handler are one decision, so a transition has to hold the tracker's lock
// across both halves. Updating them under separate locks leaves a window between the handler
// call and the ownership record: another observer can add and remove the same endpoint in
// that window, see no claim, and tear down the exporters the first observer is about to rely
// on, leaving the gauge reporting an endpoint routing can no longer reach.
//
// The window cannot be entered from outside - it exists only between two calls - so this
