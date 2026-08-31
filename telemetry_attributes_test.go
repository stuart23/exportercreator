// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

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

func exporterTypeAttrs(exporterType string) attribute.Set {
	return attribute.NewSet(attribute.String("exporter_type", exporterType))
}

func endpointAttrs(observerType, endpointType string) attribute.Set {
	return attribute.NewSet(
		attribute.String("observer_type", observerType),
		attribute.String("endpoint_type", endpointType),
	)
}

// The exporter count is reported per exporter type, so a deployment running several templates
// can see which kind of exporter the endpoints resolved to rather than one opaque total.
func TestExportersCount_IsReportedPerExporterType(t *testing.T) {
	tt := componenttest.NewTelemetry()
	telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	defer telemetry.Shutdown()
	router := newTelemetryRouter([]RoutingRule{}, telemetry)

	env := observer.EndpointEnv{"labels": map[string]string{"app": "test"}}
	router.AddExporter("otlp-1", &nopExporterComponent{}, env, "otlp")
	router.AddExporter("otlp-2", &nopExporterComponent{}, env, "otlp")
	router.AddExporter("prw-1", &nopExporterComponent{}, env, "prometheusremotewrite")

	metadatatest.AssertEqualExporterCreatorExportersCount(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: exporterTypeAttrs("otlp"), Value: 2},
		{Attributes: exporterTypeAttrs("prometheusremotewrite"), Value: 1},
	}, metricdatatest.IgnoreTimestamp())
}

// A gauge only reports the series it is written. A type whose last exporter goes away has to
// be written as zero, or it keeps exporting the count it had when it was last populated.
func TestExportersCount_EmptiedTypeReportsZero(t *testing.T) {
	tt := componenttest.NewTelemetry()
	telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	defer telemetry.Shutdown()
	router := newTelemetryRouter([]RoutingRule{}, telemetry)

	env := observer.EndpointEnv{"labels": map[string]string{"app": "test"}}
	router.AddExporter("otlp-1", &nopExporterComponent{}, env, "otlp")
	router.AddExporter("prw-1", &nopExporterComponent{}, env, "prometheusremotewrite")
	router.RemoveExporter("prw-1")

	metadatatest.AssertEqualExporterCreatorExportersCount(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: exporterTypeAttrs("otlp"), Value: 1},
		{Attributes: exporterTypeAttrs("prometheusremotewrite"), Value: 0},
	}, metricdatatest.IgnoreTimestamp())
}

// One endpoint matching two templates of different types counts against both.
func TestExportersCount_OneEndpointSeveralTypes(t *testing.T) {
	tt := componenttest.NewTelemetry()
	telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	defer telemetry.Shutdown()
	router := newTelemetryRouter([]RoutingRule{}, telemetry)

	env := observer.EndpointEnv{"labels": map[string]string{"app": "test"}}
	router.AddExporter("shared", &nopExporterComponent{}, env, "otlp")
	router.AddExporter("shared", &nopExporterComponent{}, env, "prometheusremotewrite")

	metadatatest.AssertEqualExporterCreatorExportersCount(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: exporterTypeAttrs("otlp"), Value: 1},
		{Attributes: exporterTypeAttrs("prometheusremotewrite"), Value: 1},
	}, metricdatatest.IgnoreTimestamp())

	// Removing the endpoint drops both, and each type is written back down to zero.
	router.RemoveExporter("shared")
	metadatatest.AssertEqualExporterCreatorExportersCount(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: exporterTypeAttrs("otlp"), Value: 0},
		{Attributes: exporterTypeAttrs("prometheusremotewrite"), Value: 0},
	}, metricdatatest.IgnoreTimestamp())
}

// newTestNotify returns a subscription for the named observer, wired to a working handler.
func newTestNotify(t *testing.T, tt *componenttest.Telemetry, observerType string) *observerNotify {
	t.Helper()
	telemetry, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	t.Cleanup(telemetry.Shutdown)

	cfg := createDefaultConfig().(*Config)
	router := newTelemetryRouter(cfg.Routing.Rules, telemetry)
	handler, _ := newObserverHandler(t, cfg, router)
	return newObserverNotify(
		component.MustNewIDWithName("exporter_creator", "test"),
		component.MustNewID(observerType),
		handler, telemetry)
}

// Endpoints are counted against the observer that reported them and the kind of resource they
// describe. The callbacks carry neither, so both come from the subscription and the endpoint.
func TestObservedEndpoints_CountedByObserverAndEndpointType(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newTestNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{portEndpoint, podEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: endpointAttrs("k8s_observer", "port"), Value: 1},
		{Attributes: endpointAttrs("k8s_observer", "pod"), Value: 1},
	}, metricdatatest.IgnoreTimestamp())
}

// Removal takes the count back down, and an endpoint type with nothing left reports zero
// rather than leaving its last value standing.
func TestObservedEndpoints_RemovalReportsZero(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newTestNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{portEndpoint})
	notify.OnRemove([]observer.Endpoint{portEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: endpointAttrs("k8s_observer", "port"), Value: 0},
	}, metricdatatest.IgnoreTimestamp())
}

// OnAdd is called again to resync state, so the same endpoint arriving twice is still one
// endpoint. Counting each call would make the gauge climb for as long as the collector runs.
func TestObservedEndpoints_ResyncDoesNotDoubleCount(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newTestNotify(t, tt, "host_observer")

	for i := 0; i < 5; i++ {
		notify.OnAdd([]observer.Endpoint{hostportEndpoint})
	}

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: endpointAttrs("host_observer", "hostport"), Value: 1},
	}, metricdatatest.IgnoreTimestamp())
}

// Removing an endpoint that was never reported must not push the count negative.
func TestObservedEndpoints_UnknownRemovalIsIgnored(t *testing.T) {
	tt := componenttest.NewTelemetry()
	notify := newTestNotify(t, tt, "k8s_observer")

	notify.OnAdd([]observer.Endpoint{portEndpoint})
	notify.OnRemove([]observer.Endpoint{podEndpoint})
	notify.OnRemove([]observer.Endpoint{portEndpoint})
	notify.OnRemove([]observer.Endpoint{portEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: endpointAttrs("k8s_observer", "port"), Value: 0},
	}, metricdatatest.IgnoreTimestamp())
}

// Two observers reporting endpoints keep separate series, which is the point of the attribute.
func TestObservedEndpoints_ObserversCountedSeparately(t *testing.T) {
	tt := componenttest.NewTelemetry()
	k8s := newTestNotify(t, tt, "k8s_observer")
	host := newTestNotify(t, tt, "host_observer")

	k8s.OnAdd([]observer.Endpoint{portEndpoint})
	host.OnAdd([]observer.Endpoint{hostportEndpoint})

	metadatatest.AssertEqualExporterCreatorObservedEndpoints(t, tt, []metricdata.DataPoint[int64]{
		{Attributes: endpointAttrs("k8s_observer", "port"), Value: 1},
		{Attributes: endpointAttrs("host_observer", "hostport"), Value: 1},
	}, metricdatatest.IgnoreTimestamp())
}
