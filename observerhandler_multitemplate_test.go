// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// twoTemplateConfig returns a config whose two templates both match a port endpoint.
func twoTemplateConfig(t *testing.T) *Config {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	rule, err := newRule(`type == "port"`)
	require.NoError(t, err)

	// A rule matching the endpoint's own pod label, so Route does real matching. With no
	// rules it returns nil before looking at the exporters at all, and a regression that
	// registered both exporters but routed to only one would still pass.
	cfg.Routing.Rules = []RoutingRule{
		{ResourceAttribute: "k8s.pod.labels.app", EndpointProperty: "pod.labels.app"},
	}

	cfg.exporterTemplates = map[string]exporterTemplate{}
	for _, name := range []string{"a", "b"} {
		id := component.MustNewIDWithName("otlp", name)
		cfg.exporterTemplates[id.String()] = exporterTemplate{
			exporterConfig: exporterConfig{
				id:         id,
				config:     userConfigMap{"endpoint": "localhost:4317"},
				endpointID: portEndpoint.ID,
			},
			rule:               rule,
			Rule:               `type == "port"`,
			ResourceAttributes: map[string]any{},
			signals:            exporterSignals{metrics: true, logs: true, traces: true},
		}
	}
	return cfg
}

func newTwoTemplateHandler(t *testing.T) (*observerHandler, *mockRunner, *telemetryRouter) {
	t.Helper()
	cfg := twoTemplateConfig(t)
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	router := newTelemetryRouter(cfg.Routing.Rules, telemetry)
	handler, mr := newObserverHandler(t, cfg, router)
	return handler, mr, router
}

// An endpoint matching several templates must produce a live exporter for each: all of them
// routable, and all of them shut down when the endpoint goes away. Tracking them by endpoint
// alone overwrote every exporter but the last, stranding the others - started, never routed
// to, and never stopped.
func TestObserverHandler_MultipleTemplatesPerEndpoint(t *testing.T) {
	handler, mr, router := newTwoTemplateHandler(t)

	handler.OnAdd([]observer.Endpoint{portEndpoint})
	require.Len(t, mr.startedComponents, 2, "both templates should start an exporter")
	assert.Equal(t, 2, router.Count(), "both exporters should be registered for routing")

	// Registration is only half of it: telemetry has to actually reach both.
	assert.ElementsMatch(t, mr.startedComponents, router.Route(podAppAttrs("redis")),
		"routing must reach every exporter the endpoint created, not just one")

	handler.OnRemove([]observer.Endpoint{portEndpoint})
	assert.ElementsMatch(t, mr.startedComponents, mr.shutdownComponents,
		"every started exporter must be shut down")
	assert.Equal(t, 0, router.Count())
	assert.Empty(t, router.Route(podAppAttrs("redis")),
		"a removed endpoint's exporters must no longer be routed to")
}

// podAppAttrs builds resource attributes carrying the pod label the routing rule matches on.
func podAppAttrs(app string) pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("k8s.pod.labels.app", app)
	return attrs
}

// Endpoint churn must not strand exporters. OnChange is a remove followed by an add, so this
// is also the update path.
func TestObserverHandler_ChurnDoesNotStrandExporters(t *testing.T) {
	handler, mr, router := newTwoTemplateHandler(t)

	for i := 0; i < 20; i++ {
		handler.OnAdd([]observer.Endpoint{portEndpoint})
		handler.OnRemove([]observer.Endpoint{portEndpoint})
	}

	assert.Equal(t, 0, router.Count())
	assert.Len(t, mr.shutdownComponents, len(mr.startedComponents),
		"every exporter started during churn must have been shut down")
}

// shutdown must stop every exporter, including the extras beyond the first per endpoint.
func TestObserverHandler_ShutdownStopsEveryExporter(t *testing.T) {
	handler, mr, _ := newTwoTemplateHandler(t)

	handler.OnAdd([]observer.Endpoint{portEndpoint})
	require.Len(t, mr.startedComponents, 2)

	require.NoError(t, handler.shutdown())
	assert.ElementsMatch(t, mr.startedComponents, mr.shutdownComponents)
}

// soleExporter returns the single exporter created for an endpoint, failing the test if the
// endpoint produced anything other than exactly one.
func soleExporter(t *testing.T, handler *observerHandler, id observer.EndpointID) component.Component {
	t.Helper()
	exps := handler.exportersByEndpoint.Get(id)
	require.Len(t, exps, 1, "expected exactly one exporter for endpoint %q", id)
	return exps[0]
}
