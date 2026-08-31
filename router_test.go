// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// nopExporterComponent is a simple exporter component for testing
type nopExporterComponent struct {
	component.StartFunc
	component.ShutdownFunc
}

func TestTelemetryRouter_AddExporter(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	router := newTelemetryRouter([]RoutingRule{}, telemetry)

	mockExporter := &nopExporterComponent{}
	env := observer.EndpointEnv{
		"labels": map[string]string{
			"app": "test",
		},
	}

	router.AddExporter(observer.EndpointID("endpoint-1"), mockExporter, env)

	assert.Equal(t, 1, router.Count())
}

func TestTelemetryRouter_RemoveExporter(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	router := newTelemetryRouter([]RoutingRule{}, telemetry)

	mockExporter := &nopExporterComponent{}
	env := observer.EndpointEnv{
		"labels": map[string]string{
			"app": "test",
		},
	}

	router.AddExporter(observer.EndpointID("endpoint-1"), mockExporter, env)
	assert.Equal(t, 1, router.Count())

	router.RemoveExporter(observer.EndpointID("endpoint-1"))
	assert.Equal(t, 0, router.Count())
}

func TestTelemetryRouter_Route(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	rules := []RoutingRule{
		{
			ResourceAttribute: "app",
			EndpointProperty:  "labels.app",
		},
	}
	router := newTelemetryRouter(rules, telemetry)

	mockExporter := &nopExporterComponent{}
	env := observer.EndpointEnv{
		"labels": map[string]string{
			"app": "test",
		},
	}

	router.AddExporter(observer.EndpointID("endpoint-1"), mockExporter, env)

	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("app", "test")
	matched := router.Route(resourceAttrs)
	assert.Len(t, matched, 1)

	// Test non-matching resource attributes
	resourceAttrs2 := pcommon.NewMap()
	resourceAttrs2.PutStr("app", "unknown")
	matched2 := router.Route(resourceAttrs2)
	assert.Len(t, matched2, 0)

	// Test no rules
	routerNoRules := newTelemetryRouter([]RoutingRule{}, telemetry)
	matched3 := routerNoRules.Route(resourceAttrs)
	assert.Nil(t, matched3)
}

func TestTelemetryRouter_MatchesAllRules(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	rules := []RoutingRule{
		{
			ResourceAttribute: "app",
			EndpointProperty:  "labels.app",
		},
		{
			ResourceAttribute: "region",
			EndpointProperty:  "labels.region",
		},
	}
	router := newTelemetryRouter(rules, telemetry)

	properties := map[string]any{
		"labels": map[string]string{
			"app":    "test",
			"region": "west",
		},
	}

	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("app", "test")
	resourceAttrs.PutStr("region", "west")

	assert.True(t, router.matchesAllRules(resourceAttrs, properties))

	// Missing one attribute
	resourceAttrs2 := pcommon.NewMap()
	resourceAttrs2.PutStr("app", "test")
	assert.False(t, router.matchesAllRules(resourceAttrs2, properties))

	// Mismatched value
	resourceAttrs3 := pcommon.NewMap()
	resourceAttrs3.PutStr("app", "test")
	resourceAttrs3.PutStr("region", "east")
	assert.False(t, router.matchesAllRules(resourceAttrs3, properties))

	// Test with no rules - should return false (defensive check)
	routerNoRules := newTelemetryRouter([]RoutingRule{}, telemetry)
	resourceAttrs4 := pcommon.NewMap()
	resourceAttrs4.PutStr("app", "test")
	assert.False(t, routerNoRules.matchesAllRules(resourceAttrs4, properties), "matchesAllRules should return false when there are no rules")
}

func TestGetNestedProperty(t *testing.T) {
	properties := map[string]any{
		"labels": map[string]string{
			"app": "test",
		},
		"spec": map[string]any{
			"region": "west",
			"nested": map[string]any{
				"value": 123,
			},
			"resourceAttributes": map[string]any{
				"generator": "alpha",
				"service":   "my-service",
			},
		},
	}

	// Test simple property
	val := getNestedProperty(properties, "labels.app")
	assert.Equal(t, "test", val)

	// Test nested property
	val2 := getNestedProperty(properties, "spec.region")
	assert.Equal(t, "west", val2)

	// Test deeply nested property
	val3 := getNestedProperty(properties, "spec.nested.value")
	assert.Equal(t, 123, val3)

	// Test spec.resourceAttributes.generator (the actual use case)
	val4 := getNestedProperty(properties, "spec.resourceAttributes.generator")
	assert.Equal(t, "alpha", val4)

	// Test spec.resourceAttributes.service
	val5 := getNestedProperty(properties, "spec.resourceAttributes.service")
	assert.Equal(t, "my-service", val5)

	// Test non-existent property
	val6 := getNestedProperty(properties, "labels.missing")
	// When accessing a non-existent key in a map, Go returns the zero value for the type
	// For map[string]string, that's an empty string, not nil
	assert.Equal(t, "", val6)

	// Test non-existent path
	val7 := getNestedProperty(properties, "missing.path")
	assert.Nil(t, val7)
}

func TestFlattenProperties(t *testing.T) {
	env := observer.EndpointEnv{
		"labels": map[string]string{
			"app": "test",
		},
		"spec": map[string]any{
			"region": "west",
		},
		"simple": "value",
	}

	flattened := flattenProperties(env)
	// flattenProperties converts EndpointEnv to map[string]any, so we check the values match
	assert.Equal(t, env["labels"], flattened["labels"])
	assert.Equal(t, env["spec"], flattened["spec"])
	assert.Equal(t, env["simple"], flattened["simple"])
}

func TestToString(t *testing.T) {
	assert.Equal(t, "test", toString("test"))
	assert.Equal(t, "123", toString(123))
	assert.Equal(t, "true", toString(true))
	assert.Equal(t, "45.67", toString(45.67))
	assert.Equal(t, "", toString(nil))
}

func TestTelemetryRouter_Route_WithCRDSpec(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	rules := []RoutingRule{
		{
			ResourceAttribute: "generator",
			EndpointProperty:  "spec.resourceAttributes.generator",
		},
	}
	router := newTelemetryRouter(rules, telemetry)

	mockExporter := &nopExporterComponent{}
	env := observer.EndpointEnv{
		"spec": map[string]any{
			"exporterType": "prometheusremotewrite",
			"resourceAttributes": map[string]any{
				"generator": "alpha",
			},
		},
	}

	router.AddExporter(observer.EndpointID("endpoint-1"), mockExporter, env)

	// Test matching resource attributes
	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("generator", "alpha")
	matched := router.Route(resourceAttrs)
	assert.Len(t, matched, 1, "Should match CRD exporter with generator=alpha")

	// Test non-matching resource attributes
	resourceAttrs2 := pcommon.NewMap()
	resourceAttrs2.PutStr("generator", "beta")
	matched2 := router.Route(resourceAttrs2)
	assert.Len(t, matched2, 0, "Should not match with generator=beta")
}

// Route logs the exporter count before it takes r.mu, so that count must come from the
// locking Count and not a bare read of r.count: observer callbacks maintain that field while
// routing reads it, and routing runs concurrently with those callbacks in production.
func TestTelemetryRouter_RouteConcurrentWithEndpointChurn(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	router := newTelemetryRouter([]RoutingRule{
		{ResourceAttribute: "k8s.pod.labels.app", EndpointProperty: "labels.app"},
	}, telemetry)
	// The debug log that reads the count is only built when a logger is set.
	router.setLogger(zap.NewNop())

	attrs := pcommon.NewMap()
	attrs.PutStr("k8s.pod.labels.app", "test")

	const iterations = 2000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			router.Route(attrs)
		}
	}()

	go func() {
		defer wg.Done()
		env := observer.EndpointEnv{"labels": map[string]string{"app": "test"}}
		for i := 0; i < iterations; i++ {
			id := observer.EndpointID(fmt.Sprintf("endpoint-%d", i))
			router.AddExporter(id, &nopExporterComponent{}, env)
			router.RemoveExporter(id)
		}
	}()

	wg.Wait()
}

// The exporter total is now cached rather than derived, so it can drift from the map it
// summarises. Every mutation path has to keep the two in step, including the ones that are
// easy to get wrong: several exporters under one endpoint, and removing an endpoint that
// was never added or was already removed.
func TestTelemetryRouter_CachedCountMatchesExporters(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	router := newTelemetryRouter([]RoutingRule{}, telemetry)

	// recount derives the total the way countLocked used to, to compare against the cache.
	recount := func() (n int) {
		router.mu.RLock()
		defer router.mu.RUnlock()
		for _, exps := range router.exporters {
			n += len(exps)
		}
		return n
	}
	requireConsistent := func(want int, step string) {
		t.Helper()
		require.Equal(t, want, recount(), "exporters map after %s", step)
		require.Equal(t, want, router.Count(), "cached count after %s", step)
	}

	env := observer.EndpointEnv{"labels": map[string]string{"app": "test"}}
	a, b := observer.EndpointID("endpoint-a"), observer.EndpointID("endpoint-b")

	requireConsistent(0, "construction")

	// Two exporters under one endpoint: the remove has to subtract both, not one.
	router.AddExporter(a, &nopExporterComponent{}, env)
	router.AddExporter(a, &nopExporterComponent{}, env)
	requireConsistent(2, "two exporters on one endpoint")

	router.AddExporter(b, &nopExporterComponent{}, env)
	requireConsistent(3, "a second endpoint")

	router.RemoveExporter(a)
	requireConsistent(1, "removing the two-exporter endpoint")

	router.RemoveExporter(a)
	requireConsistent(1, "removing an already-removed endpoint")

	router.RemoveExporter(observer.EndpointID("never-added"))
	requireConsistent(1, "removing an unknown endpoint")

	router.RemoveExporter(b)
	requireConsistent(0, "removing the last endpoint")
}
