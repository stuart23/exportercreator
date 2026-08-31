// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// TestTelemetryRouter_Route_MismatchedGenerator verifies that metrics with a generator
// attribute that doesn't match the CRD's resourceAttributes.generator are NOT routed.
func TestTelemetryRouter_Route_MismatchedGenerator(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	// Create routing rule: generator attribute should match spec.resourceAttributes.generator
	rules := []RoutingRule{
		{
			ResourceAttribute: "generator",
			EndpointProperty:  "spec.resourceAttributes.generator",
		},
	}
	router := newTelemetryRouter(rules, telemetry)

	// Create exporter with CRD that has generator="alpha"
	properties := map[string]any{
		"spec": map[string]any{
			"resourceAttributes": map[string]any{
				"generator": "alpha",
			},
		},
	}

	mockExporter := &nopExporter{}
	router.AddExporter(observer.EndpointID("crd-alpha"), mockExporter, observer.EndpointEnv(properties), "otlp")

	// Test 1: Metric with generator="alpha" should match
	resourceAttrs1 := pcommon.NewMap()
	resourceAttrs1.PutStr("generator", "alpha")
	matched1 := router.Route(resourceAttrs1)
	assert.Len(t, matched1, 1, "Metrics with matching generator should be routed")
	assert.Equal(t, mockExporter, matched1[0])

	// Test 2: Metric with generator="beta" should NOT match
	resourceAttrs2 := pcommon.NewMap()
	resourceAttrs2.PutStr("generator", "beta")
	matched2 := router.Route(resourceAttrs2)
	assert.Len(t, matched2, 0, "Metrics with non-matching generator should NOT be routed")

	// Test 3: Metric with generator="gamma" should NOT match
	resourceAttrs3 := pcommon.NewMap()
	resourceAttrs3.PutStr("generator", "gamma")
	matched3 := router.Route(resourceAttrs3)
	assert.Len(t, matched3, 0, "Metrics with non-matching generator should NOT be routed")

	// Test 4: Metric without generator attribute should NOT match
	resourceAttrs4 := pcommon.NewMap()
	resourceAttrs4.PutStr("service.name", "my-service")
	matched4 := router.Route(resourceAttrs4)
	assert.Len(t, matched4, 0, "Metrics without generator attribute should NOT be routed")
}

// TestTelemetryRouter_Route_MultipleCRDs verifies that metrics are routed to the correct
// CRD exporter based on generator value when multiple CRDs exist.
func TestTelemetryRouter_Route_MultipleCRDs(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	rules := []RoutingRule{
		{
			ResourceAttribute: "generator",
			EndpointProperty:  "spec.resourceAttributes.generator",
		},
	}
	router := newTelemetryRouter(rules, telemetry)

	// Create two exporters with different generator values
	propertiesAlpha := map[string]any{
		"spec": map[string]any{
			"resourceAttributes": map[string]any{
				"generator": "alpha",
			},
		},
	}
	propertiesBeta := map[string]any{
		"spec": map[string]any{
			"resourceAttributes": map[string]any{
				"generator": "beta",
			},
		},
	}

	mockExporterAlpha := &nopExporter{}
	mockExporterBeta := &nopExporter{}
	router.AddExporter(observer.EndpointID("crd-alpha"), mockExporterAlpha, observer.EndpointEnv(propertiesAlpha), "otlp")
	router.AddExporter(observer.EndpointID("crd-beta"), mockExporterBeta, observer.EndpointEnv(propertiesBeta), "otlp")

	// Test: Metric with generator="alpha" should only match alpha exporter
	resourceAttrs1 := pcommon.NewMap()
	resourceAttrs1.PutStr("generator", "alpha")
	matched1 := router.Route(resourceAttrs1)
	assert.Len(t, matched1, 1, "Should match exactly one exporter")
	assert.Equal(t, mockExporterAlpha, matched1[0])

	// Test: Metric with generator="beta" should only match beta exporter
	resourceAttrs2 := pcommon.NewMap()
	resourceAttrs2.PutStr("generator", "beta")
	matched2 := router.Route(resourceAttrs2)
	assert.Len(t, matched2, 1, "Should match exactly one exporter")
	assert.Equal(t, mockExporterBeta, matched2[0])

	// Test: Metric with generator="gamma" should match neither
	resourceAttrs3 := pcommon.NewMap()
	resourceAttrs3.PutStr("generator", "gamma")
	matched3 := router.Route(resourceAttrs3)
	assert.Len(t, matched3, 0, "Should not match any exporter")
}

// TestMatchesAllRules_MismatchedGenerator verifies the matching logic directly.
func TestMatchesAllRules_MismatchedGenerator(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	rules := []RoutingRule{
		{
			ResourceAttribute: "generator",
			EndpointProperty:  "spec.resourceAttributes.generator",
		},
	}
	router := newTelemetryRouter(rules, telemetry)

	properties := map[string]any{
		"spec": map[string]any{
			"resourceAttributes": map[string]any{
				"generator": "alpha",
			},
		},
	}

	// Test matching value
	resourceAttrs1 := pcommon.NewMap()
	resourceAttrs1.PutStr("generator", "alpha")
	assert.True(t, router.matchesAllRules(resourceAttrs1, properties), "Matching values should return true")

	// Test non-matching value
	resourceAttrs2 := pcommon.NewMap()
	resourceAttrs2.PutStr("generator", "beta")
	assert.False(t, router.matchesAllRules(resourceAttrs2, properties), "Non-matching values should return false")

	// Test missing attribute
	resourceAttrs3 := pcommon.NewMap()
	resourceAttrs3.PutStr("service.name", "my-service")
	assert.False(t, router.matchesAllRules(resourceAttrs3, properties), "Missing attribute should return false")
}

// TestTelemetryRouter_Route_NoRules verifies that when no routing rules are configured,
// no exporters are matched (to prevent all metrics from going to all exporters).
func TestTelemetryRouter_Route_NoRules(t *testing.T) {
	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	// Create router with NO routing rules
	rules := []RoutingRule{}
	router := newTelemetryRouter(rules, telemetry)

	// Add an exporter
	properties := map[string]any{
		"spec": map[string]any{
			"resourceAttributes": map[string]any{
				"generator": "alpha",
			},
		},
	}
	mockExporter := &nopExporter{}
	router.AddExporter(observer.EndpointID("crd-alpha"), mockExporter, observer.EndpointEnv(properties), "otlp")

	// Test: Even with matching attributes, no exporters should be matched when no rules are configured
	resourceAttrs := pcommon.NewMap()
	resourceAttrs.PutStr("generator", "alpha")
	matched := router.Route(resourceAttrs)
	assert.Len(t, matched, 0, "No exporters should be matched when no routing rules are configured")
}
