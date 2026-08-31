// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stuart23/exportercreator/internal/metadata"
)

// newObservedSettings returns exporter settings whose logger records everything at or above
// level, along with the recorder.
func newObservedSettings(level zapcore.Level) (exporter.Settings, *observer.ObservedLogs) {
	core, logs := observer.New(level)
	params := exportertest.NewNopSettings(metadata.Type)
	params.Logger = zap.New(core)
	return params, logs
}

// unroutableMetrics returns metrics whose resource attributes match no routing rule.
func unroutableMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	for i := 0; i < 3; i++ {
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service", "unknown")
		dp := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		dp.SetName("test_metric")
		dp.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	}
	return metrics
}

func routingConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "app", EndpointProperty: "labels.app"}}
	cfg.DefaultExporters = []component.ID{}
	return cfg
}

// Routing telemetry that matches nothing is a routine condition, not an operator-facing event.
// At the default log level it must produce no output at all, however much telemetry flows.
func TestConsumeMetrics_QuietAtInfoLevel(t *testing.T) {
	params, logs := newObservedSettings(zapcore.InfoLevel)
	ec, err := newExporterCreator(params, routingConfig())
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		require.NoError(t, ec.ConsumeMetrics(context.Background(), unroutableMetrics()))
	}

	assert.Empty(t, logs.All(), "unmatched metrics must not log at info level")
}

// The diagnostics still exist, they have just moved to debug.
func TestConsumeMetrics_DiagnosticsAtDebugLevel(t *testing.T) {
	params, logs := newObservedSettings(zapcore.DebugLevel)
	ec, err := newExporterCreator(params, routingConfig())
	require.NoError(t, err)

	require.NoError(t, ec.ConsumeMetrics(context.Background(), unroutableMetrics()))

	require.NotEmpty(t, logs.All())
	for _, entry := range logs.All() {
		assert.Equal(t, zapcore.DebugLevel, entry.Level, "%q should be debug", entry.Message)
	}
	assert.NotEmpty(t, logs.FilterMessage("processing unmatched metrics").All(),
		"the unmatched-metrics detail should still be available at debug level")
}

// A single exporter matching every resource is the normal case and must not warn.
func TestRoute_SingleExporterDoesNotWarn(t *testing.T) {
	params, logs := newObservedSettings(zapcore.DebugLevel)
	telemetry, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	require.NoError(t, err)

	router := newTelemetryRouter([]RoutingRule{{ResourceAttribute: "app", EndpointProperty: "labels.app"}}, telemetry)
	router.setLogger(params.Logger)
	router.AddExporter("endpoint-1", &nopExporterComponent{}, map[string]any{
		"labels": map[string]string{"app": "test"},
	}, "otlp")

	attrs := pmetric.NewMetrics().ResourceMetrics().AppendEmpty().Resource().Attributes()
	attrs.PutStr("app", "test")

	for i := 0; i < 10; i++ {
		require.Len(t, router.Route(attrs), 1)
	}

	assert.Empty(t, logs.FilterMessage("all exporters matched routing rules - this may indicate a configuration issue").All(),
		"routing to the only configured exporter must not emit the all-exporters diagnostic")
}
