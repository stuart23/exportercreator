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
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

// TestExporterCreator_ConsumeMetrics_WithWrappedExporter verifies that metrics are actually
// consumed when routed to a wrappedExporter. This test prevents regressions where the
// wrappedExporter type assertion fails silently.
func TestExporterCreator_ConsumeMetrics_WithWrappedExporter(t *testing.T) {
	// Create a mock metrics exporter that tracks consumed metrics
	consumedMetrics := make([]pmetric.Metrics, 0)
	mockMetricsExporter := &mockMetricsExporter{
		metrics: &consumedMetrics,
	}

	// Create a mock exporter factory that returns our mock exporter
	mockFactory := &flexibleMockExporterFactory{
		createMetricsExporterFunc: func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
			return mockMetricsExporter, nil
		},
		exporterType: component.MustNewType("mock"),
	}

	// Create a mock host with the factory
	mockHost := &mockHostWithFactory{
		factories: map[component.Type]exporter.Factory{
			component.MustNewType("mock"): mockFactory,
		},
	}

	// Create exporter runner and start a wrapped exporter
	params := exportertest.NewNopSettings(metadata.Type)
	runner := newExporterRunner(params, mockHost)

	exporterCfg := exporterConfig{
		id:         component.MustNewID("mock"),
		config:     userConfigMap{},
		endpointID: observer.EndpointID("test-endpoint"),
	}

	// Start the exporter - this creates a wrappedExporter
	wrappedExp, err := runner.start(exporterCfg, userConfigMap{}, exporterSignals{metrics: true})
	require.NoError(t, err)
	require.NotNil(t, wrappedExp)

	// Verify it's a wrappedExporter
	we, ok := wrappedExp.(*wrappedExporter)
	require.True(t, ok, "expected wrappedExporter")
	require.NotNil(t, we.metrics, "wrappedExporter should have metrics exporter")

	// Create exporter_creator with routing rules
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{
		{
			ResourceAttribute: "service.name",
			EndpointProperty:  "labels.service",
		},
	}

	ec, err := newExporterCreator(params, cfg)
	require.NoError(t, err)

	// Register the wrapped exporter with the router
	env := observer.EndpointEnv{
		"type":   "k8s.crd",
		"labels": map[string]string{"service": "test-service"},
	}
	ec.router.AddExporter(observer.EndpointID("test-endpoint"), wrappedExp, env, "otlp")

	// Create metrics with matching resource attributes
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	// Add a metric with data points
	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test_metric")
	metric.SetEmptyGauge()
	dp := metric.Gauge().DataPoints().AppendEmpty()
	dp.SetIntValue(42)

	// Consume metrics - this should route to the wrapped exporter and actually consume them
	err = ec.ConsumeMetrics(context.Background(), metrics)
	require.NoError(t, err)

	// Verify metrics were actually consumed by the wrapped exporter
	require.Len(t, consumedMetrics, 1, "metrics should have been consumed by wrapped exporter")
	assert.Equal(t, int64(42), consumedMetrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).IntValue())
}

// TestExporterCreator_ConsumeLogs_WithWrappedExporter verifies that logs are actually
// consumed when routed to a wrappedExporter.
func TestExporterCreator_ConsumeLogs_WithWrappedExporter(t *testing.T) {
	// Create a mock logs exporter that tracks consumed logs
	consumedLogs := make([]plog.Logs, 0)
	mockLogsExporter := &mockLogsExporter{
		logs: &consumedLogs,
	}

	// Create a mock exporter factory
	mockFactory := &flexibleMockExporterFactory{
		createLogsExporterFunc: func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
			return mockLogsExporter, nil
		},
		exporterType: component.MustNewType("mock"),
	}

	mockHost := &mockHostWithFactory{
		factories: map[component.Type]exporter.Factory{
			component.MustNewType("mock"): mockFactory,
		},
	}

	params := exportertest.NewNopSettings(metadata.Type)
	runner := newExporterRunner(params, mockHost)

	exporterCfg := exporterConfig{
		id:         component.MustNewID("mock"),
		config:     userConfigMap{},
		endpointID: observer.EndpointID("test-endpoint"),
	}

	wrappedExp, err := runner.start(exporterCfg, userConfigMap{}, exporterSignals{logs: true})
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{
		{
			ResourceAttribute: "service.name",
			EndpointProperty:  "labels.service",
		},
	}

	ec, err := newExporterCreator(params, cfg)
	require.NoError(t, err)

	env := observer.EndpointEnv{
		"type":   "k8s.crd",
		"labels": map[string]string{"service": "test-service"},
	}
	ec.router.AddExporter(observer.EndpointID("test-endpoint"), wrappedExp, env, "otlp")

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("test log")

	err = ec.ConsumeLogs(context.Background(), logs)
	require.NoError(t, err)

	require.Len(t, consumedLogs, 1, "logs should have been consumed by wrapped exporter")
}

// TestExporterCreator_ConsumeTraces_WithWrappedExporter verifies that traces are actually
// consumed when routed to a wrappedExporter.
func TestExporterCreator_ConsumeTraces_WithWrappedExporter(t *testing.T) {
	// Create a mock traces exporter that tracks consumed traces
	consumedTraces := make([]ptrace.Traces, 0)
	mockTracesExporter := &mockTracesExporter{
		traces: &consumedTraces,
	}

	// Create a mock exporter factory
	mockFactory := &flexibleMockExporterFactory{
		createTracesExporterFunc: func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
			return mockTracesExporter, nil
		},
		exporterType: component.MustNewType("mock"),
	}

	mockHost := &mockHostWithFactory{
		factories: map[component.Type]exporter.Factory{
			component.MustNewType("mock"): mockFactory,
		},
	}

	params := exportertest.NewNopSettings(metadata.Type)
	runner := newExporterRunner(params, mockHost)

	exporterCfg := exporterConfig{
		id:         component.MustNewID("mock"),
		config:     userConfigMap{},
		endpointID: observer.EndpointID("test-endpoint"),
	}

	wrappedExp, err := runner.start(exporterCfg, userConfigMap{}, exporterSignals{traces: true})
	require.NoError(t, err)

	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{
		{
			ResourceAttribute: "service.name",
			EndpointProperty:  "labels.service",
		},
	}

	ec, err := newExporterCreator(params, cfg)
	require.NoError(t, err)

	env := observer.EndpointEnv{
		"type":   "k8s.crd",
		"labels": map[string]string{"service": "test-service"},
	}
	ec.router.AddExporter(observer.EndpointID("test-endpoint"), wrappedExp, env, "otlp")

	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-service")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("test-span")

	err = ec.ConsumeTraces(context.Background(), traces)
	require.NoError(t, err)

	require.Len(t, consumedTraces, 1, "traces should have been consumed by wrapped exporter")
}

// mockMetricsExporter is a mock exporter that tracks consumed metrics
type mockMetricsExporter struct {
	exporter.Metrics
	metrics *[]pmetric.Metrics
}

func (m *mockMetricsExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	*m.metrics = append(*m.metrics, md)
	return nil
}

func (m *mockMetricsExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m *mockMetricsExporter) Shutdown(ctx context.Context) error {
	return nil
}

// mockLogsExporter is a mock exporter that tracks consumed logs
type mockLogsExporter struct {
	exporter.Logs
	logs *[]plog.Logs
}

func (m *mockLogsExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	*m.logs = append(*m.logs, ld)
	return nil
}

func (m *mockLogsExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m *mockLogsExporter) Shutdown(ctx context.Context) error {
	return nil
}

// mockTracesExporter is a mock exporter that tracks consumed traces
type mockTracesExporter struct {
	exporter.Traces
	traces *[]ptrace.Traces
}

func (m *mockTracesExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	*m.traces = append(*m.traces, td)
	return nil
}

func (m *mockTracesExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m *mockTracesExporter) Shutdown(ctx context.Context) error {
	return nil
}

// flexibleMockExporterFactory is a mock factory that allows customizing the create functions
type flexibleMockExporterFactory struct {
	exporter.Factory
	exporterType              component.Type
	createLogsExporterFunc    func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error)
	createMetricsExporterFunc func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error)
	createTracesExporterFunc  func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error)
}

func (m *flexibleMockExporterFactory) Type() component.Type {
	return m.exporterType
}

func (m *flexibleMockExporterFactory) CreateDefaultConfig() component.Config {
	return &mockExporterConfig{}
}

func (m *flexibleMockExporterFactory) CreateLogs(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	if m.createLogsExporterFunc != nil {
		return m.createLogsExporterFunc(ctx, set, cfg)
	}
	return newNopExporter(), nil
}

func (m *flexibleMockExporterFactory) CreateMetrics(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	if m.createMetricsExporterFunc != nil {
		return m.createMetricsExporterFunc(ctx, set, cfg)
	}
	return newNopExporter(), nil
}

func (m *flexibleMockExporterFactory) CreateTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	if m.createTracesExporterFunc != nil {
		return m.createTracesExporterFunc(ctx, set, cfg)
	}
	return newNopExporter(), nil
}
