// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap/zapcore"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadatatest"
)

// metricsOnlyCreator returns a creator whose single endpoint resolves to a metrics-only
// exporter, as a prometheusremotewrite template would produce.
func metricsOnlyCreator(t *testing.T, tt *componenttest.Telemetry) *exporterCreator {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}

	ec, err := newExporterCreator(metadatatest.NewSettings(tt), cfg)
	require.NoError(t, err)
	ec.router.AddExporter("prw-endpoint", &wrappedExporter{metrics: &nopExporter{}},
		observer.EndpointEnv{"labels": map[string]string{"service": "svc"}})
	return ec
}

func matchingLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rl.ScopeLogs().At(0).LogRecords().AppendEmpty()
	return ld
}

func matchingTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	return td
}

// Logs matching an endpoint whose exporter only handles metrics used to be discarded in
// silence: not delivered, not passed to default_exporters, and not counted anywhere.
func TestUnsupportedSignal_LogsAreCounted(t *testing.T) {
	tt := componenttest.NewTelemetry()
	ec := metricsOnlyCreator(t, tt)

	require.NoError(t, ec.ConsumeLogs(context.Background(), matchingLogs()))

	metadatatest.AssertEqualExporterCreatorNonroutableLogRecordsTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 2}}, metricdatatest.IgnoreTimestamp())
}

func TestUnsupportedSignal_SpansAreCounted(t *testing.T) {
	tt := componenttest.NewTelemetry()
	ec := metricsOnlyCreator(t, tt)

	require.NoError(t, ec.ConsumeTraces(context.Background(), matchingTraces()))

	metadatatest.AssertEqualExporterCreatorNonroutableSpansTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 1}}, metricdatatest.IgnoreTimestamp())
}

// The same silent drop exists for metrics when the matched exporter handles only logs.
func TestUnsupportedSignal_MetricPointsAreCounted(t *testing.T) {
	tt := componenttest.NewTelemetry()
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}
	ec, err := newExporterCreator(metadatatest.NewSettings(tt), cfg)
	require.NoError(t, err)
	ec.router.AddExporter("logs-endpoint", &wrappedExporter{logs: &nopExporter{}},
		observer.EndpointEnv{"labels": map[string]string{"service": "svc"}})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	g := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("m")
	g.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	g.Gauge().DataPoints().AppendEmpty().SetIntValue(2)

	require.NoError(t, ec.ConsumeMetrics(context.Background(), md))

	metadatatest.AssertEqualExporterCreatorNonroutableMetricPointsTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 2}}, metricdatatest.IgnoreTimestamp())
}

// The mismatch is a static property of the exporter, so it is reported once rather than on
// every export.
func TestUnsupportedSignal_WarnsOncePerExporter(t *testing.T) {
	params, logs := newObservedSettings(zapcore.WarnLevel)
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}
	ec, err := newExporterCreator(params, cfg)
	require.NoError(t, err)
	ec.router.AddExporter("prw-endpoint", &wrappedExporter{metrics: &nopExporter{}},
		observer.EndpointEnv{"labels": map[string]string{"service": "svc"}})

	for i := 0; i < 10; i++ {
		require.NoError(t, ec.ConsumeLogs(context.Background(), matchingLogs()))
		require.NoError(t, ec.ConsumeTraces(context.Background(), matchingTraces()))
	}

	warns := logs.FilterMessage("matched exporter does not handle this signal, dropping telemetry").All()
	require.Len(t, warns, 2, "one warning per signal, not per export")
	assert.ElementsMatch(t, []string{"logs", "traces"},
		[]string{warns[0].ContextMap()["signal"].(string), warns[1].ContextMap()["signal"].(string)})
}

// A resource delivered to at least one exporter is not counted as dropped, even when another
// matched exporter cannot handle it.
func TestUnsupportedSignal_NotCountedWhenAnotherExporterAccepts(t *testing.T) {
	tt := componenttest.NewTelemetry()
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}
	ec, err := newExporterCreator(metadatatest.NewSettings(tt), cfg)
	require.NoError(t, err)
	env := observer.EndpointEnv{"labels": map[string]string{"service": "svc"}}
	ec.router.AddExporter("metrics-only", &wrappedExporter{metrics: &nopExporter{}}, env)
	ec.router.AddExporter("logs-capable", &wrappedExporter{logs: &nopExporter{}}, env)

	require.NoError(t, ec.ConsumeLogs(context.Background(), matchingLogs()))

	_, err = tt.GetMetric("otelcol_exporter_creator_nonroutable_log_records_total")
	require.Error(t, err, "nothing should be counted when a matched exporter accepted the logs")
}

// A batch can carry both metric points a matched exporter could not take and points that
// matched nothing at all. The unmatched tally used to be assigned over the unsupported-signal
// one rather than added to it, so a batch containing both reported only the unmatched half.
func TestUnsupportedSignal_MixedBatchCountsBothLosses(t *testing.T) {
	tt := componenttest.NewTelemetry()
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}
	ec, err := newExporterCreator(metadatatest.NewSettings(tt), cfg)
	require.NoError(t, err)
	// Logs-only, so metrics matching it are dropped for an unhandled signal. No default
	// exporters are configured, so whatever matches nothing is dropped too.
	ec.router.AddExporter("logs-endpoint", &wrappedExporter{logs: &nopExporter{}},
		observer.EndpointEnv{"labels": map[string]string{"service": "svc"}})

	md := pmetric.NewMetrics()
	// Two points that match the logs-only exporter and cannot be handled by it.
	matched := md.ResourceMetrics().AppendEmpty()
	matched.Resource().Attributes().PutStr("service.name", "svc")
	g := matched.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	g.SetName("m")
	g.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	g.Gauge().DataPoints().AppendEmpty().SetIntValue(2)
	// One point matching no rule at all.
	unmatched := md.ResourceMetrics().AppendEmpty()
	unmatched.Resource().Attributes().PutStr("service.name", "nobody")
	u := unmatched.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	u.SetName("u")
	u.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(3)

	require.NoError(t, ec.ConsumeMetrics(context.Background(), md))

	metadatatest.AssertEqualExporterCreatorNonroutableMetricPointsTotal(t, tt,
		[]metricdata.DataPoint[int64]{{Value: 3}}, metricdatatest.IgnoreTimestamp())
}

// The warning has to name the exporter that could not take the signal. Every runtime exporter
// is the same Go type, so logging the type identifies nothing; the component ID carries both
// the template it came from and the endpoint it was created for.
func TestUnsupportedSignal_WarningIdentifiesTheExporter(t *testing.T) {
	params, logs := newObservedSettings(zapcore.WarnLevel)
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}
	ec, err := newExporterCreator(params, cfg)
	require.NoError(t, err)

	id := component.MustNewIDWithName("otlp", `prw/0/otlp{endpoint="localhost:9090"}/prw-endpoint`)
	ec.router.AddExporter("prw-endpoint", &wrappedExporter{id: id, metrics: &nopExporter{}},
		observer.EndpointEnv{"labels": map[string]string{"service": "svc"}})

	require.NoError(t, ec.ConsumeLogs(context.Background(), matchingLogs()))

	warns := logs.FilterMessage("matched exporter does not handle this signal, dropping telemetry").All()
	require.Len(t, warns, 1)
	assert.Equal(t, id.String(), warns[0].ContextMap()["exporter"],
		"the warning must name which exporter dropped the telemetry")
}
