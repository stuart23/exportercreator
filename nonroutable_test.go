// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/stuart23/exportercreator/internal/metadatatest"
)

// The non-routable counters cover two separate losses. Telemetry dropped for an unhandled
// signal is covered in unsupported_signal_test.go; these cover the other half, telemetry that
// matched no routing rule and that default_exporters did not take. Logs and traces are counted
// here for the first time, so every outcome of that branch is pinned down.

// failingLogsExporter is a default exporter that accepts logs but always fails to export them.
type failingLogsExporter struct {
	exporter.Logs
}

func (f *failingLogsExporter) ConsumeLogs(context.Context, plog.Logs) error {
	return errors.New("export failed")
}

// failingTracesExporter is a default exporter that accepts traces but always fails.
type failingTracesExporter struct {
	exporter.Traces
}

func (f *failingTracesExporter) ConsumeTraces(context.Context, ptrace.Traces) error {
	return errors.New("export failed")
}

// unroutableLogs returns two log records whose resource attributes match no routing rule.
func unroutableLogs() plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "nobody")
	rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	rl.ScopeLogs().At(0).LogRecords().AppendEmpty()
	return ld
}

// unroutableTraces returns one span whose resource attributes match no routing rule.
func unroutableTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "nobody")
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	return td
}

// nonRoutableCreator returns a creator with routing rules that the unroutable fixtures do not
// match, and the given default exporters.
func nonRoutableCreator(t *testing.T, tt *componenttest.Telemetry, defaults map[component.ID]component.Component) *exporterCreator {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.Routing.Rules = []RoutingRule{{ResourceAttribute: "service.name", EndpointProperty: "labels.service"}}
	ec, err := newExporterCreator(metadatatest.NewSettings(tt), cfg)
	require.NoError(t, err)
	ec.defaultExporters = defaults
	return ec
}

func TestNonRoutable_UnmatchedLogsAccounting(t *testing.T) {
	tests := []struct {
		name      string
		defaults  map[component.ID]component.Component
		wantCount int64 // 0 means the counter must not be recorded at all
		wantErr   bool
	}{
		{
			name:      "no default exporters configured",
			defaults:  nil,
			wantCount: 2,
		},
		{
			name: "no default exporter handles logs",
			defaults: map[component.ID]component.Component{
				component.MustNewID("metricsonly"): &mockMetricsExporter{metrics: &[]pmetric.Metrics{}},
			},
			wantCount: 2,
		},
		{
			name: "the default logs exporter fails",
			defaults: map[component.ID]component.Component{
				component.MustNewID("failing"): &failingLogsExporter{},
			},
			wantCount: 2,
			wantErr:   true,
		},
		{
			name: "the default logs exporter accepts",
			defaults: map[component.ID]component.Component{
				component.MustNewID("sink"): &mockLogsExporter{logs: &[]plog.Logs{}},
			},
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tt := componenttest.NewTelemetry()
			ec := nonRoutableCreator(t, tt, tc.defaults)

			err := ec.ConsumeLogs(context.Background(), unroutableLogs())
			if tc.wantErr {
				require.Error(t, err, "a failing default export must be reported to the pipeline")
			} else {
				require.NoError(t, err)
			}

			if tc.wantCount == 0 {
				_, err := tt.GetMetric("otelcol_exporter_creator_nonroutable_log_records_total")
				assert.Error(t, err, "nothing is lost when a default exporter took the logs")
				return
			}
			metadatatest.AssertEqualExporterCreatorNonroutableLogRecordsTotal(t, tt,
				[]metricdata.DataPoint[int64]{{Value: tc.wantCount}}, metricdatatest.IgnoreTimestamp())
		})
	}
}

func TestNonRoutable_UnmatchedTracesAccounting(t *testing.T) {
	tests := []struct {
		name      string
		defaults  map[component.ID]component.Component
		wantCount int64 // 0 means the counter must not be recorded at all
		wantErr   bool
	}{
		{
			name:      "no default exporters configured",
			defaults:  nil,
			wantCount: 1,
		},
		{
			name: "no default exporter handles traces",
			defaults: map[component.ID]component.Component{
				component.MustNewID("metricsonly"): &mockMetricsExporter{metrics: &[]pmetric.Metrics{}},
			},
			wantCount: 1,
		},
		{
			name: "the default traces exporter fails",
			defaults: map[component.ID]component.Component{
				component.MustNewID("failing"): &failingTracesExporter{},
			},
			wantCount: 1,
			wantErr:   true,
		},
		{
			name: "the default traces exporter accepts",
			defaults: map[component.ID]component.Component{
				component.MustNewID("sink"): &mockTracesExporter{traces: &[]ptrace.Traces{}},
			},
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tt := componenttest.NewTelemetry()
			ec := nonRoutableCreator(t, tt, tc.defaults)

			err := ec.ConsumeTraces(context.Background(), unroutableTraces())
			if tc.wantErr {
				require.Error(t, err, "a failing default export must be reported to the pipeline")
			} else {
				require.NoError(t, err)
			}

			if tc.wantCount == 0 {
				_, err := tt.GetMetric("otelcol_exporter_creator_nonroutable_spans_total")
				assert.Error(t, err, "nothing is lost when a default exporter took the spans")
				return
			}
			metadatatest.AssertEqualExporterCreatorNonroutableSpansTotal(t, tt,
				[]metricdata.DataPoint[int64]{{Value: tc.wantCount}}, metricdatatest.IgnoreTimestamp())
		})
	}
}
