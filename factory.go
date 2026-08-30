// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/stuart23/exportercreator/internal/metadata"
	"github.com/stuart23/exportercreator/internal/sharedcomponent"
)

// This file implements factory for exporter_creator. An exporter_creator can create other exporters at runtime.

var exporters = sharedcomponent.NewSharedComponents()

// NewFactory creates a factory for exporter creator.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		exporterTemplates: map[string]exporterTemplate{},
		Routing:           RoutingConfig{Rules: []RoutingRule{}},
		DefaultExporters:  []component.ID{},
	}
}

// sharedExporter presents one exporter_creator instance to every pipeline that references it.
//
// receivercreator hands the collector the sharedcomponent.SharedComponent directly, so that Start
// and Shutdown run once no matter how many pipelines the component appears in. An exporter cannot
// do the same, because exporter.Logs/Metrics/Traces require the consumer methods that
// SharedComponent does not have. Embedding it keeps the start-once and stop-once guarantees while
// the consume methods are forwarded to the single underlying creator.
type sharedExporter struct {
	*sharedcomponent.SharedComponent
	creator *exporterCreator
}

var (
	_ exporter.Logs    = (*sharedExporter)(nil)
	_ exporter.Metrics = (*sharedExporter)(nil)
	_ exporter.Traces  = (*sharedExporter)(nil)
)

func (s *sharedExporter) Capabilities() consumer.Capabilities {
	return s.creator.Capabilities()
}

func (s *sharedExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return s.creator.ConsumeLogs(ctx, ld)
}

func (s *sharedExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return s.creator.ConsumeMetrics(ctx, md)
}

func (s *sharedExporter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return s.creator.ConsumeTraces(ctx, td)
}

// getOrAddExporterCreator returns the exporter_creator shared by every pipeline using this config.
func getOrAddExporterCreator(params exporter.Settings, cfg component.Config) (*sharedExporter, error) {
	var createErr error
	sc := exporters.GetOrAdd(cfg, func() component.Component {
		ec, err := newExporterCreator(params, cfg.(*Config))
		if err != nil {
			createErr = err
			return nil
		}
		return ec
	})
	if createErr != nil {
		return nil, createErr
	}
	// A previous pipeline may have cached an entry whose construction failed.
	ec, ok := sc.Unwrap().(*exporterCreator)
	if !ok {
		return nil, errors.New("exporter_creator failed to initialize for this configuration")
	}
	// Every pipeline gets the same wrapper, as receivercreator hands every pipeline the same
	// SharedComponent. Factory calls happen serially while the collector builds the graph.
	if ec.shared == nil {
		ec.shared = &sharedExporter{SharedComponent: sc, creator: ec}
	}
	return ec.shared, nil
}

func createLogsExporter(
	_ context.Context,
	params exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	se, err := getOrAddExporterCreator(params, cfg)
	if err != nil {
		// Returning se directly would hand back a non-nil interface holding a nil pointer.
		return nil, err
	}
	return se, nil
}

func createMetricsExporter(
	_ context.Context,
	params exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	se, err := getOrAddExporterCreator(params, cfg)
	if err != nil {
		// Returning se directly would hand back a non-nil interface holding a nil pointer.
		return nil, err
	}
	return se, nil
}

func createTracesExporter(
	_ context.Context,
	params exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	se, err := getOrAddExporterCreator(params, cfg)
	if err != nil {
		// Returning se directly would hand back a non-nil interface holding a nil pointer.
		return nil, err
	}
	return se, nil
}
