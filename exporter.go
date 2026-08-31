// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/service/hostcapabilities"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

var (
	_ exporter.Logs    = (*exporterCreator)(nil)
	_ exporter.Metrics = (*exporterCreator)(nil)
	_ exporter.Traces  = (*exporterCreator)(nil)
	_ consumer.Logs    = (*exporterCreator)(nil)
	_ consumer.Metrics = (*exporterCreator)(nil)
	_ consumer.Traces  = (*exporterCreator)(nil)
)

// exporterCreator implements the exporter that dynamically creates sub-exporters at runtime.
type exporterCreator struct {
	params           exporter.Settings
	cfg              *Config
	observerHandler  *observerHandler
	observables      []observer.Observable
	router           *telemetryRouter
	telemetry        *metadata.TelemetryBuilder
	defaultExporters map[component.ID]component.Component
	// shared is the wrapper handed to every pipeline referencing this creator; see factory.go.
	shared *sharedExporter
}

// host is an interface that the component.Host passed to exportercreator's Start function must implement
type host interface {
	component.Host
	hostcapabilities.ComponentFactory
}

func newExporterCreator(params exporter.Settings, cfg *Config) (*exporterCreator, error) {
	telemetry, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	router := newTelemetryRouter(cfg.Routing.Rules, telemetry)
	router.setLogger(params.Logger)
	ec := &exporterCreator{
		params:    params,
		cfg:       cfg,
		router:    router,
		telemetry: telemetry,
	}
	return ec, nil
}

// Start exporter_creator.
func (ec *exporterCreator) Start(ctx context.Context, h component.Host) error {
	ecHost, ok := h.(host)
	if !ok {
		return errors.New("the exporter_creator is not compatible with the provided component.host")
	}

	// Initialize the gauge metric to 0
	if ec.telemetry != nil {
		ec.telemetry.ExporterCreatorExportersCount.Record(ctx, 0)
	}

	// Get default exporters from host
	// Note: Default exporters should be static exporters configured in the service pipelines
	// For now, we'll store the IDs and look them up when needed
	ec.defaultExporters = make(map[component.ID]component.Component)
	// TODO: Implement proper default exporter lookup from host
	// This requires access to the service's exporter registry which may not be available
	// through the standard component.Host interface

	// Create observer handler
	ec.observerHandler = &observerHandler{
		config:              ec.cfg,
		params:              ec.params,
		exportersByEndpoint: make(exporterMap),
		router:              ec.router,
		runner:              newExporterRunner(ec.params, ecHost),
	}

	observers := map[component.ID]observer.Observable{}

	// Match all configured observables to the extensions that are running.
	for _, watchObserver := range ec.cfg.WatchObservers {
		for cid, ext := range ecHost.GetExtensions() {
			if cid != watchObserver {
				continue
			}

			obs, ok := ext.(observer.Observable)
			if !ok {
				return fmt.Errorf("extension %q in watch_observers is not an observer", watchObserver.String())
			}
			observers[watchObserver] = obs
		}
	}

	// Make sure all observables are present before starting any.
	for _, watchObserver := range ec.cfg.WatchObservers {
		if observers[watchObserver] == nil {
			return fmt.Errorf("failed to find observer %q in the extensions list", watchObserver.String())
		}
	}

	if len(observers) == 0 {
		ec.params.Logger.Warn("no observers were configured and no subexporters will be started. exporter_creator will be disabled")
	}

	// Start all configured watchers.
	for _, observable := range observers {
		ec.observables = append(ec.observables, observable)
		observable.ListAndWatch(ec.observerHandler)
	}

	return nil
}

// Shutdown stops the exporter_creator and all its exporters started at runtime.
func (ec *exporterCreator) Shutdown(ctx context.Context) error {
	var errs []error

	// Unsubscribe from all observers
	for _, observable := range ec.observables {
		observable.Unsubscribe(ec.observerHandler)
	}

	// Shutdown observer handler (which shuts down all sub-exporters)
	if ec.observerHandler != nil {
		if err := ec.observerHandler.shutdown(); err != nil {
			errs = append(errs, err)
		}
	}

	if ec.telemetry != nil {
		ec.telemetry.Shutdown()
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// Capabilities implements consumer.Logs, consumer.Metrics, consumer.Traces.
func (ec *exporterCreator) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeLogs routes logs to the appropriate sub-exporter based on resource attributes.
func (ec *exporterCreator) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	if ec.router == nil {
		return nil
	}

	var errs []error
	nonRoutableCount := int64(0)
	exportersByComponent := make(map[component.Component]plog.Logs)
	unmatchedLogs := plog.NewLogs()

	// Group logs by matching exporters
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resourceAttrs := rl.Resource().Attributes()

		// Find matching exporters
		matchedExporters := ec.router.Route(resourceAttrs)

		if len(matchedExporters) > 0 {
			// Route to matched exporters
			accepted := false
			for _, exp := range matchedExporters {
				var logsExp exporter.Logs
				// Check if it's a wrappedExporter and extract the logs exporter
				if we, ok := exp.(*wrappedExporter); ok && we.logs != nil {
					logsExp = we.logs
				} else if le, ok := exp.(exporter.Logs); ok {
					logsExp = le
				}

				if logsExp == nil {
					// The exporter this endpoint resolves to cannot handle logs, as a
					// metrics-only exporter cannot. Routing already matched, so this would
					// otherwise be dropped without reaching the default exporters.
					ec.warnUnsupportedSignal(exp, "logs")
					continue
				}
				accepted = true
				if _, exists := exportersByComponent[exp]; !exists {
					exportersByComponent[exp] = plog.NewLogs()
				}
				rl.CopyTo(exportersByComponent[exp].ResourceLogs().AppendEmpty())
			}
			if !accepted {
				nonRoutableCount += countResourceLogRecords(rl)
			}
		} else {
			// No match, add to unmatched
			rl.CopyTo(unmatchedLogs.ResourceLogs().AppendEmpty())
		}
	}

	// Send to matched exporters
	for exp, logs := range exportersByComponent {
		var logsExp exporter.Logs
		// Check if it's a wrappedExporter and extract the logs exporter
		if we, ok := exp.(*wrappedExporter); ok && we.logs != nil {
			logsExp = we.logs
		} else if le, ok := exp.(exporter.Logs); ok {
			logsExp = le
		}

		if logsExp != nil {
			if err := logsExp.ConsumeLogs(ctx, logs); err != nil {
				errs = append(errs, fmt.Errorf("failed to export logs to exporter: %w", err))
			}
		}
	}

	// Route unmatched logs to default exporters
	if unmatchedLogs.ResourceLogs().Len() > 0 {
		hasLogsExporter := false
		exportSucceeded := false
		for _, defaultExp := range ec.defaultExporters {
			if logsExp, ok := defaultExp.(exporter.Logs); ok {
				hasLogsExporter = true
				if err := logsExp.ConsumeLogs(ctx, unmatchedLogs); err != nil {
					errs = append(errs, fmt.Errorf("failed to export logs to default exporter: %w", err))
				} else {
					exportSucceeded = true
				}
			}
		}
		if !hasLogsExporter || !exportSucceeded {
			nonRoutableCount += countLogRecords(unmatchedLogs)
		}
	}

	if nonRoutableCount > 0 && ec.telemetry != nil {
		ec.telemetry.ExporterCreatorNonroutableLogRecordsTotal.Add(ctx, nonRoutableCount)
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// ConsumeMetrics routes metrics to the appropriate sub-exporter based on resource attributes.
// ConsumeMetrics routes metrics to the appropriate sub-exporter based on resource attributes.
func (ec *exporterCreator) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if ec.router == nil {
		return nil
	}

	logger := ec.params.Logger
	// Rendering resource attributes for logging allocates a map per resource, on every export.
	// Only pay for it when a debug-level log would actually be emitted.
	debug := logger.Core().Enabled(zapcore.DebugLevel)

	var errs []error
	// unroutableBySignal counts points whose endpoint matched an exporter that cannot handle
	// metrics. They never reach the unmatched set, so they are accounted for separately.
	unroutableBySignal := int64(0)
	exportersByComponent := make(map[component.Component]pmetric.Metrics)
	unmatchedMetrics := pmetric.NewMetrics()

	// Group metrics by matching exporters
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resourceAttrs := rm.Resource().Attributes()

		// Find matching exporters. The router logs why routing failed at debug level.
		matchedExporters := ec.router.Route(resourceAttrs)

		if len(matchedExporters) > 0 {
			// Route to matched exporters
			accepted := false
			for _, exp := range matchedExporters {
				var metricsExp exporter.Metrics
				// Check if it's a wrappedExporter and extract the metrics exporter
				if we, ok := exp.(*wrappedExporter); ok && we.metrics != nil {
					metricsExp = we.metrics
				} else if me, ok := exp.(exporter.Metrics); ok {
					metricsExp = me
				}

				if metricsExp == nil {
					ec.warnUnsupportedSignal(exp, "metrics")
					continue
				}
				accepted = true
				if _, exists := exportersByComponent[exp]; !exists {
					exportersByComponent[exp] = pmetric.NewMetrics()
				}
				if debug {
					logger.Debug("adding ResourceMetrics to exporter",
						zap.String("exporter_type", fmt.Sprintf("%T", exp)),
						zap.Any("resource_attributes", attrsToMap(resourceAttrs)),
					)
				}
				rm.CopyTo(exportersByComponent[exp].ResourceMetrics().AppendEmpty())
			}
			if !accepted {
				unroutableBySignal += countResourceMetricPoints(rm)
			}
		} else {
			// No match, add to unmatched. Per-resource detail is debug-only; the aggregate
			// is reported below and counted by the non-routable metric.
			if debug {
				logger.Debug("metrics did not match any routing rules, adding to unmatched",
					zap.Any("resource_attributes", attrsToMap(resourceAttrs)),
				)
			}
			rm.CopyTo(unmatchedMetrics.ResourceMetrics().AppendEmpty())
		}
	}

	// Send to matched exporters
	if debug && len(exportersByComponent) > 0 {
		logger.Debug("sending metrics to matched exporters",
			zap.Int("exporter_count", len(exportersByComponent)),
			zap.Int("total_metric_points", int(countMetricPoints(md))),
		)
	}

	for exp, metrics := range exportersByComponent {
		var metricsExp exporter.Metrics
		expType := fmt.Sprintf("%T", exp)
		// Check if it's a wrappedExporter and extract the metrics exporter
		if we, ok := exp.(*wrappedExporter); ok {
			if we.metrics != nil {
				metricsExp = we.metrics
			} else {
				// Unreachable: the grouping loop above only records exporters that yielded a
				// non-nil metrics exporter. Kept as a guard against that invariant breaking.
				logger.Warn("wrappedExporter has no metrics exporter, dropping metrics",
					zap.String("exporter_type", expType),
					zap.Int("metrics_lost", metrics.ResourceMetrics().Len()),
				)
			}
		} else if me, ok := exp.(exporter.Metrics); ok {
			metricsExp = me
		} else {
			// Unreachable for the same reason as above.
			logger.Warn("exporter does not support metrics and is not a wrappedExporter, dropping metrics",
				zap.String("exporter_type", expType),
				zap.Int("metrics_lost", metrics.ResourceMetrics().Len()),
			)
		}

		if metricsExp == nil {
			continue
		}

		if debug {
			resourceAttrsList := make([]map[string]string, 0, metrics.ResourceMetrics().Len())
			for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
				resourceAttrsList = append(resourceAttrsList, attrsToMap(metrics.ResourceMetrics().At(i).Resource().Attributes()))
			}
			logger.Debug("sending metrics to exporter",
				zap.Int("resource_metrics_count", metrics.ResourceMetrics().Len()),
				zap.Any("resource_attributes", resourceAttrsList),
				zap.String("exporter_type", fmt.Sprintf("%T", metricsExp)),
			)
		}

		if err := metricsExp.ConsumeMetrics(ctx, metrics); err != nil {
			logger.Error("failed to export metrics to exporter",
				zap.Error(err),
				zap.String("exporter_type", fmt.Sprintf("%T", metricsExp)),
				zap.Int("metrics_count", metrics.ResourceMetrics().Len()),
			)
			errs = append(errs, fmt.Errorf("failed to export metrics to exporter: %w", err))
		} else if debug {
			logger.Debug("successfully exported metrics to exporter",
				zap.Int("metric_count", metrics.ResourceMetrics().Len()),
				zap.String("exporter_type", fmt.Sprintf("%T", metricsExp)),
			)
		}
	}

	// Route unmatched metrics to default exporters
	nonRoutableCount := unroutableBySignal
	if unmatchedMetrics.ResourceMetrics().Len() > 0 {
		if debug {
			unmatchedAttrsList := make([]map[string]string, 0, unmatchedMetrics.ResourceMetrics().Len())
			for i := 0; i < unmatchedMetrics.ResourceMetrics().Len(); i++ {
				unmatchedAttrsList = append(unmatchedAttrsList, attrsToMap(unmatchedMetrics.ResourceMetrics().At(i).Resource().Attributes()))
			}
			logger.Debug("processing unmatched metrics",
				zap.Int("unmatched_resource_metrics", unmatchedMetrics.ResourceMetrics().Len()),
				zap.Any("unmatched_resource_attributes", unmatchedAttrsList),
				zap.Int("default_exporters_count", len(ec.defaultExporters)),
			)
		}

		if len(ec.defaultExporters) > 0 {
			hasMetricsExporter := false
			exportSucceeded := false
			for _, defaultExp := range ec.defaultExporters {
				if metricsExp, ok := defaultExp.(exporter.Metrics); ok {
					hasMetricsExporter = true
					if err := metricsExp.ConsumeMetrics(ctx, unmatchedMetrics); err != nil {
						errs = append(errs, fmt.Errorf("failed to export metrics to default exporter: %w", err))
					} else {
						exportSucceeded = true
					}
				}
			}
			// If no default exporter supports metrics, or export failed, count all unmatched as non-routable
			if !hasMetricsExporter || !exportSucceeded {
				unmatchedCount := countMetricPoints(unmatchedMetrics)
				// Added, not assigned: a batch can carry both points no matched exporter
				// could take and points nothing matched at all, and both are lost.
				nonRoutableCount += unmatchedCount
				// Counted by the non-routable metric; a failed export is separately returned
				// to the pipeline as an error. Detail here is debug-only to stay off the
				// per-batch log path.
				if debug {
					logger.Debug("unmatched metrics could not be sent to a default exporter",
						zap.Int64("unmatched_metric_points", unmatchedCount),
						zap.Bool("has_metrics_exporter", hasMetricsExporter),
						zap.Bool("export_succeeded", exportSucceeded),
					)
				}
			}
		} else {
			// No default exporters configured, count all unmatched as non-routable. This is a
			// valid configuration, so it is reported by the non-routable metric rather than a log.
			unmatchedCount := countMetricPoints(unmatchedMetrics)
			nonRoutableCount += unmatchedCount
			if debug {
				logger.Debug("counting unmatched metrics as non-routable (no default exporters configured)",
					zap.Int64("unmatched_metric_points", unmatchedCount),
				)
			}
		}
	}

	// Record non-routable metric points
	if nonRoutableCount > 0 {
		if ec.telemetry != nil {
			ec.telemetry.ExporterCreatorNonroutableMetricPointsTotal.Add(ctx, nonRoutableCount)
			if debug {
				logger.Debug("recorded non-routable metric points",
					zap.Int64("non_routable_count", nonRoutableCount),
					zap.Int("unmatched_resource_metrics", unmatchedMetrics.ResourceMetrics().Len()),
				)
			}
		} else {
			// Without telemetry the drop goes uncounted, so it has to be logged. Unreachable
			// via newExporterCreator, which fails rather than returning a nil telemetry builder.
			logger.Warn("dropping non-routable metric points but telemetry is unavailable to record them",
				zap.Int64("non_routable_count", nonRoutableCount),
				zap.Int("unmatched_resource_metrics", unmatchedMetrics.ResourceMetrics().Len()),
			)
		}
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// countMetricPoints counts the total number of data points in a pmetric.Metrics.
func countMetricPoints(md pmetric.Metrics) int64 {
	var count int64
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		count += countResourceMetricPoints(md.ResourceMetrics().At(i))
	}
	return count
}

// ConsumeTraces routes traces to the appropriate sub-exporter based on resource attributes.
func (ec *exporterCreator) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	if ec.router == nil {
		return nil
	}

	var errs []error
	nonRoutableCount := int64(0)
	exportersByComponent := make(map[component.Component]ptrace.Traces)
	unmatchedTraces := ptrace.NewTraces()

	// Group traces by matching exporters
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resourceAttrs := rs.Resource().Attributes()

		// Find matching exporters
		matchedExporters := ec.router.Route(resourceAttrs)

		if len(matchedExporters) > 0 {
			// Route to matched exporters
			accepted := false
			for _, exp := range matchedExporters {
				var tracesExp exporter.Traces
				// Check if it's a wrappedExporter and extract the traces exporter
				if we, ok := exp.(*wrappedExporter); ok && we.traces != nil {
					tracesExp = we.traces
				} else if te, ok := exp.(exporter.Traces); ok {
					tracesExp = te
				}

				if tracesExp == nil {
					ec.warnUnsupportedSignal(exp, "traces")
					continue
				}
				accepted = true
				if _, exists := exportersByComponent[exp]; !exists {
					exportersByComponent[exp] = ptrace.NewTraces()
				}
				rs.CopyTo(exportersByComponent[exp].ResourceSpans().AppendEmpty())
			}
			if !accepted {
				nonRoutableCount += countResourceSpans(rs)
			}
		} else {
			// No match, add to unmatched
			rs.CopyTo(unmatchedTraces.ResourceSpans().AppendEmpty())
		}
	}

	// Send to matched exporters
	for exp, traces := range exportersByComponent {
		var tracesExp exporter.Traces
		// Check if it's a wrappedExporter and extract the traces exporter
		if we, ok := exp.(*wrappedExporter); ok && we.traces != nil {
			tracesExp = we.traces
		} else if te, ok := exp.(exporter.Traces); ok {
			tracesExp = te
		}

		if tracesExp != nil {
			if err := tracesExp.ConsumeTraces(ctx, traces); err != nil {
				errs = append(errs, fmt.Errorf("failed to export traces to exporter: %w", err))
			}
		}
	}

	// Route unmatched traces to default exporters
	if unmatchedTraces.ResourceSpans().Len() > 0 {
		hasTracesExporter := false
		exportSucceeded := false
		for _, defaultExp := range ec.defaultExporters {
			if tracesExp, ok := defaultExp.(exporter.Traces); ok {
				hasTracesExporter = true
				if err := tracesExp.ConsumeTraces(ctx, unmatchedTraces); err != nil {
					errs = append(errs, fmt.Errorf("failed to export traces to default exporter: %w", err))
				} else {
					exportSucceeded = true
				}
			}
		}
		if !hasTracesExporter || !exportSucceeded {
			nonRoutableCount += countSpans(unmatchedTraces)
		}
	}

	if nonRoutableCount > 0 && ec.telemetry != nil {
		ec.telemetry.ExporterCreatorNonroutableSpansTotal.Add(ctx, nonRoutableCount)
	}

	if len(errs) > 0 {
		return multierr.Combine(errs...)
	}
	return nil
}

// attrsToMap renders resource attributes for logging. Callers should check that the log will
// actually be emitted before calling it.
func attrsToMap(attrs pcommon.Map) map[string]string {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

// warnUnsupportedSignal reports, once per exporter and signal, that telemetry matched an
// exporter which cannot handle it. Whether that telemetry is then counted as non-routable
// depends on the other exporters it matched: it is only lost, and only counted, when none of
// them accepted it. The warning names the exporter so the mismatch can be traced back to a
// template.
func (ec *exporterCreator) warnUnsupportedSignal(exp component.Component, signal string) {
	we, ok := exp.(*wrappedExporter)
	if !ok || !we.firstUnsupported(signal) {
		return
	}
	ec.params.Logger.Warn("matched exporter does not handle this signal, dropping telemetry",
		zap.String("signal", signal),
		zap.String("exporter", we.id.String()),
	)
}

// countLogRecords counts the total number of log records in a plog.Logs.
func countLogRecords(ld plog.Logs) int64 {
	var count int64
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		count += countResourceLogRecords(ld.ResourceLogs().At(i))
	}
	return count
}

// countSpans counts the total number of spans in a ptrace.Traces.
func countSpans(td ptrace.Traces) int64 {
	var count int64
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		count += countResourceSpans(td.ResourceSpans().At(i))
	}
	return count
}

// countResourceLogRecords counts the log records in a single ResourceLogs.
func countResourceLogRecords(rl plog.ResourceLogs) int64 {
	var count int64
	for i := 0; i < rl.ScopeLogs().Len(); i++ {
		count += int64(rl.ScopeLogs().At(i).LogRecords().Len())
	}
	return count
}

// countResourceSpans counts the spans in a single ResourceSpans.
func countResourceSpans(rs ptrace.ResourceSpans) int64 {
	var count int64
	for i := 0; i < rs.ScopeSpans().Len(); i++ {
		count += int64(rs.ScopeSpans().At(i).Spans().Len())
	}
	return count
}

// countResourceMetricPoints counts the data points in a single ResourceMetrics.
func countResourceMetricPoints(rm pmetric.ResourceMetrics) int64 {
	var count int64
	for i := 0; i < rm.ScopeMetrics().Len(); i++ {
		sm := rm.ScopeMetrics().At(i)
		for j := 0; j < sm.Metrics().Len(); j++ {
			metric := sm.Metrics().At(j)
			//exhaustive:enforce
			switch metric.Type() {
			case pmetric.MetricTypeGauge:
				count += int64(metric.Gauge().DataPoints().Len())
			case pmetric.MetricTypeSum:
				count += int64(metric.Sum().DataPoints().Len())
			case pmetric.MetricTypeHistogram:
				count += int64(metric.Histogram().DataPoints().Len())
			case pmetric.MetricTypeExponentialHistogram:
				count += int64(metric.ExponentialHistogram().DataPoints().Len())
			case pmetric.MetricTypeSummary:
				count += int64(metric.Summary().DataPoints().Len())
			case pmetric.MetricTypeEmpty:
				// Empty metrics have no data points
			}
		}
	}
	return count
}
