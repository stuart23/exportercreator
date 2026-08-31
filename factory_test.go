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
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/otel/metric"
	noopmetric "go.opentelemetry.io/otel/metric/noop"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
	"github.com/stuart23/exportercreator/internal/metadata"
)

func TestCreateExporter_SameInstancePerConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	params := exportertest.NewNopSettings(metadata.Type)
	ctx := t.Context()

	lExp, err := factory.CreateLogs(ctx, params, cfg)
	require.NoError(t, err)
	mExp, err := factory.CreateMetrics(ctx, params, cfg)
	require.NoError(t, err)
	tExp, err := factory.CreateTraces(ctx, params, cfg)
	require.NoError(t, err)

	require.Same(t, lExp, mExp)
	require.Same(t, lExp, tExp)
}

// countingObservable records how many times it is subscribed to.
type countingObservable struct {
	component.Component
	listAndWatch int
	unsubscribe  int
}

func (c *countingObservable) ListAndWatch(observer.Notify) { c.listAndWatch++ }
func (c *countingObservable) Unsubscribe(observer.Notify)  { c.unsubscribe++ }

type observerHost struct {
	component.Host
	exts map[component.ID]component.Component
}

func (h *observerHost) GetFactory(component.Kind, component.Type) component.Factory { return nil }
func (h *observerHost) GetExtensions() map[component.ID]component.Component         { return h.exts }

// One exporter_creator referenced by the logs, metrics and traces pipelines is a single shared
// component, so it must subscribe to its observers exactly once however many pipelines start it.
// receivercreator gets this from handing the collector the SharedComponent itself; returning the
// unwrapped creator instead used to start it once per pipeline, which created a duplicate set of
// sub-exporters per pipeline and orphaned every observerHandler but the last.
func TestCreateExporter_StartsOncePerConfig(t *testing.T) {
	obsID := component.MustNewID("mock_observer")
	obs := &countingObservable{}
	host := &observerHost{exts: map[component.ID]component.Component{obsID: obs}}

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.WatchObservers = []component.ID{obsID}
	params := exportertest.NewNopSettings(metadata.Type)
	ctx := context.Background()

	lExp, err := factory.CreateLogs(ctx, params, cfg)
	require.NoError(t, err)
	mExp, err := factory.CreateMetrics(ctx, params, cfg)
	require.NoError(t, err)
	tExp, err := factory.CreateTraces(ctx, params, cfg)
	require.NoError(t, err)

	require.NoError(t, lExp.Start(ctx, host))
	require.NoError(t, mExp.Start(ctx, host))
	require.NoError(t, tExp.Start(ctx, host))
	assert.Equal(t, 1, obs.listAndWatch, "observer should be subscribed to once, not once per pipeline")

	require.NoError(t, lExp.Shutdown(ctx))
	require.NoError(t, mExp.Shutdown(ctx))
	require.NoError(t, tExp.Shutdown(ctx))
	assert.Equal(t, 1, obs.unsubscribe, "observer should be unsubscribed from once")
}

// failingMeter fails to create the first instrument the telemetry builder asks for, which is
// what makes newExporterCreator return an error.
type failingMeter struct{ noopmetric.Meter }

func (failingMeter) Int64Gauge(string, ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	return nil, errors.New("induced telemetry failure")
}

type failingMeterProvider struct{ noopmetric.MeterProvider }

func (failingMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter { return failingMeter{} }

// A failed construction must not be remembered. GetOrAdd caches whatever its callback returns,
// so failing inside the callback used to leave an entry with no component behind: every later
// pipeline with this config skipped construction and got that entry back, and it could not be
// evicted either, because removal runs through Shutdown and would dereference the nil component.
func TestCreateExporter_FailedCreationIsNotCached(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	ctx := context.Background()

	broken := exportertest.NewNopSettings(metadata.Type)
	broken.MeterProvider = failingMeterProvider{}
	_, err := factory.CreateLogs(ctx, broken, cfg)
	require.Error(t, err, "construction should fail with a meter that cannot create instruments")
	require.ErrorContains(t, err, "induced telemetry failure")

	// Retrying the same config must construct afresh rather than return the cached failure.
	lExp, err := factory.CreateLogs(ctx, exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err, "a retry after a failed creation must not see the failed attempt")
	require.NotNil(t, lExp)

	// And the instance the retry produced is the one later pipelines share.
	mExp, err := factory.CreateMetrics(ctx, exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.Same(t, lExp, mExp)
}
