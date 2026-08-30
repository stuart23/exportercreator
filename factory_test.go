// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"

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
