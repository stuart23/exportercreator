// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportercreator // import "github.com/stuart23/exportercreator"

import (
	"go.opentelemetry.io/collector/component"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/observer"
)

// exporterMap is a multimap for mapping one id to many exporters. It does
// not deduplicate the same value being associated with the same key.
type exporterMap map[observer.EndpointID][]component.Component

// Put exp into key id. If exp is a duplicate it will still be added.
func (em exporterMap) Put(id observer.EndpointID, exp component.Component) {
	em[id] = append(em[id], exp)
}

// Get exporters by id.
func (em exporterMap) Get(id observer.EndpointID) []component.Component {
	return em[id]
}

// RemoveAll removes all exporters by id.
func (em exporterMap) RemoveAll(id observer.EndpointID) {
	delete(em, id)
}

// Values returns all exporters in the map.
func (em exporterMap) Values() (out []component.Component) {
	for _, m := range em {
		out = append(out, m...)
	}
	return out
}

// Size is the number of total exporters in the map.
func (em exporterMap) Size() (out int) {
	for _, m := range em {
		out += len(m)
	}
	return out
}
