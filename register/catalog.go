// Package register exposes Workers' explicit provider catalog to generated Wago
// runtimes. Importing this package has no registration side effects.
package register

import (
	"github.com/wago-org/wago"
	"github.com/wago-org/workers"
)

func Providers() []wago.PluginProvider {
	return []wago.PluginProvider{workers.Provider()}
}
