// Package register activates the workers plugin's init-time registration for
// Wago-generated hosts. Applications normally import github.com/wago-org/workers
// directly; generated hosts blank-import this package.
package register

import _ "github.com/wago-org/workers"
