<div align="center">
    <h1><code>workers</code></h1>
    <p>Bounded, composable worker primitives for the <a href="https://github.com/wago-org/wago">Wago</a> WebAssembly runtime.</p>
</div>

<p align="center">
    <a href="https://pkg.go.dev/github.com/wago-org/workers"><img src="https://pkg.go.dev/badge/github.com/wago-org/workers.svg" alt="Go Reference"></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License: Apache-2.0"></a>
    <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.24-00ADD8.svg" alt="Go >= 1.24"></a>
    <img src="https://img.shields.io/badge/stability-experimental-orange.svg" alt="Stability: experimental">
</p>

<details>
<summary>Table of Contents</summary>

- [Overview](#overview)
- [Installation](#installation)
- [Concepts](#concepts)
- [Usage](#usage)
- [API](#api)
  - [Spawning a worker](#spawning-a-worker)
  - [Sending messages](#sending-messages)
  - [Receiving inside a worker](#receiving-inside-a-worker)
  - [Linking and termination](#linking-and-termination)
  - [Worker options and limits](#worker-options-and-limits)
  - [Errors](#errors)
- [Examples](#examples)
- [Testing](#testing)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

</details>

## Overview

`workers` is an optional [Wago](https://github.com/wago-org/wago) plugin that turns a
WebAssembly table entry into a **bounded, host-supervised worker**. Each worker is a
forked instance of the caller, driven on its own goroutine, fed by a fixed-size,
byte-bounded mailbox. The host observes every message and every exit.

Workers does *not* define PIDs, guest-visible mailboxes, monitors, supervision trees, restart
policy, or a guest ABI. Those are policies you build on top of the primitives here: `Spawn`, `Send`,
`DispatchNext`, `Link`, and `Kill`, using the host callbacks `OnMessage` and `OnExit`.

What you get out of the box:

- **Bounded by construction**: every worker has a capped queue depth, a max payload
  size, and a max total queued bytes. `Send` copies the payload and never blocks.
- **Host-supervised**: `OnMessage` sees every delivered message; `OnExit` sees every
  return, failure, or kill, with the cause.
- **Lifecycle-safe**: a linked worker is torn down cooperatively when its creating
  instance closes, and every worker is drained when the runtime shuts down.

> **Stability:** experimental (`v0.1.0`). The API may change before `v1.0.0`.

## Installation

If you have the [`wago`](https://github.com/wago-org/wago) CLI installed:

```sh
wago pkg install github.com/wago-org/workers
```

or use [`go get`](https://pkg.go.dev/cmd/go#hdr-Get_packages_and_dependencies):

```sh
go get github.com/wago-org/workers
```

The plugin requires two privileged capabilities - `instance.manage` (to fork and manage
worker instances) and `instance.lifecycle` (to observe parent shutdown). Grant them in
your `wago.json`:

```json
{
  "dependencies": ["github.com/wago-org/workers"],
  "plugins": [
    {
      "name": "workers",
      "capabilities": {
        "instance.manage": { "maxInstances": 64 },
        "instance.lifecycle": true
      }
    }
  ]
}
```

When you register the plugin programmatically, grant the same capabilities with
[`wago.WithPluginGrants`](https://pkg.go.dev/github.com/wago-org/wago#WithPluginGrants)
(see [Usage](#usage)). Grants are enforced in strict mode; omitting the option registers
the plugin without capability checks.

## Concepts

| Term | Meaning |
| --- | --- |
| **Plugin** | `workers.New()` - the `wago.Extension` you register with a runtime. It provides one **service**. |
| **Service** (`*workers.Workers`) | The host-side handle you use to spawn, send, link, and kill workers, and to register `OnMessage` / `OnExit` observers. Obtain it with `plugin.Service()` or look it up through `workers.ServiceKey`. |
| **Worker** | A forked child instance running one table entry with the exact Wasm signature `() -> ()`, on its own goroutine. Identified by a `WorkerID`. |
| **Mailbox** | A fixed-capacity, byte-bounded FIFO queue owned by each worker. `Send` enqueues; the worker drains it cooperatively via `DispatchNext`. |
| **Creator** | The instance that spawned the worker. `Link` ties a worker's lifetime to its exact creator. |

A worker's life: **spawn → run its table entry → receive messages cooperatively →
exit (return, fail, or kill) → `OnExit` fires → resources freed.**

## Usage

Register the plugin, grant its capabilities, and attach host observers:

```go
package main

import (
	"log"

	"github.com/wago-org/wago"
	"github.com/wago-org/workers"
)

func main() {
	workerPlugin := workers.New()

	rt := wago.NewRuntime()
	if err := rt.Use(workerPlugin, wago.WithPluginGrants(
		wago.PluginManagedInstances, // "instance.manage"
		wago.PluginInstanceHooks,    // "instance.lifecycle"
	)); err != nil {
		log.Fatal(err)
	}
	defer rt.Close()

	service := workerPlugin.Service()

	// Observe every message delivered to any worker.
	service.OnMessage(func(ctx *workers.MessageContext) error {
		// Decode ctx.Tag / ctx.Payload, or write into ctx.Caller.Memory().
		// Returning an error stops that worker with WorkerFailed.
		return nil
	})

	// Observe every worker exit: return, failure, or kill.
	service.OnExit(func(ctx *workers.WorkerExitContext) {
		// Build monitoring, restarts, signalling, or logging here.
		log.Printf("worker %d exited: kind=%d err=%v", ctx.WorkerID, ctx.Kind, ctx.Err)
	})

	// ... compile and instantiate guest modules that spawn/drive workers.
}
```

`Service()` returns the same handle that a downstream plugin can retrieve through the
exported `workers.ServiceKey`, so composing plugins share one worker registry. Inside your
plugin's `Register`, take a typed reference and resolve it once the runtime is wired up:

```go
ref, err := plugin.Require(reg, workers.ServiceKey)
if err != nil {
	return err
}
// later, when handling a call:
svc, err := ref.Get()
```

## API

All host-side operations that cross into a guest - `Spawn`, `DispatchNext`, `Current`,
`Link` - must be called from **inside a synchronous host import**, where `caller` is the
active `wago.HostModule` for that call. `Send` and `Kill` take a `WorkerID` and may be
called from anywhere.

### Spawning a worker

Spawn forks the calling instance and runs the table entry at `tableIndex`. The entry must
have the exact Wasm signature `() -> ()`; otherwise `Spawn` returns a validation error and
the child is discarded.

```go
id, err := service.Spawn(caller, tableIndex, workers.WorkerOptions{
	QueueCapacity:   64,
	MaxPayloadBytes: 64 << 10, // 64 KiB
	MaxQueueBytes:   1 << 20,  // 1 MiB
})
if err != nil {
	return err
}
```

Zero-valued options fall back to the package defaults (see
[Worker options and limits](#worker-options-and-limits)).

### Sending messages

`Send` copies the payload and returns immediately - it never blocks the caller. It fails
if the worker is unknown, stopping, or its mailbox is at capacity (by count or by bytes),
or if the payload exceeds the worker's `MaxPayloadBytes`.

```go
if err := service.Send(id, 42 /* tag */, []byte("hello")); err != nil {
	// e.g. errors.Is(err, workers.ErrWorkerQueueFull)
	return err
}
```

Because the payload is copied, the caller may reuse or mutate its buffer as soon as `Send`
returns.

### Receiving inside a worker

A worker drains one message per `DispatchNext`, which you expose to the guest through a
plugin-defined host import. It delivers the next queued message to every registered
`OnMessage` observer, blocking cooperatively until a message arrives or the worker stops.

```go
// Inside the host import the guest calls to receive:
if err := service.DispatchNext(caller); err != nil {
	return pluginErrno(err) // map to a guest-visible errno
}
```

A worker can learn its own ID with `Current`:

```go
id, err := service.Current(caller)
```

### Linking and termination

`Link` ties a worker's lifetime to its **exact creator**: when the creating instance
closes, the runtime's `BeforeClose` hook cooperatively stops every linked worker and waits
for them to drain. A worker cannot link to itself, and only the creator may link it.

```go
if err := service.Link(caller, childID); err != nil {
	return err
}
```

`Kill` requests cooperative termination of any worker by ID. The worker's in-flight
`DispatchNext` unblocks, its queue is cleared, and it exits with `WorkerKilled`.

```go
_ = service.Kill(id)
```

Every exit - whether the table entry returned, an `OnMessage` observer returned an error,
or the worker was killed - surfaces through `OnExit`:

```go
service.OnExit(func(ctx *workers.WorkerExitContext) {
	switch ctx.Kind {
	case workers.WorkerReturned: // clean return from the table entry
	case workers.WorkerFailed:   // trap or observer error; see ctx.Err
	case workers.WorkerKilled:   // Kill, parent close, or runtime shutdown
	}
})
```

### Worker options and limits

`WorkerOptions` bounds a single worker. A zero field takes the package default. Values
above the hard maximum (or a `MaxQueueBytes` smaller than `MaxPayloadBytes`) are rejected
with `ErrInvalidWorkerOptions`.

| Field | Default | Maximum | Meaning |
| --- | --- | --- | --- |
| `QueueCapacity` | `64` | `65536` | Max number of queued messages. |
| `MaxPayloadBytes` | `64 KiB` | `16 MiB` | Max size of a single `Send` payload. |
| `MaxQueueBytes` | `1 MiB` | `64 MiB` | Max total bytes queued at once (must be ≥ `MaxPayloadBytes`). |

The corresponding exported constants are `DefaultWorkerQueueCapacity`,
`DefaultWorkerMaxPayloadBytes`, `DefaultWorkerMaxQueueBytes`, `MaxWorkerQueueCapacity`,
`MaxWorkerPayloadBytes`, and `MaxWorkerQueueBytes`.

### Errors

All errors are comparable sentinels - match them with `errors.Is`.

| Error | When |
| --- | --- |
| `ErrWorkersInactive` | Service is nil or not registered. |
| `ErrInvalidWorkerOptions` | Options exceed a hard limit or are inconsistent. |
| `ErrInvalidWorkerCaller` | Operation needs the current plugin host caller and none is active. |
| `ErrWorkerImportLifetime` | The worker would inherit a borrowed import it cannot safely keep. |
| `ErrWorkerNotFound` | No worker with that ID (or not owned by the caller). |
| `ErrWorkerStopping` | The worker is already shutting down. |
| `ErrWorkerQueueFull` | Mailbox at capacity by count or bytes. |
| `ErrWorkerDispatchActive` | `DispatchNext` is already running for that worker. |
| `ErrPayloadTooLarge` | Payload exceeds the worker's `MaxPayloadBytes`. |
| `ErrWorkerIDExhausted` | The `WorkerID` space is exhausted. |
| `ErrInvalidWorkerLink` | Link target is not the caller's direct child, or is self. |
| `ErrWorkerKilled` | Exit cause: `Kill` was requested. |
| `ErrWorkerParentClosed` | Exit cause: the linked creator instance closed. |
| `ErrWorkerRuntimeClosed` | Exit cause: the runtime shut down. |

## Examples

The end-to-end integration test is the canonical, runnable example: it defines a guest
module that imports `spawn`/`next`, spawns a worker, and verifies the host copies a
message and observes a clean exit. See
[`workers_test.go`](./workers_test.go) - in particular `TestPluginSpawnsCopiesMessageAndStops`.

A generated Wago host does not import this package directly; it blank-imports the
[`register`](./register) subpackage, which activates the plugin's init-time registration:

```go
import _ "github.com/wago-org/workers/register"
```

## Testing

```sh
go test ./...
```

The suite uses `github.com/wago-org/wago/testutil/wasmtest` to hand-build guest modules,
so no external toolchain is required. Run with the race detector while iterating on the
concurrency paths:

```sh
go test -race ./...
```

## Architecture

- **`workers.go`** - the whole plugin: the `Plugin` extension, the `Workers` service, the
  per-worker mailbox and goroutine, and the lifecycle wiring (`BeforeClose` teardown,
  runtime-close drain).
- **`register/`** - a blank-import shim that activates init-time registration for
  Wago-generated hosts.
- **`wago.json`** - the package manifest declaring the plugin and its required
  capabilities.

Design notes:

- Each worker runs on one goroutine (`worker.run`) that invokes the table entry and, on
  return, resolves the exit kind, closes the child instance, unregisters it, and fans out
  to `OnExit` observers (each guarded against panics).
- The mailbox is a fixed-size ring buffer guarded by a mutex; `Send` enqueues and signals,
  `DispatchNext` dequeues and delivers. Back-pressure is explicit: a full queue rejects
  rather than blocks.
- `Spawn` forks through the managed-instance API and validates the target table entry
  before the worker is made visible, so a bad spawn leaves no partial state.

## Contributing

Contributions are welcome! Please:

- Run `go test -race ./...` and `go vet ./...` before opening a pull request.
- Keep the plugin's boundaries intact - no PIDs, guest mailboxes, supervision, or guest
  ABI belong in this package; those are policies for layers built on top.
- Follow standard Go formatting (`gofmt`) and conventional commit messages.

## License

This project is distributed under the [Apache License 2.0](./LICENSE). Work on this
project is done out of passion - if you want to support it financially, you can donate
through [GitHub Sponsors](https://github.com/sponsors/JairusSW).

## Contact

Please file issues at [GitHub Issues](https://github.com/wago-org/workers/issues). To chat,
join the [Wago Discord](https://wago.sh/discord).

- **GitHub:** [https://github.com/wago-org/](https://github.com/wago-org/)
- **Website:** [https://wago.sh/](https://wago.sh/)
- **Discord:** [https://wago.sh/discord](https://wago.sh/discord)
