<div align="center">
  <h1><code>workers</code></h1>
  <p>Bounded, composable WebAssembly worker primitives for Wago.</p>
</div>

<p align="center">
  <a href="https://github.com/wago-org/workers/actions/workflows/ci.yml"><img src="https://github.com/wago-org/workers/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/go-%3E%3D1.24-00ADD8.svg" alt="Go >= 1.24"></a>
</p>

Workers turns a WebAssembly table entry into a bounded, host-supervised worker.
Each worker is a managed fork of its caller, runs on one goroutine, and owns a
fixed-capacity, byte-bounded mailbox.

The package deliberately stops at primitives: spawn, send, receive, link, kill,
and observe. Policies such as supervision trees, restarts, and guest-visible
mailbox ABIs belong in a plugin built on top.

> Workers is experimental (`v0.1.0`). Its API may change before the first stable
> release.

## Install

```sh
wago add github.com/wago-org/workers
```

The install review shows two exact required authorities:

- `instance.manage`, with positive instance and memory ceilings, to fork and own
  workers.
- `instance.close.observe`, to stop linked workers when their exact creator
  closes.

You may narrow the published `instance.manage` limits. The plugin also enforces
its own worker-count and mailbox-memory ceilings.

## Use from another plugin

Workers provides the typed contract
`github.com/wago-org/workers/service@1`. Declare it in the consumer's immutable
definition:

```go
var WorkersContract = workers.Contract

func Definition() wago.PluginDefinition {
	return wago.PluginDefinition{
		// ...ID, version, and provenance...
		Requires: []wago.PluginRequirement{{
			ID: workers.PluginID, Version: "^0.1.0",
		}},
		Consumes: []wago.ContractRequirement{{
			ID: WorkersContract.ID(), Major: WorkersContract.Major(),
			Mode: wago.ContractRequired,
		}},
	}
}
```

Require the contract during registration and use it only through its callback:

```go
workersRef, err := wagoplugin.Require(reg, workers.Contract)
if err != nil {
	return err
}

err = workersRef.With(func(service workers.Service) error {
	id, err := service.Spawn(caller, tableIndex, workers.WorkerOptions{})
	if err != nil {
		return err
	}
	return service.Send(id, 42, payload)
})
```

Wago records the exact provider binding in `wago-lock.json`. A new matching
provider cannot silently enter the graph, and the reference fails closed after
the consumer stops. The package requirement lets the resolver install Workers
transitively; the contract requirement records the exact reviewed call target.

## Observe safely

Observers return an opaque subscription token:

```go
err = workersRef.With(func(service workers.Service) error {
	messages, err = service.ObserveMessages(func(ctx *workers.MessageContext) error {
		return handleMessage(ctx.WorkerID, ctx.Tag, ctx.Payload)
	})
	return err
})
```

Pass subscriptions to `service.Unsubscribe` through `workersRef.With` in the
consuming plugin's `Stop` callback. Unsubscribe removes the observer and waits
for callbacks already in flight, so the consumer can then release its state
safely. It must not be called from inside its own callback.

## Worker operations

`Spawn`, `Current`, `DispatchNext`, and `Link` must run inside a synchronous host
call and receive that call's `wago.HostModule`. This prevents one guest from
impersonating another.

- `Spawn` forks the caller and runs a `() -> ()` table entry.
- `Send` copies a tagged payload into a bounded mailbox and never blocks.
- `DispatchNext` waits cooperatively and delivers one queued message. Its
  context lets the consuming plugin cancel the wait during `Stop`.
- `Current` returns the worker ID for the current managed caller.
- `Link` ties a worker to its exact creator.
- `Kill` requests cooperative termination by worker ID.

Every worker has `QueueCapacity`, `MaxPayloadBytes`, and `MaxQueueBytes` bounds.
Zero fields take safe package defaults.

## Configure

```sh
wago plugin config github.com/wago-org/workers \
  '{"maxLiveWorkers":128,"maxQueueBytes":134217728}'
```

Configuration is strict: unknown fields, zero explicit limits, out-of-range
values, and trailing JSON are rejected before activation.

## Test

```sh
go test ./...
go test -race ./...
```

The suite covers quotas, host-call identity, worker teardown, copied payloads,
typed contract revocation, and observer unsubscribe races.

## License

Apache-2.0. See [LICENSE](./LICENSE).
