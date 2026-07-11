# Wago Workers

Bounded, composable worker primitives for the [Wago](https://github.com/wago-org/wago)
WebAssembly runtime. This package is an optional open-source Wago plugin; the
core runtime contains no worker queues, IDs, messaging, links, or supervision
policy.

```sh
go get github.com/wago-org/workers
```

```json
{
  "dependencies": ["github.com/wago-org/workers"],
  "plugins": [{
    "name": "workers",
    "capabilities": {
      "instance.manage": {"maxInstances": 64}
    }
  }]
}
```

See the package documentation and tests for registration and guest ABI examples.

## Programmatic use

```go
workerPlugin := workers.New()
rt := wago.NewRuntime()
if err := rt.Use(workerPlugin, wago.WithPluginGrants(
    wago.PluginManagedInstances,
    wago.PluginInstanceHooks,
)); err != nil {
    return err
}

service := workerPlugin.Service()
service.OnMessage(func(ctx *workers.MessageContext) error {
    // Decode plugin-defined tags/payloads or write into ctx.Caller.Memory().
    return nil
})
service.OnExit(func(ctx *workers.WorkerExitContext) {
    // Build monitoring, restarts, signals, or logging here.
})
```

Spawn must happen inside a synchronous host import, where `caller` is the active
`wago.HostModule`. The table entry must have the exact Wasm signature `() -> ()`:

```go
id, err := service.Spawn(caller, tableIndex, workers.WorkerOptions{
    QueueCapacity:   64,
    MaxPayloadBytes: 64 << 10,
    MaxQueueBytes:   1 << 20,
})
if err != nil {
    return err
}

if err := service.Send(id, 42, []byte("hello")); err != nil {
    return err
}
```

The guest callback cooperatively receives through a plugin-defined host import:

```go
if err := service.DispatchNext(caller); err != nil {
    return pluginErrno(err)
}
```

`Send` copies payloads and never blocks. `Link` ties a child to its exact creator,
while `Kill` requests cooperative termination. Workers intentionally does not
define PIDs, guest mailboxes, monitors, supervision, restart policy, or a guest ABI.
