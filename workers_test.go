package workers

import (
	"errors"
	"testing"
	"time"

	"github.com/wago-org/wago"
	"github.com/wago-org/wago/testutil/wasmtest"
)

type integrationPlugin struct {
	Plugin
	id       WorkerID
	exits    chan WorkerExitContext
	messages chan MessageContext
}

func (p *integrationPlugin) Info() wago.ExtensionInfo {
	info := p.Plugin.Info()
	info.ID = "wago.workers.integration"
	info.RequiresCapabilities = append(info.RequiresCapabilities, wago.PluginHostImports)
	return info
}

func (p *integrationPlugin) Register(reg *wago.Registry) error {
	if err := p.Plugin.Register(reg); err != nil {
		return err
	}
	p.Service().OnMessage(func(ctx *MessageContext) error { p.messages <- *ctx; return nil })
	p.Service().OnExit(func(ctx *WorkerExitContext) { p.exits <- *ctx })
	imports, err := reg.HostImports()
	if err != nil {
		return err
	}
	imports.Module("test").Func("spawn", func(caller wago.HostModule, _, _ []uint64) {
		p.id, _ = p.Service().Spawn(caller, 0, WorkerOptions{QueueCapacity: 1, MaxPayloadBytes: 16, MaxQueueBytes: 16})
	})
	imports.Module("test").Func("next", func(caller wago.HostModule, _, _ []uint64) { _ = p.Service().DispatchNext(caller) })
	return nil
}

func integrationModule() []byte {
	types := wasmtest.Vec(wasmtest.FuncType(nil, nil))
	spawn := append(wasmtest.Name("test"), wasmtest.Name("spawn")...)
	spawn = append(spawn, 0, 0)
	next := append(wasmtest.Name("test"), wasmtest.Name("next")...)
	next = append(next, 0, 0)
	imports := wasmtest.Vec(spawn, next)
	funcs := wasmtest.Vec(wasmtest.ULEB(0), wasmtest.ULEB(0))
	table := wasmtest.Vec([]byte{0x70, 0x00, 0x01})
	export := append(wasmtest.Name("start"), 0x00)
	export = append(export, wasmtest.ULEB(3)...)
	elem := append([]byte{0x00, 0x41, 0x00, 0x0b, 0x01}, wasmtest.ULEB(2)...)
	code := wasmtest.Vec(wasmtest.Code([]byte{0x10, 0x01, 0x0b}), wasmtest.Code([]byte{0x10, 0x00, 0x0b}))
	return wasmtest.Module(
		wasmtest.Section(1, types), wasmtest.Section(2, imports), wasmtest.Section(3, funcs),
		wasmtest.Section(4, table), wasmtest.Section(7, wasmtest.Vec(export)),
		wasmtest.Section(9, wasmtest.Vec(elem)), wasmtest.Section(10, code),
	)
}

func TestPluginSpawnsCopiesMessageAndStops(t *testing.T) {
	p := &integrationPlugin{exits: make(chan WorkerExitContext, 1), messages: make(chan MessageContext, 1)}
	rt := wago.NewRuntime()
	if err := rt.Use(p); err != nil {
		t.Fatal(err)
	}
	mod, err := rt.Compile(integrationModule())
	if err != nil {
		t.Fatal(err)
	}
	in, err := rt.Instantiate(nil, mod)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if _, err := in.Invoke("start"); err != nil {
		t.Fatal(err)
	}
	if p.id == 0 {
		t.Fatal("spawn returned zero ID")
	}
	payload := []byte("abc")
	if err := p.Service().Send(p.id, 42, payload); err != nil {
		t.Fatal(err)
	}
	copy(payload, "zzz")
	select {
	case msg := <-p.messages:
		if msg.Tag != 42 || string(msg.Payload) != "abc" {
			t.Fatalf("message = %#v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("message timeout")
	}
	select {
	case ex := <-p.exits:
		if ex.Kind != WorkerReturned || ex.Err != nil {
			t.Fatalf("exit = %#v", ex)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exit timeout")
	}
	if err := p.Service().Send(p.id, 0, nil); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("send after exit = %v", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
}
