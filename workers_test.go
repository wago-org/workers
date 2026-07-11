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

func TestWorkerLimitsQuota(t *testing.T) {
	// Defaults apply when a field is zero.
	if got := normalizeLimits(WorkerLimits{}); got.MaxLiveWorkers != DefaultMaxLiveWorkers || got.MaxQueueBytes != DefaultMaxServiceQueueBytes {
		t.Fatalf("normalizeLimits(zero) = %+v", got)
	}
	if got := normalizeLimits(WorkerLimits{MaxLiveWorkers: 3}); got.MaxQueueBytes != DefaultMaxServiceQueueBytes || got.MaxLiveWorkers != 3 {
		t.Fatalf("normalizeLimits partial = %+v", got)
	}

	// MaxLiveWorkers ceiling: the third reservation is rejected, and a release frees a slot.
	w := newWorkers(nil, WorkerLimits{MaxLiveWorkers: 2, MaxQueueBytes: 1 << 20})
	if err := w.reserve(100); err != nil {
		t.Fatalf("reserve 1: %v", err)
	}
	if err := w.reserve(100); err != nil {
		t.Fatalf("reserve 2: %v", err)
	}
	if err := w.reserve(100); !errors.Is(err, ErrWorkerQuotaExceeded) {
		t.Fatalf("reserve 3 = %v, want ErrWorkerQuotaExceeded", err)
	}
	w.release(100)
	if err := w.reserve(100); err != nil {
		t.Fatalf("reserve after release: %v", err)
	}

	// Aggregate queue-byte ceiling is enforced independently and cannot overflow.
	w2 := newWorkers(nil, WorkerLimits{MaxLiveWorkers: 100, MaxQueueBytes: 1000})
	if err := w2.reserve(600); err != nil {
		t.Fatalf("reserve 600: %v", err)
	}
	if err := w2.reserve(600); !errors.Is(err, ErrWorkerQuotaExceeded) {
		t.Fatalf("reserve 600 over budget = %v, want ErrWorkerQuotaExceeded", err)
	}
	if err := w2.reserve(400); err != nil {
		t.Fatalf("reserve exact remaining 400: %v", err)
	}

	// A closed service rejects reservations.
	w2.closed = true
	if err := w2.reserve(1); !errors.Is(err, ErrWorkerRuntimeClosed) {
		t.Fatalf("reserve on closed = %v, want ErrWorkerRuntimeClosed", err)
	}
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
