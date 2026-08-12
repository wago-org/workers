package workers

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wago-org/wago"
	wagoplugin "github.com/wago-org/wago/plugin"
	"github.com/wago-org/wago/tests/wasmtest"
)

type registerFunc func(*wago.Registrar) error

func (f registerFunc) Register(reg *wago.Registrar) error { return f(reg) }

func pluginTestSet(t *testing.T, providers []wago.PluginProvider, config json.RawMessage) wago.PluginSet {
	t.Helper()
	set := wago.PluginSet{Providers: providers}
	for _, provider := range providers {
		digest, err := wago.DefinitionDigest(provider.Definition)
		if err != nil {
			t.Fatal(err)
		}
		selection := wago.PluginSelection{
			ID: provider.Definition.ID, DefinitionDigest: digest, Direct: true,
			Dependencies: map[string]string{},
		}
		for _, requirement := range provider.Definition.Requires {
			selection.Dependencies[requirement.ID] = requirement.Version
		}
		if provider.Definition.ID == PluginID {
			selection.Config = append(json.RawMessage(nil), config...)
		}
		for _, authority := range provider.Definition.Authorities {
			selection.Grants = append(selection.Grants, wago.AuthorityGrant{Name: authority.Name, Scope: authority.Scope})
		}
		for _, requirement := range provider.Definition.Consumes {
			var owners []string
			for _, candidate := range providers {
				for _, provided := range candidate.Definition.Provides {
					if provided.ID == requirement.ID && provided.Major == requirement.Major {
						owners = append(owners, candidate.Definition.ID)
					}
				}
			}
			sort.Strings(owners)
			if requirement.Mode != wago.ContractMany && len(owners) > 1 {
				owners = owners[:1]
			}
			selection.Contracts = append(selection.Contracts, wago.ContractBinding{ID: requirement.ID, Major: requirement.Major, Providers: owners})
		}
		set.Selections = append(set.Selections, selection)
	}
	return set
}

type integrationPlugin struct {
	service  *wagoplugin.Ref[Service]
	id       WorkerID
	exits    chan WorkerExitContext
	messages chan MessageContext
	message  Subscription
	exit     Subscription
}

func integrationProvider(p *integrationPlugin) wago.PluginProvider {
	definition := wago.PluginDefinition{
		ID: "example.com/workers-integration", Version: "1.0.0",
		Provenance: wago.PluginProvenance{Repository: "https://example.com/workers-integration", License: "MIT"},
		Requires:   []wago.PluginRequirement{{ID: PluginID, Version: "^0.1.0"}},
		Authorities: []wago.AuthorityRequest{{
			Name: wago.AuthorityHostImportDefine, Mode: wago.AuthorityRequired,
			Reason: "exercise worker calls from a guest host import",
			Scope:  wago.AuthorityScope{Modules: []string{"test"}},
		}},
		Consumes: []wago.ContractRequirement{{ID: Contract.ID(), Major: Contract.Major(), Mode: wago.ContractRequired}},
	}
	return wago.PluginProvider{Definition: definition, New: func() wago.Plugin {
		return registerFunc(func(reg *wago.Registrar) error {
			var err error
			p.service, err = wagoplugin.Require(reg, Contract)
			if err != nil {
				return err
			}
			imports, err := reg.HostImports()
			if err != nil {
				return err
			}
			module, err := imports.Module("test")
			if err != nil {
				return err
			}
			module.Func("spawn", func(caller wago.HostModule, _, _ []uint64) {
				_ = p.service.With(func(service Service) error {
					p.id, _ = service.Spawn(caller, 0, WorkerOptions{QueueCapacity: 1, MaxPayloadBytes: 16, MaxQueueBytes: 16})
					return nil
				})
			})
			module.Func("next", func(caller wago.HostModule, _, _ []uint64) {
				_ = p.service.With(func(service Service) error { return service.DispatchNext(context.Background(), caller) })
			})
			return reg.Lifecycle(wago.PluginLifecycle{
				Start: func(context.Context) error {
					return p.service.With(func(service Service) error {
						p.message, err = service.ObserveMessages(func(ctx *MessageContext) error { p.messages <- *ctx; return nil })
						if err != nil {
							return err
						}
						p.exit, err = service.ObserveExits(func(ctx *WorkerExitContext) { p.exits <- *ctx })
						return err
					})
				},
				Stop: func(context.Context) error {
					return p.service.With(func(service Service) error {
						return errors.Join(service.Unsubscribe(p.message), service.Unsubscribe(p.exit))
					})
				},
			})
		})
	}}
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
	if got := normalizeLimits(WorkerLimits{}); got.MaxLiveWorkers != DefaultMaxLiveWorkers || got.MaxQueueBytes != DefaultMaxServiceQueueBytes {
		t.Fatalf("normalizeLimits(zero) = %+v", got)
	}
	w := newWorkers(nil, WorkerLimits{MaxLiveWorkers: 2, MaxQueueBytes: 1000})
	if err := w.reserve(600); err != nil {
		t.Fatal(err)
	}
	if err := w.reserve(400); err != nil {
		t.Fatal(err)
	}
	if err := w.reserve(1); !errors.Is(err, ErrWorkerQuotaExceeded) {
		t.Fatalf("reserve above quota = %v", err)
	}
	w.release(600)
	if err := w.reserve(1); err != nil {
		t.Fatal(err)
	}
}

func TestPluginContractSpawnsCopiesMessageAndStops(t *testing.T) {
	p := &integrationPlugin{exits: make(chan WorkerExitContext, 1), messages: make(chan MessageContext, 1)}
	rt := wago.NewRuntime()
	// Reverse the dependency order to prove Requires plus the reviewed contract
	// binding, rather than fixture order, places Workers before its consumer.
	set := pluginTestSet(t, []wago.PluginProvider{integrationProvider(p), Provider()}, json.RawMessage(`{"maxLiveWorkers":8,"maxQueueBytes":1024}`))
	if err := rt.LoadPlugins(context.Background(), set); err != nil {
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
	if err := p.service.With(func(service Service) error { return service.Send(p.id, 42, payload) }); err != nil {
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
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.service.With(func(Service) error { return nil }); !errors.Is(err, wago.ErrPermissionDenied) {
		t.Fatalf("contract after close = %v", err)
	}
}

func TestSubscriptionCloseWaitsAndPreventsFutureCalls(t *testing.T) {
	w := newWorkers(nil, WorkerLimits{})
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	sub, err := w.ObserveMessages(func(*MessageContext) error {
		calls.Add(1)
		close(started)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	observer := w.messageObservers()[0]
	done := make(chan struct{})
	go func() { _ = observer.invoke(new(MessageContext)); close(done) }()
	<-started
	closed := make(chan struct{})
	go func() { _ = w.Unsubscribe(sub); close(closed) }()
	select {
	case <-closed:
		t.Fatal("subscription closed before callback returned")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-done
	<-closed
	if err := observer.invoke(new(MessageContext)); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("observer calls = %d, want 1", got)
	}
}

func TestPluginRejectsStrictInvalidConfig(t *testing.T) {
	for _, config := range []json.RawMessage{
		json.RawMessage(`null`),
		json.RawMessage(`[]`),
		json.RawMessage(`{"maxLiveWorkers":null}`),
		json.RawMessage(`{"maxLiveWorkers":1,"maxLiveWorkers":2}`),
		json.RawMessage(`{"unknown":1}`),
		json.RawMessage(`{"maxLiveWorkers":0}`),
		json.RawMessage(`{"maxQueueBytes":0}`),
		json.RawMessage(`{} {}`),
	} {
		if err := wago.ValidatePluginSet(pluginTestSet(t, []wago.PluginProvider{Provider()}, config)); err == nil {
			t.Fatalf("accepted invalid config %s", config)
		}
	}
}
