// Package workers provides bounded WebAssembly workers as an optional Wago plugin.
package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/wago-org/wago"
	wagoplugin "github.com/wago-org/wago/plugin"
)

const PluginID = "github.com/wago-org/workers"

type WorkerID uint64

const (
	DefaultWorkerQueueCapacity   uint32 = 64
	DefaultWorkerMaxPayloadBytes uint32 = 64 << 10
	DefaultWorkerMaxQueueBytes   uint32 = 1 << 20
	MaxWorkerQueueCapacity       uint32 = 1 << 16
	MaxWorkerPayloadBytes        uint32 = 16 << 20
	MaxWorkerQueueBytes          uint32 = 64 << 20

	// DefaultMaxLiveWorkers bounds the number of simultaneously live workers for
	// one service when WorkerLimits.MaxLiveWorkers is left zero. Each worker owns a
	// managed instance, a goroutine, and a foreign stack, so an unbounded count is a
	// denial-of-service vector; a package-level default remains useful even when
	// the reviewed instance.manage grant permits a larger ceiling.
	DefaultMaxLiveWorkers uint32 = 64
	// DefaultMaxServiceQueueBytes bounds the total queued-payload reservation across
	// all live workers when WorkerLimits.MaxQueueBytes is left zero.
	DefaultMaxServiceQueueBytes uint64 = 64 << 20
)

type workerError string

func (e workerError) Error() string { return string(e) }

const (
	ErrWorkersInactive      workerError = "worker service is not active"
	ErrInvalidWorkerOptions workerError = "invalid worker options"
	ErrInvalidWorkerCaller  workerError = "worker operation requires a current plugin host caller"
	ErrWorkerImportLifetime workerError = "worker cannot safely inherit a borrowed import"
	ErrWorkerNotFound       workerError = "worker not found"
	ErrWorkerStopping       workerError = "worker is stopping"
	ErrWorkerQueueFull      workerError = "worker queue is full"
	ErrWorkerDispatchActive workerError = "worker message dispatch is already active"
	ErrPayloadTooLarge      workerError = "worker payload is too large"
	ErrWorkerIDExhausted    workerError = "worker ID space exhausted"
	ErrInvalidWorkerLink    workerError = "invalid worker link"
	ErrWorkerKilled         workerError = "worker killed"
	ErrWorkerParentClosed   workerError = "worker parent closed"
	ErrWorkerRuntimeClosed  workerError = "worker runtime closed"
	ErrWorkerQuotaExceeded  workerError = "worker service resource quota exceeded"
)

// WorkerLimits bounds the aggregate resources one worker service may hold at
// once, independent of the per-worker WorkerOptions. Zero fields take the
// package defaults. It complements — and does not replace — the core
// instance.manage maxInstances budget: this cap always applies, so workers stay
// bounded even when the host grants instance.manage without a budget.
type WorkerLimits struct {
	// MaxLiveWorkers is the maximum number of simultaneously live workers.
	MaxLiveWorkers uint32
	// MaxQueueBytes is the maximum total per-worker queue-byte reservation summed
	// across all live workers (each worker reserves its MaxQueueBytes for its
	// lifetime).
	MaxQueueBytes uint64
}

func normalizeLimits(l WorkerLimits) WorkerLimits {
	if l.MaxLiveWorkers == 0 {
		l.MaxLiveWorkers = DefaultMaxLiveWorkers
	}
	if l.MaxQueueBytes == 0 {
		l.MaxQueueBytes = DefaultMaxServiceQueueBytes
	}
	return l
}

type WorkerOptions struct {
	QueueCapacity   uint32
	MaxPayloadBytes uint32
	MaxQueueBytes   uint32
}

type MessageContext struct {
	WorkerID WorkerID
	Tag      uint64
	Payload  []byte
	Caller   wago.HostModule
}

type WorkerExitKind uint8

const (
	WorkerReturned WorkerExitKind = iota + 1
	WorkerFailed
	WorkerKilled
)

type WorkerExitContext struct {
	WorkerID WorkerID
	Kind     WorkerExitKind
	Err      error
}

// Service is Workers' typed cross-plugin contract. Call it only inside the
// callback of a wagoplugin.Ref; Wago holds the provider alive for that callback.
type Service interface {
	Spawn(wago.HostModule, uint32, WorkerOptions) (WorkerID, error)
	Send(WorkerID, uint64, []byte) error
	Current(wago.HostModule) (WorkerID, error)
	DispatchNext(context.Context, wago.HostModule) error
	Link(wago.HostModule, WorkerID) error
	Kill(WorkerID) error
	ObserveMessages(func(*MessageContext) error) (Subscription, error)
	ObserveExits(func(*WorkerExitContext)) (Subscription, error)
	Unsubscribe(Subscription) error
}

// Contract is the major-versioned Workers composition seam.
var Contract = wagoplugin.NewContract[Service](PluginID+"/service", 1)

type pluginConfig struct {
	MaxLiveWorkers *uint32 `json:"maxLiveWorkers,omitempty"`
	MaxQueueBytes  *uint64 `json:"maxQueueBytes,omitempty"`
}

var configSchema = json.RawMessage(`{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "maxLiveWorkers": {"type": "integer", "minimum": 1, "maximum": 65536},
    "maxQueueBytes": {"type": "integer", "minimum": 1, "maximum": 68719476736}
  }
}`)

func decodePluginConfig(raw json.RawMessage) (pluginConfig, WorkerLimits, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if err := validateConfigObject(raw); err != nil {
		return pluginConfig{}, WorkerLimits{}, fmt.Errorf("workers: config: %w", err)
	}
	var cfg pluginConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return pluginConfig{}, WorkerLimits{}, fmt.Errorf("workers: config: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return pluginConfig{}, WorkerLimits{}, fmt.Errorf("workers: config has a trailing JSON value")
	}
	limits := WorkerLimits{MaxLiveWorkers: DefaultMaxLiveWorkers, MaxQueueBytes: DefaultMaxServiceQueueBytes}
	if cfg.MaxLiveWorkers != nil {
		if *cfg.MaxLiveWorkers == 0 || *cfg.MaxLiveWorkers > 65536 {
			return pluginConfig{}, WorkerLimits{}, fmt.Errorf("workers: maxLiveWorkers must be in [1, 65536]")
		}
		limits.MaxLiveWorkers = *cfg.MaxLiveWorkers
	}
	if cfg.MaxQueueBytes != nil {
		if *cfg.MaxQueueBytes == 0 || *cfg.MaxQueueBytes > 64<<30 {
			return pluginConfig{}, WorkerLimits{}, fmt.Errorf("workers: maxQueueBytes must be in [1, 68719476736]")
		}
		limits.MaxQueueBytes = *cfg.MaxQueueBytes
	}
	return cfg, limits, nil
}

func validateConfigObject(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	token, err := dec.Token()
	if err != nil {
		return err
	}
	if token != json.Delim('{') {
		return fmt.Errorf("must be a JSON object")
	}
	seen := map[string]struct{}{}
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("object key is not a string")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate field %q", key)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("field %q must not be null", key)
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("has a trailing JSON value")
		}
		return err
	}
	return nil
}

// Definition returns fresh immutable metadata for Workers' explicit provider.
func Definition() wago.PluginDefinition {
	return wago.PluginDefinition{
		ID:          PluginID,
		Name:        "Workers",
		Version:     "0.1.0",
		Description: "Bounded, composable WebAssembly worker primitives for Wago plugins.",
		Stability:   wago.Experimental,
		Compatibility: wago.Compatibility{
			Engines: map[string]string{"wago": ">=0.1.0"},
		},
		Provenance: wago.PluginProvenance{
			Homepage:   "https://github.com/wago-org/workers#readme",
			Repository: "https://github.com/wago-org/workers",
			License:    "Apache-2.0",
			Authors:    []string{"Wago contributors"},
		},
		Authorities: []wago.AuthorityRequest{
			{
				Name: wago.AuthorityInstanceManage, Mode: wago.AuthorityRequired,
				Reason: "fork and own bounded worker instances",
				Scope:  wago.AuthorityScope{MaxInstances: 1024, MaxMemoryBytes: 4 << 30},
			},
			{
				Name: wago.AuthorityInstanceCloseObserve, Mode: wago.AuthorityRequired,
				Reason: "stop linked workers when their exact creator closes",
			},
		},
		ConfigSchema: append(json.RawMessage(nil), configSchema...),
		Provides:     []wago.ContractSpec{Contract.Spec()},
	}
}

// Provider is Workers' side-effect-free catalog entry.
func Provider() wago.PluginProvider {
	return wago.PluginProvider{
		Definition: Definition(),
		New:        func() wago.Plugin { return new(plugin) },
		ValidateConfig: func(raw json.RawMessage) error {
			_, _, err := decodePluginConfig(raw)
			return err
		},
	}
}

type plugin struct{ service *Workers }

func (p *plugin) Register(reg *wago.Registrar) error {
	var cfg pluginConfig
	if err := reg.Config(&cfg); err != nil {
		return err
	}
	limits := WorkerLimits{MaxLiveWorkers: DefaultMaxLiveWorkers, MaxQueueBytes: DefaultMaxServiceQueueBytes}
	if cfg.MaxLiveWorkers != nil {
		limits.MaxLiveWorkers = *cfg.MaxLiveWorkers
	}
	if cfg.MaxQueueBytes != nil {
		limits.MaxQueueBytes = *cfg.MaxQueueBytes
	}
	manager, err := reg.ManagedInstances()
	if err != nil {
		return err
	}
	closeObserver, err := reg.InstanceCloseObserver()
	if err != nil {
		return err
	}
	p.service = newWorkers(manager, limits)
	if err := closeObserver.Before(func(event wago.InstanceCloseEvent) { p.service.parentClosing(event.Instance) }); err != nil {
		return err
	}
	if err := wagoplugin.Provide(reg, Contract, Service(p.service)); err != nil {
		return err
	}
	return reg.Lifecycle(wago.PluginLifecycle{Stop: func(context.Context) error { return p.service.close() }})
}

type Workers struct {
	mu         sync.Mutex
	manager    *wago.InstanceManager
	limits     WorkerLimits
	live       uint32 // number of workers currently holding a quota reservation
	queueBytes uint64 // total per-worker queue-byte reservation currently held
	next       WorkerID
	workers    map[WorkerID]*worker
	byInstance map[wago.InstanceIdentity]*worker
	nextObs    uint64
	messages   map[uint64]*messageObserver
	exits      map[uint64]*exitObserver
	closed     bool
	exitPanics []error
}

func newWorkers(manager *wago.InstanceManager, limits WorkerLimits) *Workers {
	return &Workers{manager: manager, limits: normalizeLimits(limits), next: 1,
		workers: map[WorkerID]*worker{}, byInstance: map[wago.InstanceIdentity]*worker{},
		messages: map[uint64]*messageObserver{}, exits: map[uint64]*exitObserver{}}
}

// reserve claims one live-worker slot and queueBytes of the aggregate queue-byte
// budget, or reports ErrWorkerQuotaExceeded. The reservation is held until the
// worker's goroutine finishes (see release), so a concurrent Spawn cannot exceed
// the ceiling while a worker is still finalizing.
func (w *Workers) reserve(queueBytes uint32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrWorkerRuntimeClosed
	}
	if w.live >= w.limits.MaxLiveWorkers || uint64(queueBytes) > w.limits.MaxQueueBytes-w.queueBytes {
		return ErrWorkerQuotaExceeded
	}
	w.live++
	w.queueBytes += uint64(queueBytes)
	return nil
}

func (w *Workers) release(queueBytes uint32) {
	w.mu.Lock()
	w.live--
	w.queueBytes -= uint64(queueBytes)
	w.mu.Unlock()
}

type subscriptionKind uint8

const (
	messageSubscription subscriptionKind = iota + 1
	exitSubscription
)

// Subscription is an opaque observer token. It has no provider operations of
// its own; pass it back to Service.Unsubscribe inside a leased contract call.
type Subscription struct {
	id   uint64
	kind subscriptionKind
}

type observerGate struct {
	mu       sync.Mutex
	cond     *sync.Cond
	active   bool
	inFlight uint32
}

func newObserverGate() *observerGate {
	g := &observerGate{active: true}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *observerGate) begin() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.active {
		return false
	}
	g.inFlight++
	return true
}

func (g *observerGate) end() {
	g.mu.Lock()
	g.inFlight--
	if g.inFlight == 0 {
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

func (g *observerGate) stop() {
	g.mu.Lock()
	g.active = false
	for g.inFlight != 0 {
		g.cond.Wait()
	}
	g.mu.Unlock()
}

type messageObserver struct {
	id   uint64
	gate *observerGate
	fn   func(*MessageContext) error
}

func (o *messageObserver) invoke(ctx *MessageContext) (err error) {
	if !o.gate.begin() {
		return nil
	}
	defer o.gate.end()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("workers: message observer %d panicked: %v", o.id, recovered)
		}
	}()
	return o.fn(ctx)
}

type exitObserver struct {
	id   uint64
	gate *observerGate
	fn   func(*WorkerExitContext)
}

func (o *exitObserver) invoke(ctx *WorkerExitContext) (panicErr error) {
	if !o.gate.begin() {
		return nil
	}
	defer o.gate.end()
	defer func() {
		if recovered := recover(); recovered != nil {
			panicErr = fmt.Errorf("workers: exit observer %d panicked: %v", o.id, recovered)
		}
	}()
	o.fn(ctx)
	return nil
}

func (w *Workers) nextObserverIDLocked() (uint64, error) {
	w.nextObs++
	if w.nextObs == 0 {
		return 0, fmt.Errorf("workers: observer ID space exhausted")
	}
	return w.nextObs, nil
}

// ObserveMessages registers a message observer until it is unsubscribed.
func (w *Workers) ObserveMessages(fn func(*MessageContext) error) (Subscription, error) {
	if w == nil || fn == nil {
		return Subscription{}, fmt.Errorf("workers: invalid message observer")
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return Subscription{}, ErrWorkerRuntimeClosed
	}
	id, err := w.nextObserverIDLocked()
	if err != nil {
		w.mu.Unlock()
		return Subscription{}, err
	}
	observer := &messageObserver{id: id, gate: newObserverGate(), fn: fn}
	w.messages[id] = observer
	w.mu.Unlock()
	return Subscription{id: id, kind: messageSubscription}, nil
}

// ObserveExits registers an exit observer until it is unsubscribed.
func (w *Workers) ObserveExits(fn func(*WorkerExitContext)) (Subscription, error) {
	if w == nil || fn == nil {
		return Subscription{}, fmt.Errorf("workers: invalid exit observer")
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return Subscription{}, ErrWorkerRuntimeClosed
	}
	id, err := w.nextObserverIDLocked()
	if err != nil {
		w.mu.Unlock()
		return Subscription{}, err
	}
	observer := &exitObserver{id: id, gate: newObserverGate(), fn: fn}
	w.exits[id] = observer
	w.mu.Unlock()
	return Subscription{id: id, kind: exitSubscription}, nil
}

// Unsubscribe removes one observer and waits for callbacks already in flight.
// It is idempotent. Do not call it from inside that observer's callback.
func (w *Workers) Unsubscribe(subscription Subscription) error {
	if w == nil || subscription.id == 0 {
		return fmt.Errorf("workers: invalid subscription")
	}
	var gate *observerGate
	w.mu.Lock()
	switch subscription.kind {
	case messageSubscription:
		if observer := w.messages[subscription.id]; observer != nil {
			delete(w.messages, subscription.id)
			gate = observer.gate
		}
	case exitSubscription:
		if observer := w.exits[subscription.id]; observer != nil {
			delete(w.exits, subscription.id)
			gate = observer.gate
		}
	default:
		w.mu.Unlock()
		return fmt.Errorf("workers: invalid subscription")
	}
	w.mu.Unlock()
	if gate != nil {
		gate.stop()
	}
	return nil
}

func (w *Workers) messageObservers() []*messageObserver {
	w.mu.Lock()
	ids := make([]uint64, 0, len(w.messages))
	for id := range w.messages {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	observers := make([]*messageObserver, 0, len(ids))
	for _, id := range ids {
		observers = append(observers, w.messages[id])
	}
	w.mu.Unlock()
	return observers
}

func (w *Workers) exitObservers() []*exitObserver {
	w.mu.Lock()
	ids := make([]uint64, 0, len(w.exits))
	for id := range w.exits {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	observers := make([]*exitObserver, 0, len(ids))
	for _, id := range ids {
		observers = append(observers, w.exits[id])
	}
	w.mu.Unlock()
	return observers
}

func normalizeOptions(o WorkerOptions) (WorkerOptions, error) {
	if o.QueueCapacity == 0 {
		o.QueueCapacity = DefaultWorkerQueueCapacity
	}
	if o.MaxPayloadBytes == 0 {
		o.MaxPayloadBytes = DefaultWorkerMaxPayloadBytes
	}
	if o.MaxQueueBytes == 0 {
		o.MaxQueueBytes = DefaultWorkerMaxQueueBytes
	}
	if o.QueueCapacity > MaxWorkerQueueCapacity || o.MaxPayloadBytes > MaxWorkerPayloadBytes || o.MaxQueueBytes > MaxWorkerQueueBytes || o.MaxQueueBytes < o.MaxPayloadBytes {
		return WorkerOptions{}, ErrInvalidWorkerOptions
	}
	return o, nil
}

func (w *Workers) Spawn(caller wago.HostModule, tableIndex uint32, opts WorkerOptions) (WorkerID, error) {
	if w == nil || w.manager == nil {
		return 0, ErrWorkersInactive
	}
	parent, err := w.manager.CallerIdentity(caller)
	if err != nil {
		return 0, ErrInvalidWorkerCaller
	}
	opts, err = normalizeOptions(opts)
	if err != nil {
		return 0, err
	}
	// Claim aggregate quota before forking so an over-limit Spawn never allocates a
	// managed instance. The reservation is released by the worker's goroutine when
	// it exits, or here on any failure before the goroutine starts.
	if err := w.reserve(opts.MaxQueueBytes); err != nil {
		return 0, err
	}
	child, err := w.manager.Fork(context.Background(), caller)
	if errors.Is(err, wago.ErrManagedImportLifetime) {
		w.release(opts.MaxQueueBytes)
		return 0, ErrWorkerImportLifetime
	}
	if err != nil {
		w.release(opts.MaxQueueBytes)
		return 0, err
	}
	if err := child.ValidateVoidTableEntry(tableIndex); err != nil {
		_ = child.Close()
		w.release(opts.MaxQueueBytes)
		return 0, err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		_ = child.Close()
		w.release(opts.MaxQueueBytes)
		return 0, ErrWorkerRuntimeClosed
	}
	if w.next == 0 {
		w.mu.Unlock()
		_ = child.Close()
		w.release(opts.MaxQueueBytes)
		return 0, ErrWorkerIDExhausted
	}
	id := w.next
	if id == ^WorkerID(0) {
		w.next = 0
	} else {
		w.next++
	}
	childIdentity := child.Identity()
	wr := &worker{owner: w, id: id, creator: parent, identity: childIdentity, instance: child, tableIndex: tableIndex,
		queue: make([]message, opts.QueueCapacity), maxPayload: opts.MaxPayloadBytes, maxQueueBytes: opts.MaxQueueBytes,
		wake: make(chan struct{}, 1), done: make(chan struct{})}
	w.workers[id], w.byInstance[childIdentity] = wr, wr
	w.mu.Unlock()
	go wr.run()
	return id, nil
}

func (w *Workers) Send(id WorkerID, tag uint64, payload []byte) error {
	wr, err := w.owned(id)
	if err != nil {
		return err
	}
	return wr.enqueue(tag, payload)
}

func (w *Workers) Current(caller wago.HostModule) (WorkerID, error) {
	owned, err := w.manager.ManagedCaller(caller)
	if err != nil {
		return 0, ErrWorkerNotFound
	}
	w.mu.Lock()
	wr := w.byInstance[owned.Identity()]
	w.mu.Unlock()
	if wr == nil {
		return 0, ErrWorkerNotFound
	}
	return wr.id, nil
}

func (w *Workers) DispatchNext(ctx context.Context, caller wago.HostModule) error {
	if ctx == nil {
		ctx = context.Background()
	}
	owned, err := w.manager.ManagedCaller(caller)
	if err != nil {
		return ErrInvalidWorkerCaller
	}
	w.mu.Lock()
	wr := w.byInstance[owned.Identity()]
	w.mu.Unlock()
	if wr == nil {
		return ErrWorkerNotFound
	}
	return wr.dispatch(ctx, caller)
}

func (w *Workers) Link(caller wago.HostModule, childID WorkerID) error {
	parent, err := w.manager.CallerIdentity(caller)
	if err != nil {
		return ErrInvalidWorkerCaller
	}
	wr, err := w.owned(childID)
	if err != nil {
		return err
	}
	if wr.creator != parent || wr.identity == parent {
		return ErrInvalidWorkerLink
	}
	wr.mu.Lock()
	defer wr.mu.Unlock()
	if wr.stopping {
		return ErrWorkerStopping
	}
	wr.linked = true
	return nil
}

func (w *Workers) Kill(id WorkerID) error {
	wr, err := w.owned(id)
	if err != nil {
		return err
	}
	wr.stop(ErrWorkerKilled)
	return nil
}

func (w *Workers) owned(id WorkerID) (*worker, error) {
	if w == nil {
		return nil, ErrWorkersInactive
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrWorkerRuntimeClosed
	}
	wr := w.workers[id]
	if wr == nil {
		return nil, ErrWorkerNotFound
	}
	return wr, nil
}

type message struct {
	tag     uint64
	payload []byte
}
type worker struct {
	mu                            sync.Mutex
	owner                         *Workers
	id                            WorkerID
	creator                       wago.InstanceIdentity
	identity                      wago.InstanceIdentity
	instance                      *wago.ManagedInstance
	tableIndex                    uint32
	queue                         []message
	head, count                   uint32
	queuedBytes                   uint32
	maxPayload, maxQueueBytes     uint32
	wake                          chan struct{}
	done                          chan struct{}
	stopping, dispatching, linked bool
	stopErr                       error
}

func (wr *worker) signal() {
	select {
	case wr.wake <- struct{}{}:
	default:
	}
}

func (wr *worker) enqueue(tag uint64, payload []byte) error {
	if uint64(len(payload)) > uint64(wr.maxPayload) {
		return ErrPayloadTooLarge
	}
	copyPayload := append([]byte(nil), payload...)
	wr.mu.Lock()
	if wr.stopping {
		wr.mu.Unlock()
		return ErrWorkerStopping
	}
	if wr.count == uint32(len(wr.queue)) || uint64(wr.queuedBytes)+uint64(len(copyPayload)) > uint64(wr.maxQueueBytes) {
		wr.mu.Unlock()
		return ErrWorkerQueueFull
	}
	idx := (wr.head + wr.count) % uint32(len(wr.queue))
	wr.queue[idx] = message{tag: tag, payload: copyPayload}
	wr.count++
	wr.queuedBytes += uint32(len(copyPayload))
	wr.mu.Unlock()
	wr.signal()
	return nil
}

func (wr *worker) dispatch(ctx context.Context, caller wago.HostModule) error {
	wr.mu.Lock()
	if wr.dispatching {
		wr.mu.Unlock()
		return ErrWorkerDispatchActive
	}
	wr.dispatching = true
	wr.mu.Unlock()
	defer func() { wr.mu.Lock(); wr.dispatching = false; wr.mu.Unlock() }()
	expired, cancel, err := wr.owner.manager.WatchCaller(caller)
	if err != nil {
		return ErrInvalidWorkerCaller
	}
	defer cancel()
	for {
		wr.mu.Lock()
		if wr.stopping {
			err := wr.stopErr
			wr.mu.Unlock()
			return err
		}
		if wr.count != 0 {
			msg := wr.queue[wr.head]
			wr.queue[wr.head] = message{}
			wr.head = (wr.head + 1) % uint32(len(wr.queue))
			wr.count--
			wr.queuedBytes -= uint32(len(msg.payload))
			wr.mu.Unlock()
			ctx := &MessageContext{WorkerID: wr.id, Tag: msg.tag, Payload: msg.payload, Caller: caller}
			for _, observer := range wr.owner.messageObservers() {
				if err := observer.invoke(ctx); err != nil {
					wr.stop(err)
					return err
				}
			}
			return nil
		}
		wr.mu.Unlock()
		select {
		case <-wr.wake:
		case <-expired:
			return ErrInvalidWorkerCaller
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (wr *worker) stop(err error) {
	wr.mu.Lock()
	if !wr.stopping {
		wr.stopping, wr.stopErr = true, err
		for i := range wr.queue {
			wr.queue[i] = message{}
		}
		wr.count, wr.queuedBytes = 0, 0
	}
	wr.mu.Unlock()
	wr.signal()
}

func (wr *worker) run() {
	err := wr.instance.InvokeVoidTable(context.Background(), wr.tableIndex)
	kind := WorkerReturned
	wr.mu.Lock()
	cause := wr.stopErr
	stopping := wr.stopping
	wr.mu.Unlock()
	if stopping {
		err = cause
		kind = WorkerKilled
	} else if err != nil {
		kind = WorkerFailed
	}
	_ = wr.instance.Close()
	wr.owner.mu.Lock()
	delete(wr.owner.workers, wr.id)
	delete(wr.owner.byInstance, wr.identity)
	wr.owner.mu.Unlock()
	ctx := &WorkerExitContext{WorkerID: wr.id, Kind: kind, Err: err}
	for _, observer := range wr.owner.exitObservers() {
		if panicErr := observer.invoke(ctx); panicErr != nil {
			wr.owner.mu.Lock()
			wr.owner.exitPanics = append(wr.owner.exitPanics, fmt.Errorf("worker %d: %w", wr.id, panicErr))
			wr.owner.mu.Unlock()
		}
	}
	// Release aggregate quota only after the instance is closed and every exit
	// observer has run, so a concurrent Spawn cannot exceed the ceiling while this
	// worker is still finalizing.
	wr.owner.release(wr.maxQueueBytes)
	close(wr.done)
}

func (w *Workers) parentClosing(parent wago.InstanceIdentity) {
	w.mu.Lock()
	var linked []*worker
	for _, wr := range w.workers {
		if wr.creator == parent && wr.linked {
			linked = append(linked, wr)
		}
	}
	w.mu.Unlock()
	for _, wr := range linked {
		wr.stop(ErrWorkerParentClosed)
	}
	for _, wr := range linked {
		<-wr.done
	}
}

func (w *Workers) close() error {
	w.mu.Lock()
	if w.closed {
		errs := append([]error(nil), w.exitPanics...)
		w.mu.Unlock()
		return errors.Join(errs...)
	}
	w.closed = true
	list := make([]*worker, 0, len(w.workers))
	for _, wr := range w.workers {
		list = append(list, wr)
	}
	w.mu.Unlock()
	for _, wr := range list {
		wr.stop(ErrWorkerRuntimeClosed)
	}
	for _, wr := range list {
		<-wr.done
	}
	w.mu.Lock()
	observers := make([]*observerGate, 0, len(w.messages)+len(w.exits))
	for _, observer := range w.messages {
		observers = append(observers, observer.gate)
	}
	for _, observer := range w.exits {
		observers = append(observers, observer.gate)
	}
	w.messages = nil
	w.exits = nil
	errs := append([]error(nil), w.exitPanics...)
	w.exitPanics = nil
	w.mu.Unlock()
	for _, observer := range observers {
		observer.stop()
	}
	return errors.Join(errs...)
}
