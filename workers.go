// Package workers provides bounded WebAssembly workers as an optional Wago plugin.
package workers

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wago-org/wago"
	"github.com/wago-org/wago/plugin"
)

const PluginName = "workers"

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
	// denial-of-service vector; a default keeps Spawn bounded even when the host
	// grants instance.manage without a maxInstances budget.
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

var ServiceKey = plugin.NewServiceKey[*Workers]("wago.workers/v1")

type Plugin struct {
	service *Workers
	limits  WorkerLimits
}

// Option configures the workers Plugin at construction.
type Option func(*Plugin)

// WithLimits sets the aggregate resource limits for the worker service. Zero
// fields fall back to the package defaults (see WorkerLimits).
func WithLimits(l WorkerLimits) Option { return func(p *Plugin) { p.limits = l } }

// New creates the workers plugin. Pass WithLimits to override the default
// aggregate resource caps.
func New(opts ...Option) *Plugin {
	p := &Plugin{}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (*Plugin) Info() wago.ExtensionInfo {
	return wago.ExtensionInfo{
		ID: "wago.workers", Name: "Workers", Version: "0.1.0",
		Description: "Bounded, extension-scoped WebAssembly worker primitives",
		Stability:   wago.Experimental, Repository: "https://github.com/wago-org/workers",
		License: "Apache-2.0", Tags: []string{"workers", "concurrency", "plugin-foundation"},
		RequiresCapabilities: []wago.PluginCapability{wago.PluginManagedInstances, wago.PluginInstanceHooks},
		Compat:               wago.Compatibility{Engines: map[string]string{"wago": ">=0.1.0"}},
	}
}

func (p *Plugin) Register(reg *wago.Registry) error {
	manager, err := reg.ManagedInstances()
	if err != nil {
		return err
	}
	lifecycle, err := reg.InstanceLifecycle()
	if err != nil {
		return err
	}
	p.service = newWorkers(manager, p.limits)
	lifecycle.BeforeClose(func(ctx *wago.InstanceContext) { p.service.parentClosing(ctx.Instance) })
	return plugin.Provide(reg, ServiceKey, p.service)
}

func (p *Plugin) Stop(context.Context) error {
	if p == nil || p.service == nil {
		return nil
	}
	return p.service.close()
}

func (p *Plugin) Service() *Workers {
	if p == nil {
		return nil
	}
	return p.service
}

func init() { wago.RegisterExtension(PluginName, func() wago.Extension { return New() }) }

type Workers struct {
	mu         sync.Mutex
	manager    *wago.InstanceManager
	limits     WorkerLimits
	live       uint32 // number of workers currently holding a quota reservation
	queueBytes uint64 // total per-worker queue-byte reservation currently held
	next       WorkerID
	workers    map[WorkerID]*worker
	byInstance map[*wago.Instance]*worker
	messages   []func(*MessageContext) error
	exits      []func(*WorkerExitContext)
	closed     bool
	exitPanics []error
}

func newWorkers(manager *wago.InstanceManager, limits WorkerLimits) *Workers {
	return &Workers{manager: manager, limits: normalizeLimits(limits), next: 1,
		workers: map[WorkerID]*worker{}, byInstance: map[*wago.Instance]*worker{}}
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

func (w *Workers) OnMessage(fns ...func(*MessageContext) error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.messages = append(w.messages, fns...)
	}
}
func (w *Workers) OnExit(fns ...func(*WorkerExitContext)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.exits = append(w.exits, fns...)
	}
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
	parent, err := w.manager.Caller(caller)
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
	wr := &worker{owner: w, id: id, creator: parent, instance: child, tableIndex: tableIndex,
		queue: make([]message, opts.QueueCapacity), maxPayload: opts.MaxPayloadBytes, maxQueueBytes: opts.MaxQueueBytes,
		wake: make(chan struct{}, 1), done: make(chan struct{}), messages: append([]func(*MessageContext) error(nil), w.messages...), exits: append([]func(*WorkerExitContext){}, w.exits...)}
	w.workers[id], w.byInstance[child.Instance()] = wr, wr
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
	wr := w.byInstance[owned.Instance()]
	w.mu.Unlock()
	if wr == nil {
		return 0, ErrWorkerNotFound
	}
	return wr.id, nil
}

func (w *Workers) DispatchNext(caller wago.HostModule) error {
	owned, err := w.manager.ManagedCaller(caller)
	if err != nil {
		return ErrInvalidWorkerCaller
	}
	w.mu.Lock()
	wr := w.byInstance[owned.Instance()]
	w.mu.Unlock()
	if wr == nil {
		return ErrWorkerNotFound
	}
	return wr.dispatch(caller)
}

func (w *Workers) Link(caller wago.HostModule, childID WorkerID) error {
	parent, err := w.manager.Caller(caller)
	if err != nil {
		return ErrInvalidWorkerCaller
	}
	wr, err := w.owned(childID)
	if err != nil {
		return err
	}
	if wr.creator != parent || wr.instance.Instance() == parent {
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
	creator                       *wago.Instance
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
	messages                      []func(*MessageContext) error
	exits                         []func(*WorkerExitContext)
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

func (wr *worker) dispatch(caller wago.HostModule) error {
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
			for _, fn := range wr.messages {
				if err := fn(ctx); err != nil {
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
	in := wr.instance.Instance()
	_ = wr.instance.Close()
	wr.owner.mu.Lock()
	delete(wr.owner.workers, wr.id)
	delete(wr.owner.byInstance, in)
	wr.owner.mu.Unlock()
	ctx := &WorkerExitContext{WorkerID: wr.id, Kind: kind, Err: err}
	for i, fn := range wr.exits {
		func() {
			defer func() {
				if v := recover(); v != nil {
					wr.owner.mu.Lock()
					wr.owner.exitPanics = append(wr.owner.exitPanics, fmt.Errorf("worker %d exit observer %d: %v", wr.id, i, v))
					wr.owner.mu.Unlock()
				}
			}()
			fn(ctx)
		}()
	}
	// Release aggregate quota only after the instance is closed and every exit
	// observer has run, so a concurrent Spawn cannot exceed the ceiling while this
	// worker is still finalizing.
	wr.owner.release(wr.maxQueueBytes)
	close(wr.done)
}

func (w *Workers) parentClosing(parent *wago.Instance) {
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
	errs := append([]error(nil), w.exitPanics...)
	w.exitPanics = nil
	w.mu.Unlock()
	return errors.Join(errs...)
}
