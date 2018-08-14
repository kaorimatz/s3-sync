package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/robfig/cron"
)

type runner interface {
	run(ctx context.Context) error
}

func newRunner(specs []*syncSpec, tags []string, oneshot bool, stopTimeout time.Duration) (runner, error) {
	awsClientFactory, err := newDefaultAWSClientFactory()
	if err != nil {
		return nil, err
	}

	if oneshot {
		return &oneshotRunner{
			awsClientFactory: awsClientFactory,
			specs:            specs,
			tags:             tags,
		}, nil
	} else {
		return &cronRunner{
			awsClientFactory: awsClientFactory,
			c:                cron.New(),
			specs:            specs,
			stopTimeout:      stopTimeout,
			tags:             tags,
		}, nil
	}
}

type oneshotRunner struct {
	awsClientFactory awsClientFactory
	specs            []*syncSpec
	tags             []string
}

func (r *oneshotRunner) run(ctx context.Context) error {
	if err := r.sync(ctx); err != nil {
		return err
	}

	if err := r.build(ctx); err != nil {
		return err
	}

	return nil
}

func (r *oneshotRunner) sync(ctx context.Context) error {
	log.Println("Starting syncing...")
	for _, s := range r.specs {
		syncer := newSyncer(s.region, s.bucket, s.prefix, s.dst, r.awsClientFactory)
		if _, err := syncer.sync(ctx); err != nil {
			return fmt.Errorf("error syncing: %v", err)
		}
	}
	log.Println("Finished syncing")
	return nil
}

func (r *oneshotRunner) build(ctx context.Context) error {
	if r.tags == nil {
		return nil
	}

	builder, err := newBuilderFromSyncSpecs(r.tags, r.specs, r.awsClientFactory)
	if err != nil {
		return err
	}

	log.Println("Starting building image...")
	if err := builder.build(ctx); err != nil {
		return fmt.Errorf("error building image: %v", err)
	}
	log.Println("Finished building image")

	return nil
}

type cronRunner struct {
	awsClientFactory awsClientFactory
	buildCh          chan struct{}
	c                *cron.Cron
	cancelCtx        context.Context
	cancelFunc       context.CancelFunc
	mutex            sync.RWMutex
	specs            []*syncSpec
	tags             []string
	stopCtx          context.Context
	stopFunc         context.CancelFunc
	stopTimeout      time.Duration
	wg               sync.WaitGroup
}

func (r *cronRunner) run(ctx context.Context) error {
	r.stopCtx, r.stopFunc = context.WithCancel(ctx)
	r.cancelCtx, r.cancelFunc = context.WithCancel(ctx)

	if err := r.startBuilder(); err != nil {
		return err
	}

	if err := r.startSyncers(ctx); err != nil {
		return err
	}

	r.waitSignal()

	r.stop()

	return nil
}

func (r *cronRunner) startSyncers(ctx context.Context) error {
	var syncers []*syncer
	for _, s := range r.specs {
		syncer := newSyncer(s.region, s.bucket, s.prefix, s.dst, r.awsClientFactory)
		if s.schedule == "" || s.onStart {
			syncers = append(syncers, syncer)
		}
		if s.schedule != "" {
			if err := r.scheduleSync(syncer, s.schedule); err != nil {
				return err
			}
		}
	}

	var changed bool
	if len(syncers) > 0 {
		log.Println("Starting syncing...")
		for _, syncer := range syncers {
			c, err := syncer.sync(ctx)
			if err != nil {
				log.Printf("Error syncing: %v\n", err)
			}
			changed = changed || c
		}
		log.Println("Finished syncing")
	}

	if changed && r.buildCh != nil {
		r.buildCh <- struct{}{}
	}

	r.c.Start()

	return nil
}

func (r *cronRunner) scheduleSync(syncer *syncer, schedule string) error {
	guardCh := make(chan struct{}, 1)
	f := func() { r.runSync(syncer, guardCh) }
	if err := r.c.AddFunc(schedule, f); err != nil {
		return err
	}
	return nil
}

func (r *cronRunner) runSync(syncer *syncer, guardCh chan struct{}) {
	r.wg.Add(1)
	defer r.wg.Done()

	select {
	case guardCh <- struct{}{}:
		if r.stopCtx.Err() != nil {
			break
		}
		r.sync(syncer)
		<-guardCh
	default:
		log.Println("A previous job is still running")
		return
	}
}

func (r *cronRunner) sync(syncer *syncer) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	log.Println("Starting syncing...")
	changed, err := syncer.sync(r.cancelCtx)
	if err != nil {
		log.Printf("Error syncing: %v\n", err)
		return
	}
	log.Println("Finished syncing")

	if changed && r.buildCh != nil {
		r.buildCh <- struct{}{}
	}
}

func (r *cronRunner) startBuilder() error {
	if r.tags == nil {
		return nil
	}

	builder, err := newBuilderFromSyncSpecs(r.tags, r.specs, r.awsClientFactory)
	if err != nil {
		return err
	}

	r.buildCh = make(chan struct{}, len(r.specs))

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.buildCh:
				if r.stopCtx.Err() != nil {
					return
				}
				r.build(builder)
			case <-r.stopCtx.Done():
				return
			}
		}
	}()

	return nil
}

func (r *cronRunner) build(builder *builder) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Println("Starting building image...")
	if err := builder.build(r.cancelCtx); err != nil {
		log.Printf("Error building image: %v\n", err)
		return
	}
	log.Println("Finished building image")
}

func (r *cronRunner) waitSignal() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	signal := <-signalCh
	log.Printf("Received a shutdown signal: %s\n", signal)
	return
}

func (r *cronRunner) stop() {
	r.c.Stop()
	r.stopFunc()

	timer := time.NewTimer(r.stopTimeout)
	select {
	case <-timer.C:
		log.Println("Stop timeout is exceeded. Cancelling jobs...")
		r.cancelFunc()
	case <-r.waitCh():
		log.Println("All jobs have been stopped")
		timer.Stop()
	}
}

func (r *cronRunner) waitCh() <-chan struct{} {
	waitCh := make(chan struct{})
	go func() {
		defer close(waitCh)
		r.wg.Wait()
	}()
	return waitCh
}
