package dsync

import (
	"github.com/coreos/etcd/client"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"sync"
	"time"
)

type etcdMutex struct {
	backoff BackoffFunc
	ctx     context.Context
	etcd    client.KeysAPI
	key     string
	locked  bool
	m       sync.Mutex
	refresh time.Duration
	stop    chan struct{}
	ttl     time.Duration
	uuid    string
	wg      sync.WaitGroup
}

func (e *etcdMutex) Lock() {
	var i int64
	e.m.Lock()
	for e.locked != false {
		// The lock is already held within this process.
		// Wait for the lock to be unlocked.
	}
	e.stop = make(chan struct{})
	options := client.SetOptions{PrevExist: client.PrevNoExist, TTL: e.ttl}
	for i = 0; e.locked != true; i++ {
		select {
		case <-e.ctx.Done():
			// The parent context may decide to Timeout or Cancel,
			// in that case, stop trying to obtain the lock.
			return
		default:
			_, err := e.etcd.Set(e.ctx, e.key, e.uuid, &options)
			if err == nil {
				e.locked = true
				break
			}
			if i < 0 { // Safeguard against potential overflows.
				i = 0
			}
			time.Sleep(e.backoff(i))
		}
	}
	go e.keepAlive() // Keep the lock alive by refreshing the TTL.
	e.m.Unlock()
}

func (e *etcdMutex) keepAlive() {
	e.wg.Add(1)
	interval := time.NewTicker(e.refresh)
	options := client.SetOptions{PrevValue: e.uuid, TTL: e.ttl}
	for {
		select {
		case <-interval.C:
			e.etcd.Set(e.ctx, e.key, e.uuid, &options) // TOTDO: Should this panic on error?
		case <-e.stop:
			// Unlock has been called, stop refreshing the TTL.
			interval.Stop()
			e.wg.Done()
			return
		case <-e.ctx.Done():
			// The parent context has been canceled.
			interval.Stop()
			e.wg.Done()
			return
		}
	}
}

func (e *etcdMutex) Unlock() {
	e.m.Lock()
	if e.locked == false {
		return // TODO: sync.Mutex would panic here. Should we?
	}
	e.stop <- struct{}{} // Stop the TTL keepAlive goroutine.
	e.wg.Wait()
	options := client.DeleteOptions{PrevValue: e.uuid}
	// Ignore any error here -- there is a chance that between stopping the keepAlive
	// and sending the Delete that another process obtained the lock.
	e.etcd.Delete(e.ctx, e.key, &options)
	e.locked = false
	close(e.stop)
	e.m.Unlock()
}

func NewETCDMutex(ctx context.Context, etcd client.KeysAPI, key string, refresh time.Duration, ttl time.Duration, backoff BackoffFunc) Mutex {
	var e etcdMutex = etcdMutex{
		backoff: backoff,
		ctx:     ctx,
		etcd:    etcd,
		key:     key,
		refresh: refresh,
		ttl:     ttl,
		uuid:    uuid.New(),
	}
	return &e
}
