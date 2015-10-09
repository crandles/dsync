package dsync

import (
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
)

type etcdMutex struct {
	backoff BackoffFunc
	ctx     context.Context
	etcd    client.KeysAPI
	key     string
	locked  bool
	m       sync.Mutex
	refresh time.Duration
	stop    chan bool
	ttl     time.Duration
	uuid    string
	wg      sync.WaitGroup
}

// NewETCDMutex creates an etcd-based mutex.
//
// The refresh duration determines the interval to which a keep alive goroutine
// will update the TTL of the lock key while a lock is held.
//
// The ttl duration is used to set the ETCD key TTL.
//
// The BackoffFunc defines the back-off method to use when obtaining the remote lock.
//
// The Lock() function will block until it obtains a lock on the given key.
// It will use the given BackoffFunc to determine the duration to sleep between lock attempts.
// It will listen on the given context's Done channel,
// and abort the locking process if the context is canceled.
// -- If the context's Done channel is closed, the mutex will not be usable.
//
// The Unlock() function will block until it can free the lock.
// If no lock is currently held by the Mutex, it will return.
// If the lock is held by a different Mutex, it will return.
func NewETCDMutex(ctx context.Context, etcd client.KeysAPI, key string, refresh time.Duration, ttl time.Duration, backoff BackoffFunc) *etcdMutex {
	return &etcdMutex{
		backoff: backoff,
		ctx:     ctx,
		etcd:    etcd,
		key:     key,
		refresh: refresh,
		ttl:     ttl,
		uuid:    uuid.New(),
	}
}

func (e *etcdMutex) Lock() {
	var i int64
	e.m.Lock()
	defer e.m.Unlock()
	for e.locked != false {
		// The lock is already held within this process.
		// Wait for the lock to be unlocked.
	}
	e.stop = make(chan bool)
	// Error if the previous value of the key is not empty.
	options := client.SetOptions{
		PrevExist: client.PrevNoExist,
		TTL:       e.ttl,
	}
	for i = 0; e.locked != true; i++ {
		select {
		case <-e.ctx.Done():
			// The parent context may decide to Timeout or Cancel,
			// in that case, stop trying to obtain the lock.
			return
		default:
			if _, err := e.etcd.Set(e.ctx, e.key, e.uuid, &options); err == nil {
				e.locked = true
				go e.keepAlive() // Keep the lock alive by refreshing the TTL.
				return
			}
			if i < 0 { // Safeguard against potential overflows.
				i = 0
			}
			time.Sleep(e.backoff(i))
		}
	}
}

func (e *etcdMutex) keepAlive() {
	e.wg.Add(1)
	interval := time.NewTicker(e.refresh)
	// Only update the TTL if the previous value is the mutex's uuid
	options := client.SetOptions{
		PrevValue: e.uuid,
		TTL:       e.ttl,
	}
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
		e.m.Unlock()
		return // TODO: sync.Mutex would panic here. Should we?
	}
	e.stop <- true // Stop the TTL keepAlive goroutine.
	e.wg.Wait()
	// Only perform the delete if the previous value is the mutex's uuid
	options := client.DeleteOptions{
		PrevValue: e.uuid,
	}
	// Ignore any error here -- there is a chance that between stopping the keepAlive
	// and sending the Delete that another process obtained the lock.
	e.etcd.Delete(e.ctx, e.key, &options)
	e.locked = false
	close(e.stop)
	e.m.Unlock()
}
