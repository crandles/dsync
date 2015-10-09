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
		stop:    make(chan bool),
		ttl:     ttl,
	}
}

func (e *etcdMutex) Lock() {
	e.m.Lock()
	defer e.m.Unlock()
	// Error if the previous value of the key is not empty.
	options := client.SetOptions{
		PrevExist: client.PrevNoExist,
		TTL:       e.ttl,
	}
	id := uuid.New()
	var i int64
	for {
		select {
		case <-e.ctx.Done():
			// The parent context may decide to Timeout or Cancel,
			// in that case, stop trying to obtain the lock.
			return
		case <-time.After(e.backoff(i)):
			if _, err := e.etcd.Set(e.ctx, e.key, id, &options); err == nil {
				e.locked = true
				e.uuid = id
				go e.keepAlive(id) // Keep the lock alive by refreshing the TTL.
				return
			}
		}
		i++
		if i < 0 { // Safeguard against potential overflows. TODO: Is this needed?
			i = 0
		}
	}
}

func (e *etcdMutex) keepAlive(id string) {
	// Only update the TTL if the previous value is the mutex's uuid.
	setOptions := client.SetOptions{
		PrevValue: id,
		TTL:       e.ttl,
	}
	// Only perform the delete if the previous value is the mutex's uuid.
	deleteOptions := client.DeleteOptions{
		PrevValue: e.uuid,
	}
	for {
		select {
		case <-time.After(e.refresh):
			e.etcd.Set(e.ctx, e.key, e.uuid, &setOptions) // TOTDO: Should this panic on error?
		case <-e.stop:
			// Unlock has been called, stop refreshing the TTL, ensure the key is deleted.
			e.etcd.Delete(e.ctx, e.key, &deleteOptions)
			return
		case <-e.ctx.Done():
			// The parent context has been canceled, ensure the key is deleted.
			e.etcd.Delete(e.ctx, e.key, &deleteOptions)
			return
		}
	}
}

func (e *etcdMutex) Unlock() {
	e.m.Lock()
	defer e.m.Unlock()
	if e.locked == false {
		return // TODO: sync.Mutex would panic here. Should we?
	}
	e.stop <- true // Stop the TTL keepAlive goroutine.
	e.locked = false
	e.uuid = ""
}
