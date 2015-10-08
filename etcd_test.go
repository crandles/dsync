package dsync

import (
	"fmt"
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"os"
	"testing"
	"time"
)

var (
	etcd client.KeysAPI
)

func init() {
	node := os.Getenv("ETCD_NODE")
	if node == "" {
		node = "http://localhost:2379"
	}
	cfg := client.Config{Endpoints: []string{node}}
	c, err := client.New(cfg)
	if err != nil {
		fmt.Println("Error establishing connection to ETCD for testing.", err)
		os.Exit(1)
	}
	etcd = client.NewKeysAPI(c)
}

func TestETCDMutexSimpleLocking(t *testing.T) {
	ctx := context.Background()
	key := "/dev/locks/testLock"
	refresh := time.Second * 1
	ttl := time.Second * 15
	backoff := ConstantBackoff(time.Second * 2)

	mutex := NewETCDMutex(ctx, etcd, key, refresh, ttl, backoff)
	uuid := mutex.(*etcdMutex).uuid
	mutex.Lock()
	resp, err := etcd.Get(ctx, key, nil)
	if err != nil {
		mutex.Unlock()
		t.Fatalf("ETCD key %s was not defined correctly. Response: %v; Error: %s", key, resp, err)
	}
	if resp.Node.Value != uuid {
		mutex.Unlock()
		t.Fatalf("ETCD Lock is present, but does not match expected value. ETCD key value: %s; Expected value: %s", resp.Node.Value, uuid)
	}
	mutex.Unlock()
	_, err = etcd.Get(ctx, key, nil)
	if etcdError, ok := err.(client.Error); ok && etcdError.Code != client.ErrorCodeKeyNotFound {
		t.Fatalf("ETCD Lock should not be present for key: %s. (%s); Expected: 'ErrorCodeKeyNotFound' - 100;", key, err)
	}
}

func BenchmarkETCDSerialMutexLockingWithVerification(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	key := "/dev/locks/testLockingBenchmark"
	refresh := time.Second * 1
	ttl := time.Second * 15
	backoff := ConstantBackoff(time.Second * 2)
	mutex := NewETCDMutex(ctx, etcd, key, refresh, ttl, backoff)
	uuid := mutex.(*etcdMutex).uuid
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		testMutex(ctx, mutex, key, uuid, false)
	}
}

func BenchmarkETCDSerialMutexLockingWithoutVerification(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	key := "/dev/locks/testQuickLockingBenchmark"
	refresh := time.Second * 1
	ttl := time.Second * 15
	backoff := ConstantBackoff(time.Second * 2)
	mutex := NewETCDMutex(ctx, etcd, key, refresh, ttl, backoff)
	uuid := mutex.(*etcdMutex).uuid
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		testMutex(ctx, mutex, key, uuid, true)
	}
}

func testMutex(ctx context.Context, mutex Mutex, key string, uuid string, quick bool) error {
	mutex.Lock()
	resp, err := etcd.Get(ctx, key, nil)
	if err != nil {
		mutex.Unlock()
		return fmt.Errorf("ETCD key %s was not defined correctly. Response: %v; Error: %s", key, resp, err)
	}
	if quick {
		mutex.Unlock()
		return nil
	}
	if resp.Node.Value != uuid {
		mutex.Unlock()
		return fmt.Errorf("ETCD Lock is present, but does not match expected value. ETCD key value: %s; Expected value: %s", resp.Node.Value, uuid)
	}
	mutex.Unlock()
	_, err = etcd.Get(ctx, key, nil)
	if etcdError, ok := err.(client.Error); ok && etcdError.Code != client.ErrorCodeKeyNotFound {
		return fmt.Errorf("ETCD Lock should not be present for key: %s. (%s); Expected: 'ErrorCodeKeyNotFound' - 100;", key, err)
	}
	return nil
}

func ExampleETCDMutex() {
	c, err := client.New(client.Config{Endpoints: []string{"http://localhost:2379"}})
	if err != nil {
		// handle ETCD client error
	}
	etcd = client.NewKeysAPI(c)
	ctx := context.Background()
	refresh := time.Second * 1
	ttl := time.Second * 15
	backoff := ConstantBackoff(time.Second * 2)

	mutex := NewETCDMutex(ctx, etcd, "/path/to/lock/key", refresh, ttl, backoff)
	mutex.Lock()
	// Do work
	mutex.Unlock()
}
