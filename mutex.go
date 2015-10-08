package dsync

// A Mutex represents a mutual exclusion lock.
type Mutex interface {
	Lock()
	Unlock()
}
