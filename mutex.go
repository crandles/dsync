package dsync

type Mutex interface {
	Lock()
	Unlock()
}
