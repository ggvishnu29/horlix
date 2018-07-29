package model

import "sync"

/* Lock struct implements Lock/UnLock method. This can be used
   to serialize/synchronize access to datastructures when multiple
   goroutines tries to access the data
*/
type Lock struct {
	sync.Mutex
}

func (l *Lock) Lock() {
	l.Mutex.Lock()
}

func (l *Lock) UnLock() {
	l.Mutex.Unlock()
}
