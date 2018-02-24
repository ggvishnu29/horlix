package model

import "sync"

type Lock struct {
	sync.Mutex
}

func (l *Lock) Lock() {
	l.Mutex.Lock()
}

func (l *Lock) UnLock() {
	l.Mutex.Unlock()
}
