package txn

import (
	"stupid-kv/base"
	"sync"
)

//type Manager struct {
//	curTid base.Tid
//
//	writeSet *sync.Map
//	//writeSet map[base.Tid]map[base.KeyT]int
//	readSet *sync.Map
//	//readSet map[base.Tid]map[base.KeyT]int
//	lockMap *sync.Map
//	tidsGuard   *sync.Mutex
//
//	curActiveTids []base.Tid
//}

//func (m *Manager) acquireReadLock(key base.KeyT, tid base.Tid) {
//	tmp, _ := m.readSet.Load(tid)
//	rs := tmp.(*sync.Map)
//	if _, ok := rs.Load(key); !ok {
//		rs.Store(key, 1)
//		if rw, ok := m.lockMap.Load(key); ok {
//			rw.(*sync.RWMutex).RLock()
//		}
//	}
//}
//
func (m *Manager) acquireWriteLock(key base.KeyT, tid base.Tid) {
	tmp, _ := m.tid2writeSet.Load(tid)
	ws := tmp.(*sync.Map)
	if _, ok := ws.Load(key); !ok {
		ws.Store(key, 1)
		if _, ok := m.key2lock.Load(key); !ok {
			m.key2lock.Store(key, &sync.RWMutex{})
		}
		rw, _ := m.key2lock.Load(key)
		rw.(*sync.RWMutex).Lock()
	}
}

//
//func (m *Manager) releaseReadLock(key base.KeyT, tid base.Tid) {
//	if rw, ok := m.lockMap.Load(key); ok {
//		rw.(*sync.RWMutex).RUnlock()
//	}
//}
//
func (m *Manager) releaseWriteLock(key base.KeyT, tid base.Tid) {
	if rw, ok := m.key2lock.Load(key); ok {
		rw.(*sync.RWMutex).Unlock()
	}
}
