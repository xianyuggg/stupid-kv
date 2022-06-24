package txn

//type Manager struct {
//	curTid base.Tid
//
//	writeSet *sync.Map
//	//writeSet map[base.Tid]map[base.KeyT]int
//	readSet *sync.Map
//	//readSet map[base.Tid]map[base.KeyT]int
//	lockMap *sync.Map
//	guard   *sync.Mutex
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
//func (m *Manager) acquireWriteLock(key base.KeyT, tid base.Tid) {
//	tmp, _ := m.writeSet.Load(tid)
//	ws := tmp.(*sync.Map)
//	if _, ok := ws.Load(key); !ok {
//		ws.Store(key, 1)
//		if _, ok := m.lockMap.Load(key); !ok {
//			m.lockMap.Store(key, &sync.RWMutex{})
//		}
//		rw, _ := m.lockMap.Load(key)
//		rw.(*sync.RWMutex).Lock()
//	}
//}
//
//func (m *Manager) releaseReadLock(key base.KeyT, tid base.Tid) {
//	if rw, ok := m.lockMap.Load(key); ok {
//		rw.(*sync.RWMutex).RUnlock()
//	}
//}
//
//func (m *Manager) releaseWriteLock(key base.KeyT, tid base.Tid) {
//	if rw, ok := m.lockMap.Load(key); ok {
//		rw.(*sync.RWMutex).Unlock()
//	}
//}
