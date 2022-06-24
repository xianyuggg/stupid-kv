package txn

import (
	"stupid-kv/base"
	"stupid-kv/kv"
	log "stupid-kv/logutil"
	"sync"
	"sync/atomic"
)

type Manager struct {
	curTid base.Tid

	writeSet *sync.Map
	//writeSet map[base.Tid]map[base.KeyT]int
	readSet *sync.Map
	//readSet map[base.Tid]map[base.KeyT]int
	lockMap *sync.Map
	//lockMap map[base.KeyT]*sync.RWMutex
	//wsGuard *sync.Mutex
	//rsGuard *sync.Mutex
	//lmGuard *sync.Mutex
	guard *sync.Mutex
}

var instance *Manager
var once sync.Once

func GetManagerInstance() *Manager {
	once.Do(func() {
		log.Info("Transaction manager starts to init")
		instance = &Manager{
			curTid:   0,
			lockMap:  &sync.Map{},
			writeSet: &sync.Map{},
			readSet:  &sync.Map{},
			guard:    &sync.Mutex{},
		}
	})
	return instance
}

func (m *Manager) acquireReadLock(key base.KeyT, tid base.Tid) {
	tmp, _ := m.readSet.Load(tid)
	rs := tmp.(*sync.Map)
	if _, ok := rs.Load(key); !ok {
		rs.Store(key, 1)
		if rw, ok := m.lockMap.Load(key); ok {
			rw.(*sync.RWMutex).RLock()
		}
	}
}

func (m *Manager) acquireWriteLock(key base.KeyT, tid base.Tid) {
	tmp, _ := m.writeSet.Load(tid)
	ws := tmp.(*sync.Map)
	if _, ok := ws.Load(key); !ok {
		ws.Store(key, 1)
		if _, ok := m.lockMap.Load(key); !ok {
			m.lockMap.Store(key, &sync.RWMutex{})
		}
		rw, _ := m.lockMap.Load(key)
		rw.(*sync.RWMutex).Lock()
	}
}

func (m *Manager) releaseReadLock(key base.KeyT, tid base.Tid) {
	if rw, ok := m.lockMap.Load(key); ok {
		rw.(*sync.RWMutex).RUnlock()
	}
}

func (m *Manager) releaseWriteLock(key base.KeyT, tid base.Tid) {
	if rw, ok := m.lockMap.Load(key); ok {
		rw.(*sync.RWMutex).Unlock()
	}
}

func (m *Manager) AllocateNewTid() base.Tid {
	m.guard.Lock()
	// seems atomic doesn't work
	retVal := atomic.LoadInt64((*int64)(&m.curTid))
	atomic.AddInt64((*int64)(&m.curTid), 1)

	m.guard.Unlock()

	return base.Tid(retVal)
}

func (m *Manager) BeginTxn() base.Tid {
	newTid := m.AllocateNewTid()

	m.writeSet.Store(newTid, &sync.Map{})
	m.readSet.Store(newTid, &sync.Map{})

	log.Infof("txn %v start", newTid)
	return newTid
}

func (m *Manager) Put(key base.KeyT, value base.ValueT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)
	prevValue := kvStore.Get(key, tid)
	kvStore.Put(key, value, tid)

	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:    opPut,
		key:   key,
		value: prevValue,
	})
	return nil
}

func (m *Manager) Get(key base.KeyT, tid base.Tid) base.ValueT {
	kvStore := kv.GetManagerInstance()
	m.acquireReadLock(key, tid)
	return kvStore.Get(key, tid)
}

func (m *Manager) Inc(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)

	prevValue := kvStore.Get(key, tid)
	if prevValue == base.VALUE_NOT_FOUND {
		err := m.AbortTxn(tid)
		if err != nil {
			return err
		}
	}
	kvStore.Inc(key, tid)
	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:    opInc,
		key:   key,
		value: prevValue,
	})
	return nil
}

func (m *Manager) Dec(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)

	prevValue := kvStore.Get(key, tid)
	if prevValue == base.VALUE_NOT_FOUND {
		err := m.AbortTxn(tid)
		if err != nil {
			return err
		}
	}
	kvStore.Dec(key, tid)
	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:    opDec,
		key:   key,
		value: prevValue,
	})
	return nil
}

func (m *Manager) Del(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)

	prevValue := kvStore.Get(key, tid)
	if prevValue == base.VALUE_NOT_FOUND {
		err := m.AbortTxn(tid)
		if err != nil {
			return err
		}
	}

	kvStore.Del(key, tid)
	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:    opDel,
		key:   key,
		value: prevValue,
	})
	return nil
}

func (m *Manager) CommitTxn(tid base.Tid) error {
	if ws, ok := m.writeSet.Load(tid); ok {
		ws.(*sync.Map).Range(func(key, value interface{}) bool {
			m.releaseWriteLock(key.(base.KeyT), tid)
			return true
		})
	} else {
		log.Error("txn not exist")
	}
	if rs, ok := m.readSet.Load(tid); ok {
		rs.(*sync.Map).Range(func(key, value interface{}) bool {
			m.releaseReadLock(key.(base.KeyT), tid)
			return true
		})
	} else {
		log.Error("txn not exist")
	}

	m.writeSet.Delete(tid)
	m.readSet.Delete(tid)

	// TODO: I'm not sure
	kv.GetManagerInstance().Flush()
	log.Infof("txn %v commit", tid)
	return nil
}

func (m *Manager) AbortTxn(tid base.Tid) error {
	// probably release all locks, and do a replay
	ops := GetUndoLoggerInstance().GetTidOps(tid)
	store := kv.GetManagerInstance()
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]

		if op.op == opInc {
			store.Put(op.key, op.value, tid)
		} else if op.op == opPut {
			if op.value == base.VALUE_NOT_FOUND {
				store.Del(op.key, tid)
			} else {
				store.Put(op.key, op.value, tid)
			}
		} else if op.op == opDel {
			store.Put(op.key, op.value, tid)
		} else if op.op == opDec {
			store.Put(op.key, op.value, tid)
		} else {
			log.Error("op unknown")
		}
	}

	// release all locks
	if ws, ok := m.writeSet.Load(tid); ok {
		ws.(*sync.Map).Range(func(key, value interface{}) bool {
			m.releaseWriteLock(key.(base.KeyT), tid)
			return true
		})
	} else {
		log.Error("txn not exist")
	}
	if rs, ok := m.readSet.Load(tid); ok {
		rs.(*sync.Map).Range(func(key, value interface{}) bool {
			m.releaseWriteLock(key.(base.KeyT), tid)
			return true
		})
	} else {
		log.Error("txn not exist")
	}

	m.writeSet.Delete(tid)
	m.readSet.Delete(tid)

	kv.GetManagerInstance().Flush()
	log.Infof("txn %v abort", tid)
	return nil
}
