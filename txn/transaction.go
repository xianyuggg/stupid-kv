package txn

import (
	"stupid-kv/base"
	"stupid-kv/kv"
	log "stupid-kv/logutil"
	"sync"
	"sync/atomic"
)

type Manager struct {
	curTid   base.Tid
	writeSet map[base.Tid]map[base.KeyT]int
	readSet  map[base.Tid]map[base.KeyT]int
	lockMap  map[base.KeyT]*sync.RWMutex
	guard    sync.Mutex
}

var instance *Manager
var once sync.Once

func GetManagerInstance() *Manager {
	once.Do(func() {
		log.Info("Transaction manager starts to init")
		instance = &Manager{
			curTid:   0,
			lockMap:  make(map[base.KeyT]*sync.RWMutex),
			writeSet: make(map[base.Tid]map[base.KeyT]int),
			readSet:  make(map[base.Tid]map[base.KeyT]int),
			guard:    sync.Mutex{},
		}
	})
	return instance
}

func (m *Manager) acquireReadLock(key base.KeyT, tid base.Tid) {
	m.lockMap[key].RLock()
	m.readSet[tid][key] = 1
}

func (m *Manager) acquireWriteLock(key base.KeyT, tid base.Tid) {
	m.guard.Lock()
	if _, ok := m.lockMap[key]; !ok {
		m.lockMap[key] = &sync.RWMutex{}
	}
	m.guard.Unlock()
	m.lockMap[key].Lock()
	m.writeSet[tid][key] = 1

}

func (m *Manager) releaseReadLock(key base.KeyT, tid base.Tid) {
	m.lockMap[key].RUnlock()
}

func (m *Manager) releaseWriteLock(key base.KeyT, tid base.Tid) {
	m.lockMap[key].Unlock()
}

func (m *Manager) AllocateNewTid() base.Tid {
	retVal := atomic.LoadInt64((*int64)(&m.curTid))
	atomic.AddInt64((*int64)(&m.curTid), 1)
	return base.Tid(retVal)
}

func (m *Manager) BeginTxn() base.Tid {
	newTid := m.AllocateNewTid()
	m.guard.Lock()
	m.writeSet[newTid] = make(map[base.KeyT]int)
	m.readSet[newTid] = make(map[base.KeyT]int)
	m.guard.Unlock()
	return newTid
}

func (m *Manager) Set(key base.KeyT, value base.ValueT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)
	prevValue := kvStore.Get(key, tid)
	kvStore.Set(key, value, tid)

	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:    opSet,
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

func (m *Manager) Add(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)

	prevValue := kvStore.Get(key, tid)
	if prevValue == base.VALUE_NOT_FOUND {
		err := m.AbortTxn(tid)
		if err != nil {
			return err
		}
	}

	kvStore.Add(key, tid)
	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:    opAdd,
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
	if writeSet, ok := m.writeSet[tid]; ok {
		for k, _ := range writeSet {
			m.releaseWriteLock(k, tid)
		}
	} else {
		log.Error("txn not exist")
	}
	if readSet, ok := m.readSet[tid]; ok {
		for k, _ := range readSet {
			m.releaseReadLock(k, tid)
		}
	} else {
		log.Error("txn not exist")
	}

	delete(m.writeSet, tid)
	delete(m.readSet, tid)
	// TODO: I'm not sure
	kv.GetManagerInstance().Flush()
	return nil
}

func (m *Manager) AbortTxn(tid base.Tid) error {
	// probably release all locks, and do a replay
	ops := GetUndoLoggerInstance().GetTidOps(tid)
	store := kv.GetManagerInstance()
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]

		if op.op == opAdd {
			store.Set(op.key, op.value, tid)
		} else if op.op == opSet {
			if op.value == base.VALUE_NOT_FOUND {
				store.Del(op.key, tid)
			} else {
				store.Set(op.key, op.value, tid)
			}
		} else if op.op == opDel {
			store.Set(op.key, op.value, tid)
		} else {
			log.Error("op unknown")
		}
	}

	// release all locks
	if writeSet, ok := m.writeSet[tid]; ok {
		for k, _ := range writeSet {
			m.releaseWriteLock(k, tid)
		}
	} else {
		log.Error("txn not exist")
	}
	if readSet, ok := m.readSet[tid]; ok {
		for k, _ := range readSet {
			m.releaseReadLock(k, tid)
		}
	} else {
		log.Error("txn not exist")
	}

	delete(m.writeSet, tid)
	delete(m.readSet, tid)
	kv.GetManagerInstance().Flush()
	return nil
}