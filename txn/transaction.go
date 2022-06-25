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

	tidsGuard  *sync.Mutex
	flushGuard *sync.Mutex

	curActiveTids []base.Tid

	commitChannel chan base.Tid

	tid2writeSet *sync.Map
	key2lock     *sync.Map // used for protect write-write conflict
}

var instance *Manager
var once sync.Once

func GetManagerInstance() *Manager {
	once.Do(func() {
		log.Info("Transaction manager starts to init")
		instance = &Manager{
			curTid:        0,
			tidsGuard:     &sync.Mutex{},
			flushGuard:    &sync.Mutex{},
			curActiveTids: make([]base.Tid, 0),
			commitChannel: make(chan base.Tid, 200000),

			tid2writeSet: &sync.Map{},
			key2lock:     &sync.Map{},
		}
		instance.Load()
	})
	return instance
}

func (m *Manager) GetCurrentTid() base.Tid {
	return m.curTid
}

func (m *Manager) AllocateNewTid() base.Tid {
	m.tidsGuard.Lock()
	defer m.tidsGuard.Unlock()
	// seems atomic doesn't work
	retVal := atomic.LoadInt64((*int64)(&m.curTid))
	atomic.AddInt64((*int64)(&m.curTid), 1)
	return base.Tid(retVal)
}

func (m *Manager) BeginTxn() base.Tid {
	newTid := m.AllocateNewTid()
	m.tidsGuard.Lock()
	m.curActiveTids = append(m.curActiveTids, newTid)
	m.FlushTid()
	m.tidsGuard.Unlock()

	m.tid2writeSet.Store(newTid, &sync.Map{})
	log.Infof("txn %v start", newTid)
	return newTid
}

func remove(l []base.Tid, item base.Tid) []base.Tid {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return l
}

func (m *Manager) CommitTxn(tid base.Tid) error {
	m.tidsGuard.Lock()
	m.curActiveTids = remove(m.curActiveTids, tid)
	m.FlushTid()
	m.tidsGuard.Unlock()
	kv.GetManagerInstance().Flush()
	log.Infof("txn %v commit", tid)
	m.commitChannel <- tid
	if ws, ok := m.tid2writeSet.Load(tid); ok {
		ws.(*sync.Map).Range(func(key, value interface{}) bool {
			m.releaseWriteLock(key.(base.KeyT), tid)
			return true
		})
	} else {
		log.Error("txn not exist")
	}
	m.tid2writeSet.Delete(tid)

	return nil
}

func (m *Manager) AbortTxn(tid base.Tid) error {
	m.tidsGuard.Lock()
	m.curActiveTids = remove(m.curActiveTids, tid)
	m.FlushTid()
	m.tidsGuard.Unlock()

	for _, txnOp := range GetUndoLoggerInstance().GetTidOps(tid) {
		kv.GetManagerInstance().UnrollKeyByTid(txnOp.key, tid)
	}

	kv.GetManagerInstance().Flush()

	if ws, ok := m.tid2writeSet.Load(tid); ok {
		ws.(*sync.Map).Range(func(key, value interface{}) bool {
			m.releaseWriteLock(key.(base.KeyT), tid)
			return true
		})
	} else {
		log.Error("txn not exist")
	}
	m.tid2writeSet.Delete(tid)
	log.Infof("txn %v abort", tid)
	return nil
}

func (m *Manager) Put(key base.KeyT, value base.ValueT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)
	kvStore.Put(key, value, tid)
	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:  opPut,
		key: key,
	})

	return nil
}

func (m *Manager) Get(key base.KeyT, tid base.Tid) base.ValueT {
	kvStore := kv.GetManagerInstance()

	ret := kvStore.Get(key, tid, m.curActiveTids)

	if ret == base.VALUE_NOT_COMMIT {
		// wait until value is commit
		log.Infof("tid %v: read uncommitted value and wait", tid)
		for {
			var c = <-m.commitChannel
			log.Info("Txn", c, "committed and channel value recv")
			ret := kvStore.Get(key, tid, m.curActiveTids)
			if ret == base.VALUE_NOT_COMMIT {
				continue
			} else {
				return ret
			}
		}
	}

	return ret
}

func (m *Manager) Inc(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)
	kvStore.Inc(key, tid)

	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:  opInc,
		key: key,
	})

	return nil
}

func (m *Manager) Dec(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)
	kvStore.Dec(key, tid)

	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:  opDec,
		key: key,
	})

	return nil
}

func (m *Manager) Del(key base.KeyT, tid base.Tid) error {
	kvStore := kv.GetManagerInstance()
	m.acquireWriteLock(key, tid)
	kvStore.Del(key, tid)

	GetUndoLoggerInstance().AppendOp(tid, TxnOp{
		op:  opDec,
		key: key,
	})

	return nil
}

//func (m *Manager) AbortTxn(tid base.Tid) error {
//	// probably release all locks, and do a replay
//	ops := GetUndoLoggerInstance().GetTidOps(tid)
//	store := kv.GetManagerInstance()
//	for i := len(ops) - 1; i >= 0; i-- {
//		op := ops[i]
//
//		if op.op == opInc {
//			store.Put(op.key, op.value, tid)
//		} else if op.op == opPut {
//			if op.value == base.VALUE_NOT_FOUND {
//				store.Del(op.key, tid)
//			} else {
//				store.Put(op.key, op.value, tid)
//			}
//		} else if op.op == opDel {
//			store.Put(op.key, op.value, tid)
//		} else if op.op == opDec {
//			store.Put(op.key, op.value, tid)
//		} else {
//			log.Error("op unknown")
//		}
//	}
//
//	// release all locks
//	if ws, ok := m.writeSet.Load(tid); ok {
//		ws.(*sync.Map).Range(func(key, value interface{}) bool {
//			m.releaseWriteLock(key.(base.KeyT), tid)
//			return true
//		})
//	} else {
//		log.Error("txn not exist")
//	}
//	if rs, ok := m.readSet.Load(tid); ok {
//		rs.(*sync.Map).Range(func(key, value interface{}) bool {
//			m.releaseWriteLock(key.(base.KeyT), tid)
//			return true
//		})
//	} else {
//		log.Error("txn not exist")
//	}
//
//	m.writeSet.Delete(tid)
//	m.readSet.Delete(tid)
//
//	kv.GetManagerInstance().Flush()
//	log.Infof("txn %v abort", tid)
//	return nil
//}
