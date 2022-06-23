package kv

import (
	"io/ioutil"
	"stupid-kv/base"
	log "stupid-kv/logutil"
	"sync"
)

type ValueSlot struct {
	values []base.ValueT
	tids   []base.Tid // TODO: mvcc implementation
}

type Manager struct {
	//kv map[base.KeyT]ValueSlot
	kv *sync.Map
}

var instance *Manager
var once sync.Once

func GetManagerInstance() *Manager {
	once.Do(func() {
		log.Info("KV manager starts to init")
		instance = &Manager{
			//kv: make(map[base.KeyT]ValueSlot),
			kv: &sync.Map{},
		}
		if _, err := ioutil.ReadFile("DATA.json"); err == nil {
			instance.Load()
		}
	})
	return instance
}

func (m *Manager) Get(key base.KeyT, tid base.Tid) base.ValueT {
	//m.AcquireReadLock(key)
	//defer m.ReleaseReadLock(key)
	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)
		if len(slotCopy.values) != 1 {
			log.Error("slot has more than one record (2PL)")
		}
		return slotCopy.values[0]
	} else {
		return base.VALUE_NOT_FOUND
	}
}

func (m *Manager) Set(key base.KeyT, value base.ValueT, tid base.Tid) {
	//m.AcquireWriteLock(key)
	//defer m.ReleaseWriteLock(key)
	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)
		slotCopy.values[0] = value
		slotCopy.tids[0] = tid
		m.kv.Store(key, slotCopy)
	} else {
		m.kv.Store(key, ValueSlot{
			[]base.ValueT{value},
			[]base.Tid{tid},
		})
	}
}

func (m *Manager) Add(key base.KeyT, tid base.Tid) base.ValueT {
	//m.AcquireWriteLock(key)
	//defer m.ReleaseWriteLock(key)
	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)
		slotCopy.values[0] = slotCopy.values[0] + 1
		slotCopy.tids[0] = tid
		m.kv.Store(key, slotCopy)
		return slotCopy.values[0]
	} else {
		return base.VALUE_NOT_FOUND
	}
}

func (m *Manager) Del(key base.KeyT, tid base.Tid) {
	//m.AcquireWriteLock(key)
	//defer m.ReleaseWriteLock(key)
	m.kv.Delete(key)
}
