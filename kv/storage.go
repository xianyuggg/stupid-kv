package kv

import (
	"io/ioutil"
	"stupid-kv/base"
	log "stupid-kv/logutil"
	"sync"
)

type ValueSlot struct {
	values    []base.ValueT
	tidsBegin []base.Tid
	tidsEnd   []base.Tid // support mvcc
}

type Manager struct {
	//kv map[base.KeyT]ValueSlot
	kv         *sync.Map
	slotGuard  map[base.KeyT]*sync.RWMutex
	mapGuard   *sync.Mutex // used for guarding slotguard (concurrent map modify)
	flushGuard *sync.Mutex
}

var instance *Manager
var once sync.Once

func GetManagerInstance() *Manager {
	once.Do(func() {
		log.Info("KV manager starts to init")
		instance = &Manager{
			//kv: make(map[base.KeyT]ValueSlot),
			kv:         &sync.Map{},
			flushGuard: &sync.Mutex{},
			slotGuard:  make(map[base.KeyT]*sync.RWMutex),
			mapGuard:   &sync.Mutex{},
		}
		if _, err := ioutil.ReadFile("DATA.json"); err == nil {
			instance.Load()
		}
	})
	return instance
}

func (m *Manager) Put(key base.KeyT, value base.ValueT, tid base.Tid) {
	m.mapGuard.Lock()
	_, ok := m.slotGuard[key]
	if !ok {
		m.slotGuard[key] = &sync.RWMutex{}
	}
	guard, _ := m.slotGuard[key]
	guard.Lock()
	defer guard.Unlock()
	m.mapGuard.Unlock()
	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)

		length := len(slotCopy.values)
		slotCopy.tidsEnd[length-1] = tid // update last tid

		if tid < slotCopy.tidsBegin[length-1] {
			log.Error("write cover with older tid")
		}

		slotCopy.values = append(slotCopy.values, value)
		slotCopy.tidsBegin = append(slotCopy.tidsBegin, tid)
		slotCopy.tidsEnd = append(slotCopy.tidsEnd, base.MAX_TID)

		m.kv.Store(key, slotCopy)
	} else {
		m.kv.Store(key, ValueSlot{
			[]base.ValueT{value},
			[]base.Tid{tid},
			[]base.Tid{base.MAX_TID},
		})
	}
}

func contains(s []base.Tid, e base.Tid) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (m *Manager) Get(key base.KeyT, tid base.Tid, activeTids []base.Tid) base.ValueT {
	guard, ok := m.slotGuard[key]
	if !ok {
		return base.VALUE_NOT_FOUND
	}
	guard.RLock()
	defer guard.RUnlock()

	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)
		length := len(slotCopy.values)

		for i := length - 1; i >= 0; i-- {
			if tid >= slotCopy.tidsBegin[i] && tid <= slotCopy.tidsEnd[i] {
				if contains(activeTids, slotCopy.tidsBegin[i]) {
					// still an uncommitted tid
					return base.VALUE_NOT_COMMIT
				} else {
					return slotCopy.values[i]
				}
			}
		}
		return base.VALUE_NOT_VALID
	} else {
		return base.VALUE_NOT_FOUND
	}
}

func (m *Manager) Inc(key base.KeyT, tid base.Tid) base.ValueT {
	guard, _ := m.slotGuard[key]
	guard.Lock()
	defer guard.Unlock()

	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)

		length := len(slotCopy.values)
		slotCopy.tidsEnd[length-1] = tid // update last tid

		oldValue := slotCopy.values[length-1]
		slotCopy.values = append(slotCopy.values, oldValue+1)
		slotCopy.tidsBegin = append(slotCopy.tidsBegin, tid)
		slotCopy.tidsEnd = append(slotCopy.tidsEnd, base.MAX_TID)

		m.kv.Store(key, slotCopy)
		return oldValue + 1
	} else {
		log.Warning("inc op has no key")
		return base.VALUE_NOT_FOUND
	}
}

func (m *Manager) Dec(key base.KeyT, tid base.Tid) base.ValueT {
	guard, _ := m.slotGuard[key]
	guard.Lock()
	defer guard.Unlock()

	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)

		length := len(slotCopy.values)
		slotCopy.tidsEnd[length-1] = tid // update last tid

		oldValue := slotCopy.values[length-1]
		slotCopy.values = append(slotCopy.values, oldValue-1)
		slotCopy.tidsBegin = append(slotCopy.tidsBegin, tid)
		slotCopy.tidsEnd = append(slotCopy.tidsEnd, base.MAX_TID)

		m.kv.Store(key, slotCopy)
		return oldValue + 1
	} else {
		log.Warning("inc op has no key")
		return base.VALUE_NOT_FOUND
	}
}

func (m *Manager) Del(key base.KeyT, tid base.Tid) {
	m.Put(key, base.VALUE_NOT_FOUND, tid)
}

func (m *Manager) UnrollKeyByTid(key base.KeyT, tid base.Tid) {
	guard, _ := m.slotGuard[key]
	guard.Lock()
	defer guard.Unlock()

	if slotCopy, ok := m.kv.Load(key); ok {
		slotCopy := slotCopy.(ValueSlot)

		length := len(slotCopy.values)

		i := length - 1
		for ; i >= 0; i-- {
			if slotCopy.tidsBegin[i] == tid {
				break
			}
		}
		if i != 0 && i != length-1 {
			slotCopy.tidsEnd[i-1] = slotCopy.tidsBegin[i+1]
			slotCopy.values = append(slotCopy.values[:i], slotCopy.values[i+1:]...)
			slotCopy.tidsBegin = append(slotCopy.tidsBegin[:i], slotCopy.tidsBegin[i+1:]...)
			slotCopy.tidsEnd = append(slotCopy.tidsEnd[:i], slotCopy.tidsEnd[i+1:]...)
		} else if i == 0 {
			slotCopy.values = append(slotCopy.values[:0], slotCopy.values[i+1:]...)
			slotCopy.tidsBegin = append(slotCopy.tidsBegin[:0], slotCopy.tidsBegin[i+1:]...)
			slotCopy.tidsEnd = append(slotCopy.tidsEnd[:0], slotCopy.tidsEnd[i+1:]...)
		} else if i == length-1 {
			slotCopy.values = slotCopy.values[0 : length-1]
			slotCopy.tidsBegin = slotCopy.tidsBegin[0 : length-1]
			slotCopy.tidsEnd = slotCopy.tidsEnd[0 : length-1]
			slotCopy.tidsEnd[length-2] = base.MAX_TID
		}
		m.kv.Store(key, slotCopy)
	} else {
		log.Warning("unroll has no key")
	}
}
