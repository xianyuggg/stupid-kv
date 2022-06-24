package kv

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
	"strings"
	"stupid-kv/base"
	log "stupid-kv/logutil"
	"sync"
)

func valueSlot2String(slot ValueSlot) string {
	retString := ""
	for i := 0; i < len(slot.values); i++ {
		retString += strconv.Itoa(int(slot.values[i])) + " "
		retString += strconv.Itoa(int(slot.tids[i])) + " "
	}
	return strings.Trim(retString, " ")
}
func string2ValueSlot(s string) ValueSlot {
	tmpList := strings.Fields(s)
	valueSlot := ValueSlot{
		values: make([]base.ValueT, 0),
		tids:   make([]base.Tid, 0),
	}
	for i := 0; i < len(tmpList); i += 2 {
		val, _ := strconv.Atoi(tmpList[i])
		tid, _ := strconv.Atoi(tmpList[i+1])
		valueSlot.values = append(valueSlot.values, base.ValueT(val))
		valueSlot.tids = append(valueSlot.tids, base.Tid(tid))
	}
	return valueSlot
}

func MarshalJSON(m *sync.Map) ([]byte, error) {
	tmpMap := make(map[string]interface{})
	m.Range(func(k, v interface{}) bool {
		tmpMap[string(k.(base.KeyT))] = valueSlot2String(v.(ValueSlot))
		return true
	})
	return json.Marshal(tmpMap)
}

func UnmarshalJSON(data []byte) (*sync.Map, error) {
	var tmpMap map[string]string
	m := &sync.Map{}
	if err := json.Unmarshal(data, &tmpMap); err != nil {
		return m, err
	}
	for key, value := range tmpMap {
		m.Store(base.KeyT(key), string2ValueSlot(value))
	}
	return m, nil
}

func (m *Manager) Flush() {
	m.flushGuard.Lock()
	defer m.flushGuard.Unlock()
	jsonByte, err := MarshalJSON(m.kv)
	if err != nil {
		log.Error("flush error: ", err)
		return
	}
	if err := ioutil.WriteFile("DATA.json", jsonByte, 0644); err != nil {
		log.Error(err)
	} else {
		return
	}
}

func (m *Manager) Load() {
	jsonByte, err := ioutil.ReadFile("DATA.json")
	if err != nil {
		log.Error("read data file error: ", err)
	}
	tmpMap, err := UnmarshalJSON(jsonByte)
	if err != nil {
		log.Error("load to json error: ", err)
	}
	m.kv = tmpMap
}
