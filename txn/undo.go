package txn

import (
	"stupid-kv/base"
	log "stupid-kv/logutil"
	"sync"
)

type OpType = int

const (
	opAdd = 1 << iota
	opSet
	opDel
)

type TxnOp struct {
	op    OpType
	key   base.KeyT
	value base.ValueT
}

type UndoLogger struct {
	txnOps map[base.Tid][]TxnOp
	guard  sync.Mutex
}

var undoLogger *UndoLogger
var undoOnce sync.Once

func GetUndoLoggerInstance() *UndoLogger {
	undoOnce.Do(func() {
		log.Info("undo logger starts to init")
		undoLogger = &UndoLogger{
			txnOps: make(map[base.Tid][]TxnOp),
		}
	})
	return undoLogger
}

func (logger *UndoLogger) AppendOp(tid base.Tid, op TxnOp) {
	logger.guard.Lock()
	defer logger.guard.Unlock()
	if ops, ok := logger.txnOps[tid]; ok {
		ops = append(ops, op)
		logger.txnOps[tid] = ops
	} else {
		logger.txnOps[tid] = []TxnOp{op}
	}

}

func (logger *UndoLogger) GetTidOps(tid base.Tid) []TxnOp {
	if ops, ok := logger.txnOps[tid]; ok {
		return ops
	} else {
		return []TxnOp{}
	}
}
