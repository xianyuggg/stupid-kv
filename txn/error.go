package txn

import "errors"

var (
	ErrorWriteOlderVersion = errors.New("txn try to append older version to chain")
)
