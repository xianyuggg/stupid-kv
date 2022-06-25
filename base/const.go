package base

var VALUE_NOT_VALID = ValueT(0xdddddd)
var VALUE_NOT_COMMIT = ValueT(0xeeeeee)
var VALUE_NOT_FOUND = ValueT(0xffffff)

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1)
const MinInt = -MaxInt - 1

var MAX_TID = Tid(MaxInt)
var NIL_TID = Tid(-1)
