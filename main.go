package main

import "stupid-kv/kv"

func main() {
	kvManager := kv.GetManagerInstance()
	kvManager.Set("A", 10, 1)
	kvManager.Set("B", 20, 1)
	kvManager.Set("C", 30, 1)

	println(kvManager.Get("A", 1))
	println(kvManager.Get("B", 1))

	kvManager.Flush()
}
