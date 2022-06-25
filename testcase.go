package main

import (
	"fmt"
	"stupid-kv/kv"
	log "stupid-kv/logutil"
	"stupid-kv/txn"
	"sync"
	"time"
)

func TestCase1() {
	kvManager := kv.GetManagerInstance()
	kvManager.Put("A", 3, 1)
	kvManager.Put("B", 4, 1)
	kvManager.Inc("A", 1)
	kvManager.Inc("B", 1)
	println(kvManager.Get("A", 1))
	println(kvManager.Get("B", 1))
	kvManager.Del("A", 1)
	kvManager.Del("B", 1)

	kvManager.Put("A", 5, 1)
	println(kvManager.Get("A", 1))
	println(kvManager.Get("B", 1))
	kvManager.Put("B", 5, 1)
	println(kvManager.Get("B", 1))

	kvManager.Flush()
}

func TestCase2() {
	kvManager := kv.GetManagerInstance()
	println(kvManager.Get("A", 1))
	println(kvManager.Get("B", 1))
}

func TestCase3() {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(3)
		go Testcase31(&wg)
		go Testcase32(&wg)
		go Testcase33(&wg)
		wg.Wait()
	}
	wg.Wait()
}

func Testcase31(wg *sync.WaitGroup) {
	kvManager := kv.GetManagerInstance()
	kvManager.Put("A", 1, 1)
	kvManager.Put("B", 1, 1)
	kvManager.Inc("A", 1)
	kvManager.Inc("B", 1)

	fmt.Printf("3-1 A: %v\n", kvManager.Get("A", 1))
	kvManager.Del("A", 1)
	fmt.Printf("3-1 B: %v\n", kvManager.Get("B", 1))
	kvManager.Del("B", 1)

	wg.Done()
}

func Testcase32(wg *sync.WaitGroup) {
	kvManager := kv.GetManagerInstance()
	kvManager.Put("C", 1, 1)
	kvManager.Put("D", 1, 1)
	kvManager.Inc("C", 1)
	kvManager.Inc("D", 1)

	fmt.Printf("3-2 C: %v\n", kvManager.Get("C", 1))
	kvManager.Del("C", 1)
	fmt.Printf("3-2 D: %v\n", kvManager.Get("D", 1))
	kvManager.Del("D", 1)

	wg.Done()
}

func Testcase33(wg *sync.WaitGroup) {
	kvManager := kv.GetManagerInstance()
	kvManager.Put("E", 1, 1)
	kvManager.Put("F", 1, 1)
	kvManager.Inc("E", 1)
	kvManager.Inc("F", 1)

	fmt.Printf("3-3 E: %v\n", kvManager.Get("E", 1))
	kvManager.Del("E", 1)
	fmt.Printf("3-3 F: %v\n", kvManager.Get("F", 1))
	kvManager.Del("F", 1)

	wg.Done()
}

func TestCase4() {
	kvManager := kv.GetManagerInstance()

	println(kvManager.Get("A", 200000))
	println(kvManager.Get("B", 200000))
	kvManager.Put("A", 1, 0)
	kvManager.Put("B", 1, 0)

	wg := sync.WaitGroup{}
	wg.Add(2000000)
	go TestCase41(&wg)
	go TestCase41(&wg)
	go TestCase41(&wg)
	time.Sleep(30 * time.Second)
	kvManager.Flush()
	//wg.Wait()
}

func TestCase41(wg *sync.WaitGroup) {
	for {
		txnManager := txn.GetManagerInstance()
		{
			tid := txnManager.BeginTxn()

			if err := txnManager.Inc("A", tid); err != nil {
				log.Error("inc error: ", err)
			}
			if err := txnManager.Inc("B", tid); err != nil {
				log.Error("inc error: ", err)
			}
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("B", tid)
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("B", tid)
			_ = txnManager.Inc("B", tid)
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("B", tid)

			if err := txnManager.CommitTxn(tid); err != nil {
				log.Error("commit error: ", err)
			}
		}
		{
			tid := txnManager.BeginTxn()

			if err := txnManager.Inc("A", tid); err != nil {
				log.Error("inc error: ", err)
			}
			if err := txnManager.Inc("B", tid); err != nil {
				log.Error("inc error: ", err)
			}
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("B", tid)
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("B", tid)
			_ = txnManager.Inc("B", tid)
			_ = txnManager.Inc("A", tid)
			_ = txnManager.Inc("B", tid)

			if err := txnManager.CommitTxn(tid); err != nil {
				log.Error("commit error: ", err)
			}
		}

		{
			tid := txnManager.BeginTxn()

			if err := txnManager.Dec("A", tid); err != nil {
				log.Error("inc error: ", err)
			}
			if err := txnManager.Dec("B", tid); err != nil {
				log.Error("inc error: ", err)
			}
			_ = txnManager.Dec("A", tid)
			_ = txnManager.Dec("B", tid)
			_ = txnManager.Dec("A", tid)
			_ = txnManager.Dec("A", tid)
			_ = txnManager.Dec("B", tid)
			_ = txnManager.Dec("B", tid)
			_ = txnManager.Dec("A", tid)
			_ = txnManager.Dec("B", tid)

			if err := txnManager.AbortTxn(tid); err != nil {
				log.Error("commit error: ", err)
			}
		}
		wg.Done()
	}

}

func TestCase5() {
	kvManager := kv.GetManagerInstance()

	kvManager.Put("A", 0, 0)
	kvManager.Put("B", 0, 0)
	kvManager.Put("C", 0, 0)

	wg := &sync.WaitGroup{}
	wg.Add(6000000)

	go TestCase41(wg)
	go TestCase41(wg)
	go TestCase41(wg)
	go TestCase51(wg)
	go TestCase51(wg)
	go TestCase51(wg)
	time.Sleep(30 * time.Second)
	kvManager.Flush()
	//wg.Wait()
}

func TestCase52() {
	kvManager := kv.GetManagerInstance()
	println(kvManager.Get("A", 2000000))
	println(kvManager.Get("B", 2000000))
}

func TestCase51(wg *sync.WaitGroup) {

	for {
		txnManager := txn.GetManagerInstance()
		tid := txnManager.BeginTxn()
		txnManager.Get("A", tid)
		txnManager.Get("B", tid)
		txnManager.Get("A", tid)
		txnManager.Get("B", tid)
		err := txnManager.Inc("C", tid)
		if err != nil {
			return
		}
		err = txnManager.CommitTxn(tid)
		if err != nil {
			return
		}
	}

}
