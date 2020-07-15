package lru

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheKeyAndRequestCount(t *testing.T) {
	cache := NewLRUCache(10, 3)
	cache.Update("a", "b")
	cache.Update("c", "d")
	if cache.GetKeysCount() != 2 {
		t.Fatalf("current count not equal 2")
	}
	t.Logf("debug list zero:%v, %v", cache.debugReadyList(), cache.debugAllList())
	t.Logf("current len: %d", cache.GetKeysCount())
	val, err := cache.Get("a")
	if err != nil {
		t.Fatalf("cache a must exsit")
	}
	ret := val.(string)
	t.Logf("ret is %s", ret)
	for i := 0; i < 50; i++ {
		cache.Get("c")
		cache.Get("e")
	}
	t.Logf("current req: %d", cache.GetRequestCount())
	t.Logf("current hit: %d", cache.GetHitCount())
	t.Logf("debug list one:%v, %v", cache.debugReadyList(), cache.debugAllList())
	t.Logf("sleep 5s...")
	time.Sleep(time.Second * 5)
	t.Logf("debug list two:%v, %v", cache.debugReadyList(), cache.debugAllList())
	_, err = cache.Get("a")
	t.Logf("debug list three:%v, %v", cache.debugReadyList(), cache.debugAllList())
	if err != nil {
		t.Logf("cache err:%s", err.Error())
	}
}

func TestCacheKeyExceed(t *testing.T) {
	cache := NewLRUCache(20, 5)
	t.Logf("current key: %d", cache.GetKeysCount())
	for i := 0; i < 5; i++ {
		k := strconv.Itoa(i)
		cache.Update(k, i+10)
		t.Logf("for(%s) current key: %d", k, cache.GetKeysCount())
	}
	v, e := cache.Get("3")
	t.Logf("[get 3] ret:%v,err:%v", v, e)
	for i := 0; i < 5; i++ {
		cache.Update("c", i)
		t.Logf("same c current key: %d", cache.GetKeysCount())
	}
	v, e = cache.Get("c")
	t.Logf("[get c] ret:%v,err:%v", v, e)
	t.Logf("current key: %d", cache.GetKeysCount())
	for i := 100; i < 300; i++ {
		k := strconv.Itoa(i)
		cache.Update(k, i)
		if i == 288 {
			v, e = cache.Get("288")
			t.Logf("[get 288] ret:%v,err:%v", v, e)
		}
	}
	t.Logf("now current key: %d", cache.GetKeysCount())
	v, e = cache.Get("3")
	t.Logf("[get 3] ret:%v,err:%v", v, e)

	v, e = cache.Get("166")
	t.Logf("[get 166] ret:%v,err:%v", v, e)

	v, e = cache.Get("255")
	t.Logf("[get 255] ret:%v,err:%v", v, e)

	v, e = cache.Get("c")
	t.Logf("[get c] ret:%v,err:%v", v, e)

	t.Logf("current req: %d", cache.GetRequestCount())
	t.Logf("current hit: %d", cache.GetHitCount())
	time.Sleep(time.Second * 7)
	t.Logf("debug list:%v, %v", cache.debugReadyList(), cache.debugAllList())

}

//cpu 4core; mem:8g
//MaxCount:10w, ttl:10s
//====
//mem used: 56MB
//
//only write:
//qps: 443412
//qps: 464858
//qps: 457101
//qps: 457844
//qps: 462018

//only read:
//qps: 2900554
//qps: 2822239
//qps: 2807649
//qps: 2868974
//qps: 3026891
//qps: 2791341

//read & write
//qps: 443412
//qps: 464858
//qps: 457101
//qps: 457844
//qps: 462018

var qps int64

func TestQps(t *testing.T) {
	cache := NewLRUCache(100000, 10)
	setCache(cache)
	getCache(cache)
	monitorCache()
	time.Sleep(time.Millisecond)
}

func setCache(c *LRUCache) {
	go func() {
		init := "abc"
		i := 0
		for {
			i++
			k := strconv.Itoa(i) + init
			c.Update(k, i)
			atomic.AddInt64(&qps, 1)
		}
	}()
}

func getCache(c *LRUCache) {
	go func() {
		init := "abc"
		i := 0
		for {
			i++
			k := strconv.Itoa(i) + init
			c.Get(k)
			atomic.AddInt64(&qps, 1)
		}
	}()
}

func monitorCache() {
	for {
		time.Sleep(time.Second)
		v := atomic.SwapInt64(&qps, 0)
		fmt.Println("qps:", v)
	}
}
