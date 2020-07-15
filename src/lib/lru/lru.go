package lru

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var ErrLruNotFoundKey = errors.New("lru key not exist")

type LRUCache struct {
	reqCount int64                    //请求次数
	hitCount int64                    //命中次数
	keyCount int64                    //当前缓存数量
	maxCount int64                    //最大缓存数量
	ttl      int64                    //存活时长:秒s
	lruList  *list.List               //缓存链表
	lruMap   map[string]*list.Element //缓存map
	mutex    sync.Mutex
}

type entry struct {
	key      string
	value    interface{}
	createAt int64 //创建时间戳
}

//param: length 长度
//param: ttl 缓存有效期 (s)
func NewLRUCache(maxCount int, ttl int) *LRUCache {
	cache := &LRUCache{
		maxCount: int64(maxCount),
		ttl:      int64(ttl),
		lruList:  list.New(),
		lruMap:   make(map[string]*list.Element),
	}
	return cache
}

//写入
func (cache *LRUCache) Update(key string, value interface{}) error {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	if ele, ok := cache.lruMap[key]; ok { //exist
		item := ele.Value.(*entry)
		item.value = value
		item.createAt = time.Now().Unix()
		cache.lruList.MoveToBack(ele)
	} else { //new
		item := &entry{
			key:      key,
			value:    value,
			createAt: time.Now().Unix(),
		}
		cache.lruMap[key] = cache.lruList.PushBack(item)
		cache.keyCount++
	}
	cache.checkOverflowWithLocked()
	return nil
}

func (cache *LRUCache) checkOverflowWithLocked() {
	if cache.keyCount > cache.maxCount {
		front := cache.lruList.Front()
		if front != nil {
			e := front.Value.(*entry)
			cache.lruList.Remove(front)
			delete(cache.lruMap, e.key)
			cache.keyCount--
		}
	}
}

//获取
func (cache *LRUCache) Get(key string) (interface{}, error) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	cache.reqCount++
	if ele, ok := cache.lruMap[key]; ok {
		item := ele.Value.(*entry)
		if item.createAt+cache.ttl > time.Now().Unix() { //有效
			cache.hitCount++
			item.createAt = time.Now().Unix()
			cache.lruList.MoveToBack(ele)
			return item.value, nil
		}
		//expire
		cache.lruList.Remove(ele)
		delete(cache.lruMap, item.key)
		cache.keyCount--
	}
	return nil, ErrLruNotFoundKey
}

//获取请求次数
func (cache *LRUCache) GetRequestCount() int64 {
	return cache.reqCount
}

//获取命中次数
func (cache *LRUCache) GetHitCount() int64 {
	return cache.hitCount
}

//获取当前key数量,包括未过期和已过期的
func (cache *LRUCache) GetKeysCount() int64 {
	return cache.keyCount
}

func (cache *LRUCache) debugReadyList() []interface{} {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	node := cache.lruList.Back()
	now := time.Now().Unix()
	datas := make([]interface{}, 0)
	for node != nil {
		item := node.Value.(*entry)
		if item.createAt+cache.ttl < now {
			break
		}
		datas = append(datas, item.value)
		node = node.Prev()
	}
	return datas
}

func (cache *LRUCache) debugAllList() []interface{} {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	node := cache.lruList.Back()
	datas := make([]interface{}, 0)
	for node != nil {
		item := node.Value.(*entry)
		datas = append(datas, item.value)
		node = node.Prev()
	}
	return datas
}
