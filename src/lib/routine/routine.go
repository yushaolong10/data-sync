package routine

import (
	"log"
	"sync"
	"sync/atomic"
)

var goesMgr *GoRoutineManager

type GoRoutineManager struct {
	Count int32
	Wg    sync.WaitGroup
}

func init() {
	goesMgr = &GoRoutineManager{
		Wg: sync.WaitGroup{},
	}
}

func Go(fn func()) {
	go func() {
		//add group
		goesMgr.Wg.Add(1)
		atomic.AddInt32(&goesMgr.Count, 1)
		//defer
		defer func() {
			goesMgr.Wg.Done()
			atomic.AddInt32(&goesMgr.Count, -1)
		}()
		//exec
		fn()
	}()
}

func Wait() {
	log.Printf("[info][routine.Wait] current go routine count:%d", goesMgr.Count)
	goesMgr.Wg.Wait()
}
