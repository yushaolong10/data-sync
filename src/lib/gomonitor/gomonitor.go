package gomonitor

import "runtime"

type GoStats struct {
	GCNum        uint32
	GCPause      uint64
	MemAllocated uint64
	MemObjects   uint64
	MemHeap      uint64
	MemStack     uint64
	MemMallocs   uint64
	GoroutineNum uint32
}

var goMoPtr goMonitor

type goMonitor struct {
	lastMem runtime.MemStats
}

func (gm *goMonitor) getState() GoStats {
	mem := runtime.MemStats{}
	runtime.ReadMemStats(&mem)
	gs := GoStats{}
	gs.GCNum = mem.NumGC - gm.lastMem.NumGC
	gs.GCPause = (mem.PauseTotalNs - gm.lastMem.PauseTotalNs) / 1000 //微妙
	gs.MemAllocated = mem.Alloc
	gs.MemObjects = mem.HeapObjects
	gs.MemHeap = mem.HeapAlloc
	gs.MemStack = mem.StackInuse
	gs.MemMallocs = mem.Mallocs - gm.lastMem.Mallocs
	gs.GoroutineNum = uint32(runtime.NumGoroutine())
	gm.lastMem = mem
	return gs
}

func GetState() GoStats {
	return goMoPtr.getState()
}
