package monitor

import (
	"lib/gomonitor"
	"lib/logger"
	"lib/prometheus"
	"lib/util"
	"time"
)

const (
	MONITER_NAME_HISTOGRAM_STATISTICS = "statistics"
	MONITER_NAME_GAUGE_STATE          = "state"
	MONITER_NAME_HISTOGRAM_DEPENDENCE = "dependence"
	MONITER_NAME_COUNTER_PANIC        = "panic"
	MONITER_NAME_COUNTER_EXCEPTION    = "exception"
	MONITER_NAME_HISTOGRAM_STRATEGY   = "strategy"
)

func Init(srvName, cluster string) error {
	prometheus.Init(srvName, cluster)
	register()
	go monitor()
	return nil
}

func monitor() {
	var interval int64 = 10
	for {
		stat := gomonitor.GetState()
		logger.Public("[Monitor]MEMStat=%v", util.StructToJson(&stat))
		UpdateStatistics("GO_GCNum", int64(stat.GCNum))
		UpdateStatistics("GO_GCPause", int64(stat.GCPause))
		UpdateStatistics("GO_MemStack", int64(stat.MemStack))
		UpdateStatistics("GO_MemMallocs", int64(stat.MemMallocs))
		UpdateStatistics("GO_MemAllocated", int64(stat.MemAllocated))
		UpdateStatistics("GO_MemObjects", int64(stat.MemObjects))
		UpdateStatistics("GO_MemHeap", int64(stat.MemHeap))
		UpdateStatistics("GO_GoroutineNum", int64(stat.GoroutineNum))
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func register() {
	prometheus.Register(prometheus.PrometheusTypeTotal, MONITER_NAME_HISTOGRAM_STATISTICS, []string{"module"}, nil)
	prometheus.Register(prometheus.PrometheusTypeQPS, MONITER_NAME_COUNTER_PANIC, []string{"source"}, nil)
	prometheus.Register(prometheus.PrometheusTypeQPS, MONITER_NAME_COUNTER_EXCEPTION, []string{"name", "source", "index"}, nil)
	prometheus.Register(prometheus.PrometheusTypeGauge, MONITER_NAME_GAUGE_STATE, []string{"source", "index"}, nil)
	prometheus.Register(prometheus.PrometheusTypeTotal, MONITER_NAME_HISTOGRAM_DEPENDENCE, []string{"service", "function", "status"}, nil)
	prometheus.Register(prometheus.PrometheusTypeTotal, MONITER_NAME_HISTOGRAM_STRATEGY, []string{"strategy", "name", "status"}, nil)
}

func UpdateStatistics(module string, value int64) {
	labels := map[string]string{
		"module": module,
	}
	prometheus.Update(prometheus.PrometheusTypeTotal, MONITER_NAME_HISTOGRAM_STATISTICS, labels, float64(value))
}

func UpdatePanic(source string) {
	labels := map[string]string{
		"source": source,
	}
	prometheus.Update(prometheus.PrometheusTypeQPS, MONITER_NAME_COUNTER_PANIC, labels, 0)
}

func UpdateException(name string, source string, index string) {
	labels := map[string]string{
		"name":   name,
		"source": source,
		"index":  index,
	}
	prometheus.Update(prometheus.PrometheusTypeQPS, MONITER_NAME_COUNTER_EXCEPTION, labels, 0)
}

func UpdateIndexState(source string, index string, value int64) {
	labels := map[string]string{
		"source": source,
		"index":  index,
	}
	prometheus.Update(prometheus.PrometheusTypeGauge, MONITER_NAME_GAUGE_STATE, labels, float64(value))
}

func UpdateDependence(service string, function string, value int64, err error) {
	labels := map[string]string{
		"service":  service,
		"function": function,
	}
	if err != nil {
		labels["status"] = "1"
	} else {
		labels["status"] = "0"
	}
	prometheus.Update(prometheus.PrometheusTypeTotal, MONITER_NAME_HISTOGRAM_DEPENDENCE, labels, float64(value))
}

func UpdateStrategy(strategy string, name string, value int64, err error) {
	labels := map[string]string{
		"strategy": strategy,
		"name":     name,
	}
	if err != nil {
		labels["status"] = "1"
	} else {
		labels["status"] = "0"
	}
	prometheus.Update(prometheus.PrometheusTypeTotal, MONITER_NAME_HISTOGRAM_STRATEGY, labels, float64(value))
}
