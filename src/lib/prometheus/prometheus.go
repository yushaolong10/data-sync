package prometheus

import (
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	"strings"
	"sync"
)

//表示内部方法还是暴露的外部接口
type PrometheusType int

const (
	PrometheusTypeQPS PrometheusType = iota
	PrometheusTypeTotal
	PrometheusTypeGauge
)

var defaultBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000, 50000, 100000, 500000, 10000000}

type prometheusInner struct {
	serverName string
	idc        string
	ip         string
	qpsVec     map[string]*prometheus.CounterVec   //sum count
	totalVec   map[string]*prometheus.HistogramVec //sum count bulk
	gaugeVec   map[string]*prometheus.GaugeVec
	qpsMutex   sync.Mutex
	totalMutex sync.Mutex
	gaugeMutex sync.Mutex
}

var inner *prometheusInner

func newPrometheusInner(name string, idcName string) *prometheusInner {
	ipStr, err := getInternalIP()
	if err != nil {
		ipStr = "0.0.0.0"
	}
	ins := &prometheusInner{
		serverName: name,
		idc:        idcName,
		ip:         ipStr,
		qpsVec:     make(map[string]*prometheus.CounterVec),
		totalVec:   make(map[string]*prometheus.HistogramVec),
		gaugeVec:   make(map[string]*prometheus.GaugeVec),
	}
	return ins
}

func (pI *prometheusInner) registerQps(name string, labels []string) {
	pI.qpsMutex.Lock()
	defer pI.qpsMutex.Unlock()
	if _, ok := pI.qpsVec[name]; !ok {
		pI.qpsVec[name] = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "iot",
			Subsystem: pI.serverName,
			Name:      name,
			Help:      "iot qps",
		}, append(labels, "idc", "ip"))
		prometheus.MustRegister(pI.qpsVec[name])
	}

}

func (pI *prometheusInner) registerTotal(name string, labels []string, bulks []float64) {
	pI.totalMutex.Lock()
	defer pI.totalMutex.Unlock()
	if _, ok := pI.totalVec[name]; !ok {
		opts := prometheus.HistogramOpts{
			Namespace: "iot",
			Subsystem: pI.serverName,
			Name:      name,
			Help:      "iot total",
		}
		if bulks != nil {
			opts.Buckets = bulks
		} else {
			opts.Buckets = defaultBuckets
		}
		pI.totalVec[name] = prometheus.NewHistogramVec(opts, append(labels, "idc", "ip"))
		prometheus.MustRegister(pI.totalVec[name])
	}
}

func (pI *prometheusInner) registerGuage(name string, labels []string) {
	pI.gaugeMutex.Lock()
	defer pI.gaugeMutex.Unlock()
	if _, ok := pI.totalVec[name]; !ok {
		opts := prometheus.GaugeOpts{
			Namespace: "iot",
			Subsystem: pI.serverName,
			Name:      name,
			Help:      "iot gauge",
		}
		pI.gaugeVec[name] = prometheus.NewGaugeVec(opts, append(labels, "idc", "ip"))
		prometheus.MustRegister(pI.gaugeVec[name])
	}
}

func (pI *prometheusInner) incQps(key string, kv map[string]string) (err error) {
	pI.qpsMutex.Lock()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("check labels")
		}
	}()
	defer pI.qpsMutex.Unlock()
	if qpsP, ok := pI.qpsVec[key]; ok {
		kv["ip"] = pI.ip
		kv["idc"] = pI.idc
		qpsP.With(kv).Inc()
	} else {
		err = fmt.Errorf("not correct key(%s),please check", key)
	}
	return
}

func (pI *prometheusInner) updateTotal(key string, kv map[string]string, value float64) (err error) {
	pI.totalMutex.Lock()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("check labels")
		}
	}()
	defer pI.totalMutex.Unlock()
	if totalP, ok := pI.totalVec[key]; ok {
		kv["ip"] = pI.ip
		kv["idc"] = pI.idc
		totalP.With(kv).Observe(value)
	} else {
		err = fmt.Errorf("not correct key(%s),please check", key)
	}
	return
}

func (pI *prometheusInner) setState(key string, kv map[string]string, val float64) (err error) {
	pI.gaugeMutex.Lock()
	defer pI.gaugeMutex.Unlock()
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("check labels")
		}
	}()
	if gauge, ok := pI.gaugeVec[key]; ok {
		kv["ip"] = pI.ip
		kv["idc"] = pI.idc
		gauge.With(kv).Set(val)
	} else {
		err = fmt.Errorf("not correct key(%s),please check", key)
	}
	return
}

func Init(serverName string, idc string) {
	inner = newPrometheusInner(serverName, idc)
}

func Register(pType PrometheusType, name string, labels []string, bulks []float64) {
	switch pType {
	case PrometheusTypeQPS:
		inner.registerQps(name, labels)
	case PrometheusTypeTotal:
		inner.registerTotal(name, labels, bulks)
	case PrometheusTypeGauge:
		inner.registerGuage(name, labels)
	}
}

func Update(pType PrometheusType, name string, kv map[string]string, value float64) error {
	switch pType {
	case PrometheusTypeQPS:
		return inner.incQps(name, kv)
	case PrometheusTypeTotal:
		return inner.updateTotal(name, kv, value)
	case PrometheusTypeGauge:
		return inner.setState(name, kv, value)
	}
	return nil
}

func NewHttpHandler() http.Handler {
	return promhttp.Handler()
}

func getInternalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				strip := ipnet.IP.String()
				if strings.HasPrefix(strip, "10.") || strings.HasPrefix(strip, "172.16.") {
					return strip, nil
				}
			}
		}
	}
	return "", errors.New("no internal ip found")
}
