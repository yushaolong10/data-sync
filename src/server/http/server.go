package http

import (
	"application"
	"fmt"
	"infrastructure/config"
	"lib/gracehttp"
	"lib/json"
	"lib/logger"
	"lib/prometheus"
	"lib/util"
	"log"
	"net/http"
	"time"
)

const (
	//request uuid
	DataSyncRequestId = "data-sync-requestId"
)

func Run() error {
	//monitor
	http.Handle("/sync/monitor/metrics", prometheus.NewHttpHandler())
	//http server
	httpSrv := newHttpServer(config.Conf.Http.HttpPort)
	//monitor server
	monitorSrv := newMonitorServer(config.Conf.Http.MonitorPort)
	//run
	if err := gracehttp.Serve(httpSrv, monitorSrv); err != nil {
		return err
	}
	return nil
}

//监控
func newMonitorServer(httpPort int64) *http.Server {
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", httpPort),
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
		IdleTimeout:  time.Second * 30,
	}
	return server
}

//业务server
func newHttpServer(httpPort int64) *http.Server {
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", httpPort),
		Handler:      newHttpRouter(),
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
		IdleTimeout:  time.Second * 30,
	}
	return server
}

func newHttpRouter() *http.ServeMux {
	adaptor := &httpRouterAdaptor{secretKey: "yushaolong@360.cn"}
	mux := http.NewServeMux()
	//http router path
	mux.HandleFunc("/sync/getConfig", adaptor.wrap(adaptor.getConfig))
	mux.HandleFunc("/sync/setStrategyMode", adaptor.wrap(adaptor.setStrategyMode))
	return mux
}

//adaptor
type httpRouterAdaptor struct {
	secretKey string
}

//wrap middleware
func (adaptor *httpRouterAdaptor) wrap(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requestId := util.GetTraceId()
		r.Header.Set(DataSyncRequestId, requestId)
		//get secret key
		secretKey, ok := r.URL.Query()["secret_key"]
		if !ok {
			adaptor.send(w, &httpResponseData{
				RequestId: requestId,
				Code:      900,
				Message:   fmt.Sprintf("need secret_key"),
			})
			return
		}
		if secretKey[0] != adaptor.secretKey {
			adaptor.send(w, &httpResponseData{
				RequestId: requestId,
				Code:      901,
				Message:   fmt.Sprintf("secret_key invalid"),
			})
			return
		}
		handler.ServeHTTP(w, r)
	}
}

type httpResponseData struct {
	RequestId string      `json:"uuid"`
	Code      int         `json:"code"`
	Message   string      `json:"message,omitempty"`
	Data      interface{} `json:"data,omitempty"`
}

// http response
func (adaptor *httpRouterAdaptor) send(resp http.ResponseWriter, respData *httpResponseData) {
	resp.WriteHeader(200)
	if err := json.NewEncoder(resp).Encode(respData); err != nil {
		log.Printf("[error][httpRouterAdaptor.send] could not encode response. respData:%s,err:%s", util.StructToJson(respData), err.Error())
		http.Error(resp, "could not encode response", http.StatusInternalServerError)
	}
}

func (adaptor *httpRouterAdaptor) getConfig(w http.ResponseWriter, r *http.Request) {
	adaptor.send(w, &httpResponseData{
		RequestId: r.Header.Get(DataSyncRequestId),
		Data:      config.Conf,
	})
}

type setStrategyRequest struct {
	TaskName     string `json:"task_name"`
	StrategyMode int16  `json:"strategy_mode"`
}

func (adaptor *httpRouterAdaptor) setStrategyMode(w http.ResponseWriter, r *http.Request) {
	requestId := r.Header.Get(DataSyncRequestId)
	var reqStrategy setStrategyRequest
	if err := json.NewDecoder(r.Body).Decode(&reqStrategy); err != nil {
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1000,
			Message:   fmt.Sprintf("parse request params failed:%s", err.Error()),
		})
		return
	}
	for i := 0; i < len(config.Conf.MysqlTask); i++ {
		if config.Conf.MysqlTask[i].TaskName == reqStrategy.TaskName {
			config.Conf.MysqlTask[i].Strategy.StrategyMode = reqStrategy.StrategyMode
		}
	}
	go func() {
		err := application.Reload()
		if err != nil {
			logger.Error("[httpRouterAdaptor.setStrategyMode] application.Reload error. requestId:%s,request:%s,err:%s", requestId, util.StructToJson(&reqStrategy), err.Error())
		} else {
			logger.Info("[httpRouterAdaptor.setStrategyMode] application.Reload success, requestId:%s,request:%s", requestId, util.StructToJson(&reqStrategy))
		}
	}()
	//response
	adaptor.send(w, &httpResponseData{
		RequestId: requestId,
		Data:      config.Conf,
	})
}
