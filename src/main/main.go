package main

import (
	"application"
	"flag"
	"fmt"
	"infrastructure/config"
	"infrastructure/monitor"
	"lib/logger"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"server/http"
	"syscall"
)

var (
	confFile = flag.String("conf", "./conf/iot.sync.toml", "config file")
	logFile  = flag.String("log", "./conf/log.json", "log file")
)

func main() {
	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	flag.Parse()
	//logger init
	defer logger.Close()
	if err := logger.Init(*logFile); err != nil {
		fmt.Printf("[IOT]init logger failed,err=%v\n", err)
		return
	}
	fmt.Println("[IOT]init logger success")
	defer func() {
		if r := recover(); r != nil {
			logger.Error("[IOT]PanicError#, err=%v,trace=%v", r, string(debug.Stack()))
		}
	}()
	// crashed log
	setCrashLog("./log/iot.sync.dump")
	//config init
	if err := config.Init(*confFile); err != nil {
		logger.Error("[IOT]initial config failed, err=%v", err)
		return
	}
	logger.Info("[IOT]initial config success!")
	//monitor
	if err := monitor.Init("data_sync", config.Conf.Cluster); err != nil {
		logger.Error("[IOT]initial monitor failed, err=%v", err)
		return
	}
	logger.Info("[IOT]initial monitor success!")
	//run application
	if err := application.Run(); err != nil {
		logger.Error("[IOT] application run error, err=%v", err)
		return
	}
	logger.Info("[IOT] application run success!")
	//http server
	if err := http.Run(); err != nil {
		logger.Error("[IOT] http server run error, err=%v", err)
		return
	}
	if err := application.Stop(); err != nil {
		logger.Error("[IOT] app stop error, err=%v", err)
	}
	logger.Info("[IOT] app stop success!")
	logger.Info("[IOT] http stop success!")
}

func setCrashLog(file string) error {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	} else {
		syscall.Dup2(int(f.Fd()), 2)
		return nil
	}
}
