package main

import (
	"fmt"
	"infrastructure"
	"net/http"
	"time"
)

var count int64

func excute() {
	go func() {
		for {
			infrastructure.Update(infrastructure.PrometheusTypeQPS, "default", map[string]string{"method": "test"}, 10)
			infrastructure.Update(infrastructure.PrometheusTypeQPS, "default", map[string]string{"method": "test"}, 10)
			infrastructure.Update(infrastructure.PrometheusTypeTotal, "default1", map[string]string{"method1": "test1"}, 10)
			count++
		}
	}()
}

func monitor() {
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("qps=", count)
			count = 0
		}
	}()

}
func main() {

	infrastructure.Init("hperx", "default")                                                         //初始化
	infrastructure.Registe(infrastructure.PrometheusTypeQPS, "default", []string{"method"}, nil)    //注册
	infrastructure.Registe(infrastructure.PrometheusTypeTotal, "default1", []string{"method"}, nil) //注册

	excute() //执行
	excute() //执行
	excute() //执行
	monitor()
	//启动服务
	http.Handle("/metrics", infrastructure.NewHttpHander())
	http.ListenAndServe(":8888", nil)
	fmt.Println("hello world ")
}
