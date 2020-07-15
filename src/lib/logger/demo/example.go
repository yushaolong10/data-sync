package main

import (
	"fmt"
	"lib/logger"
)

func main() {
	logger.Init("./conf/log.json")
	logger.Info("test result=%s,a=%s", "abc", "def")
	logger.Error("abcccddd")
	logger.Public("publicfafaaa")
	logger.Close()
	fmt.Println("exucte finish")
}
