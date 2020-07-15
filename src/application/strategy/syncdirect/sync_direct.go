package syncdirect

import (
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"infrastructure/monitor"
	"lib/logger"
	"lib/routine"
	"lib/util"
	"runtime/debug"
	"time"
)

type Hooker interface {
	HandleDirect() error
}

type StrategySyncDirect struct {
	ctx  *context.BaseContext
	name string
	hook Hooker
	conf config.SyncDirectConfig
}

func NewSyncDirectStrategy(ctx *context.BaseContext, taskName string, conf config.SyncDirectConfig, hook Hooker) (*StrategySyncDirect, error) {
	//new object
	strategy := &StrategySyncDirect{
		ctx:  ctx,
		name: taskName,
		hook: hook,
		conf: conf,
	}
	return strategy, nil
}

func (strategy *StrategySyncDirect) Name() string {
	return strategy.name
}

func (strategy *StrategySyncDirect) Run() error {
	//routine run
	routine.Go(strategy.schedule)
	logger.Info("[StrategySyncDirect.Run] sync direct start success.name:%s,duration:%d", strategy.name, strategy.conf.AppendDuration)
	return nil
}

func (strategy *StrategySyncDirect) schedule() {
	appendDuration := time.Duration(strategy.conf.AppendDuration)
	ticker := time.NewTicker(appendDuration * time.Second)
	var err error
	for range ticker.C {
		if strategy.ctx.IsCanceled() {
			logger.Info("[StrategySyncDirect.schedule] context canceled. name:%s", strategy.name)
			ticker.Stop()
			return
		}
		if err = strategy.safeHook(); err != nil {
			//同步失败
			logger.Error("[StrategySyncDirect.schedule] safeHook error. name:%s,err:%s", strategy.name, err.Error())
		}
	}
}

func (strategy *StrategySyncDirect) safeHook() (err error) {
	defer func(begin time.Time) {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic")
			logger.Error("[StrategySyncDirect.safeHook]name:%s,#Panic#(%v),backTrace:%s", strategy.name, p, string(debug.Stack()))
			monitor.UpdatePanic("syncdirect")
		}
		//timeUsed
		interval := util.GetDurationMillis(begin)
		monitor.UpdateStrategy("syncdirect", strategy.name, interval, err)
		logger.Info("[StrategySyncDirect.safeHook] name:%s,timeUsed:%dus,err:%v", strategy.name, interval, err)
	}(time.Now())
	return strategy.hook.HandleDirect()
}
