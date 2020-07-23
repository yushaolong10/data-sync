package inspect

import (
	"fmt"
	"infrastructure/config"
	"infrastructure/context"
	"infrastructure/monitor"
	"lib/logger"
	"lib/routine"
	"lib/util"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type Hooker interface {
	HandleInspect(ctx *context.BizContext) bool
}

type StrategyInspect struct {
	ctx         *context.BaseContext
	name        string
	hook        Hooker
	conf        config.InspectConfig
	failedCount int32
}

//new object
func NewInspectStrategy(ctx *context.BaseContext, taskName string, conf config.InspectConfig, hook Hooker) (*StrategyInspect, error) {
	strategy := &StrategyInspect{
		ctx:  ctx,
		name: taskName,
		hook: hook,
		conf: conf,
	}
	return strategy, nil
}

func (strategy *StrategyInspect) Name() string {
	return strategy.name
}

func (strategy *StrategyInspect) Run() error {
	//routine run
	routine.Go(strategy.inspect)
	logger.Info("[StrategyInspect.Run] inspect success.name:%s,duration:%ds", strategy.name, strategy.conf.InspectDuration)
	return nil
}

func (strategy *StrategyInspect) inspect() {
	appendDuration := time.Duration(strategy.conf.InspectDuration)
	ticker := time.NewTicker(appendDuration * time.Second)
	for range ticker.C {
		if strategy.ctx.IsCanceled() {
			logger.Info("[StrategyInspect.inspect] context canceled. name:%s", strategy.name)
			ticker.Stop()
			return
		}
		//biz context with requestId
		ctx := &context.BizContext{
			RequestId:   util.GetTraceId(),
			BaseContext: strategy.ctx,
		}
		if strategy.safeHook(ctx) {
			//如果failedCount>0, 重置
			if strategy.failedCount > 0 {
				atomic.SwapInt32(&strategy.failedCount, 0)
				monitor.UpdateIndexState(strategy.name, "inspect", int64(strategy.failedCount))
			}
			continue
		}
		curFailedCount := atomic.AddInt32(&strategy.failedCount, 1)
		if curFailedCount > strategy.conf.InspectAlarmCount {
			//同步异常
			monitor.UpdateException(strategy.name, "inspect", "exceedFailedCount")
			logger.Error("[StrategyInspect.inspect] failed Count exceed max limit, please check. requestId:%s,name:%s, curFailCount:%d, maxLimitCount:%d", ctx.RequestId, strategy.name, curFailedCount, strategy.conf.InspectAlarmCount)
		}
		//gauge 监控
		monitor.UpdateIndexState(strategy.name, "inspect", int64(strategy.failedCount))
		logger.Error("[StrategyInspect.inspect] strategy safeHook returned false. requestId:%s,name:%s,curFailCount:%d", ctx.RequestId, strategy.name, strategy.failedCount)
	}
}

func (strategy *StrategyInspect) safeHook(ctx *context.BizContext) (b bool) {
	var err error
	defer func(begin time.Time) {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic")
			monitor.UpdatePanic("inspect")
			logger.Error("[StrategyInspect.safeHook]requestId:%s,name:%s, #Panic#(%v),backTrace:%s", ctx.RequestId, strategy.name, p, string(debug.Stack()))
		}
		if !b {
			err = fmt.Errorf("inspect fail")
		}
		//timeUsed
		interval := util.GetDurationMillis(begin)
		monitor.UpdateStrategy("inspect", strategy.name, interval, err)
		logger.Info("[StrategyInspect.safeHook] requestId:%s,name:%s,timeUsed:%dus,err:%v", ctx.RequestId, strategy.name, interval, err)
	}(time.Now())
	return strategy.hook.HandleInspect(ctx)
}
