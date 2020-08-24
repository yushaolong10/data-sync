package syncmq

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
	HandleMqMessage(ctx *context.BizContext, message *TopicMessage) error
}

type StrategySyncMq struct {
	ctx         *context.BaseContext
	name        string
	alias       string
	hook        Hooker
	mqConsumer  *MqConsumerServer
	messageChan chan *TopicMessage //消息流,读取流
}

func NewSyncMqStrategy(ctx *context.BaseContext, taskName string, conf config.MqConfig, hook Hooker) (*StrategySyncMq, error) {
	//create message chan
	messageChan := make(chan *TopicMessage)
	//new consumer
	consumer := NewMqConsumer(ctx, conf, messageChan)
	//new object
	strategy := &StrategySyncMq{
		ctx:         ctx,
		name:        taskName,
		alias:       conf.Name,
		hook:        hook,
		messageChan: messageChan,
		mqConsumer:  consumer,
	}
	return strategy, nil
}

func (strategy *StrategySyncMq) Name() string {
	return strategy.name
}

func (strategy *StrategySyncMq) Run() error {
	//start mq consumer
	if err := strategy.mqConsumer.Start(); err != nil {
		return fmt.Errorf("strategy sync_mq name(%s) start err:%s", strategy.name, err.Error())
	}
	//routine run
	routine.Go(strategy.getMqMessage)
	routine.Go(strategy.monitor)
	logger.Info("[StrategySyncMq.Run] mq consumer start success.name:%s", strategy.name)
	return nil
}

func (strategy *StrategySyncMq) monitor() {
	select {
	case <-strategy.ctx.Done():
		logger.Info("[StrategySyncMq.monitor] context canceled. name:%s", strategy.name)
		//close mq
		//will deadlock if not use goroutine
		//because of not send message
		strategy.mqConsumer.Close()
		//close channel
		close(strategy.messageChan)
	}
}

func (strategy *StrategySyncMq) getMqMessage() {
	for {
		//read message
		var msg *TopicMessage
		select {
		case msg = <-strategy.messageChan:
			//fix nil panic
			if msg == nil {
				goto end1
			}
			//biz context with requestId
			ctx := &context.BizContext{
				RequestId:   util.GetTraceId(),
				BaseContext: strategy.ctx,
			}
			strategy.safeHook(ctx, msg)
		}
	}
end1:
	logger.Info("[StrategySyncMq.getMqMessage] exit success. name:%s", strategy.name)
}

func (strategy *StrategySyncMq) safeHook(ctx *context.BizContext, msg *TopicMessage) (err error) {
	defer func(begin time.Time) {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic")
			monitor.UpdatePanic("syncmq")
			logger.Error("[StrategySyncMq.safeHook] requestId:%s,name:%s, #Panic#(%v),backTrace:%s", ctx.RequestId, strategy.name, p, string(debug.Stack()))
		}
		//timeUsed
		interval := util.GetDurationMillis(begin)
		monitor.UpdateStrategy("syncmq", strategy.name, interval, err)
		logger.Info("[StrategySyncDirect.safeHook] requestId:%s,name:%s,msg:%s,timeUsed:%dus,err:%v", ctx.RequestId, strategy.name, msg.Message, interval, err)
	}(time.Now())
	err = strategy.hook.HandleMqMessage(ctx, msg)
	if err != nil {
		logger.Error("[StrategySyncMq.safeHook] requestId:%s,HandleMqMessage error. name:%s,msg:%s,err:%s", ctx.RequestId, strategy.name, util.StructToJson(msg), err.Error())
	}
	return nil
}
