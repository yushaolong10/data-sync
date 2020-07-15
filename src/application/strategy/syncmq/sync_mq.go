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
	HandleMqMessage(message *TopicMessage) error
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
	logger.Info("[StrategySyncMq.Run] mq consumer start success.name:%s", strategy.name)
	return nil
}

func (strategy *StrategySyncMq) close() {
	//close mq
	strategy.mqConsumer.Close()
	//close channel
	close(strategy.messageChan)
}

func (strategy *StrategySyncMq) getMqMessage() {
	for {
		//check if canceled
		if strategy.ctx.IsCanceled() {
			strategy.close()
			logger.Info("[StrategySyncMq.getMqMessage] context canceled. name:%s", strategy.name)
			return
		}
		//read message
		var msg *TopicMessage
		select {
		case msg = <-strategy.messageChan:
			strategy.safeHook(msg)
		}
	}
}

func (strategy *StrategySyncMq) safeHook(msg *TopicMessage) (err error) {
	defer func(begin time.Time) {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic")
			monitor.UpdatePanic("syncmq")
			logger.Error("[StrategySyncMq.safeHook] name:%s, #Panic#(%v),backTrace:%s", strategy.name, p, string(debug.Stack()))
		}
		//timeUsed
		interval := util.GetDurationMillis(begin)
		monitor.UpdateStrategy("syncmq", strategy.name, interval, err)
		logger.Info("[StrategySyncDirect.safeHook] name:%s,msg:%s,timeUsed:%dus,err:%v", strategy.name, msg.Message, interval, err)
	}(time.Now())
	err = strategy.hook.HandleMqMessage(msg)
	if err != nil {
		logger.Error("[StrategySyncMq.safeHook] HandleMqMessage error. name:%s,msg:%s,err:%s", strategy.name, util.StructToJson(msg), err.Error())
	}
	return nil
}
