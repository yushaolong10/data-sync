package qbus

import (
	"360.cn/qbus"
	"fmt"
	"lib/mq"
	"sync"
)

const (
	GLOBAL_TOPIC_CALLBACK_KEY = "iot_default_topic"
)

type MQConsumer struct {
	isStart     bool
	cluster     string
	conf        string
	logPath     string
	topics      map[string]*mq.ConsumerTopicModel
	consumerMap map[string]qbus.QbusConsumer
	innerBack   qbus.QbusConsumerCallback
	mutex       sync.Mutex
	callBackMap map[string]mq.IConsumerCallback
}

func NewMQConsumer(cluster string, conf string, path string, topics []*mq.ConsumerTopicModel) *MQConsumer {
	consumer := &MQConsumer{cluster: cluster, conf: conf, logPath: path, topics: make(map[string]*mq.ConsumerTopicModel), callBackMap: make(map[string]mq.IConsumerCallback)}
	consumer.consumerMap = make(map[string]qbus.QbusConsumer)
	consumer.innerBack = qbus.NewDirectorQbusConsumerCallback(newInnerCallerBack(consumer))
	for _, topic := range topics {
		consumer.topics[topic.Topic] = topic
	}
	return consumer
}

func (qc *MQConsumer) RegisterByTopic(topic string, callback mq.IConsumerCallback) error {
	defer qc.mutex.Unlock()
	qc.mutex.Lock()
	qc.callBackMap[topic] = callback
	return nil
}

func (qc *MQConsumer) Register(callback mq.IConsumerCallback) error {
	defer qc.mutex.Unlock()
	qc.mutex.Lock()
	qc.callBackMap[GLOBAL_TOPIC_CALLBACK_KEY] = callback
	return nil
}

func (qc *MQConsumer) SubscribeOne(group string, topic string) bool {
	if qc.isStart {
		return false
	}
	qc.topics[topic] = &mq.ConsumerTopicModel{Group: group, Topic: topic}
	return true
}

func (qc *MQConsumer) Subscribe(topics []*mq.ConsumerTopicModel) bool {
	if qc.isStart {
		return false
	}
	for _, topic := range topics {
		qc.topics[topic.Topic] = topic
	}
	return true
}

func (qc *MQConsumer) Start() error {
	for _, topicModel := range qc.topics {
		consumer := qbus.NewQbusConsumer()
		if ok := consumer.Init(qc.cluster, qc.logPath, qc.conf, qbus.NewDirectorQbusConsumerCallback(newInnerCallerBack(qc))); !ok {
			return fmt.Errorf("mq(qbus) consumer init failed, cluster=%s", qc.cluster)
		}
		if ok := consumer.SubscribeOne(topicModel.Group, topicModel.Topic); !ok {
			return fmt.Errorf("mq(qbus) consumer subscribe error,group=%s,topic=%s", topicModel.Group, topicModel.Topic)
		}
		if ok := consumer.Start(); !ok {
			return fmt.Errorf("mq(qbus) consumer start error, cluster=%s", qc.cluster)
		}
		qc.consumerMap[topicModel.Topic] = consumer
	}
	return nil
}

func (qc *MQConsumer) Close() error {
	for _, consumer := range qc.consumerMap {
		consumer.Stop()
		qbus.DeleteQbusConsumer(consumer)
		//qbus.DeleteDirectorQbusConsumerCallback(qc.innerBack)
	}
	return nil
}

func (qc *MQConsumer) getCallBack(topic string) mq.IConsumerCallback {
	defer qc.mutex.Unlock()
	qc.mutex.Lock()
	if callback, ok := qc.callBackMap[topic]; ok {
		return callback
	} else if callback, ok := qc.callBackMap[GLOBAL_TOPIC_CALLBACK_KEY]; ok {
		return callback
	} else {
		return nil
	}

}

type innerCallerBack struct {
	qc *MQConsumer
}

func newInnerCallerBack(qbusConsumer *MQConsumer) *innerCallerBack {
	inner := &innerCallerBack{qbusConsumer}
	return inner

}

//纯手工提交offset, 需要在consumer.config中添加user.manual.commit.offset=true
func (p *innerCallerBack) DeliveryMsgForCommitOffset(msg_info qbus.QbusMsgContentInfo) {
	//excute nothing
}
func (p *innerCallerBack) DeliveryMsg(topic string, msg string, msg_len int64) {
	callback := p.qc.getCallBack(topic)
	if callback == nil {
		return
	}
	callback.DistributeTopic(topic, msg[:msg_len])
}
