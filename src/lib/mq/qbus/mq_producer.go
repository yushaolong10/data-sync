package qbus

import (
	"360.cn/qbus"
	"fmt"
)

type MQProducer struct {
	cluster    string
	conf       string
	logPath    string
	topicProds map[string]qbus.QbusProducer
}

func NewMQProducer(cluster string, logPath string, confPath string, topics []string) *MQProducer {
	mqProducer := &MQProducer{cluster: cluster, logPath: logPath, conf: confPath}
	mqProducer.topicProds = make(map[string]qbus.QbusProducer)
	for _, topic := range topics {
		mqProducer.topicProds[topic] = qbus.NewQbusProducer()
	}
	return mqProducer
}

func (mp *MQProducer) Start() error {
	for topic, producer := range mp.topicProds {
		if ok := producer.Init(mp.cluster, mp.logPath, mp.conf, topic); !ok {
			return fmt.Errorf("init error ,topic=%s", topic)
		}
	}
	return nil
}

func (mp *MQProducer) Produce(topic string, msg string) error {
	if producer, ok := mp.topicProds[topic]; ok {
		if !producer.Produce(msg, int64(len(msg)), "") {
			return fmt.Errorf("product topic err,topic=%s", topic)
		}
	} else {
		return fmt.Errorf("no init this topic=%s", topic)
	}
	return nil
}

func (mp *MQProducer) Close() error {
	for _, producer := range mp.topicProds {
		producer.Uninit()
		qbus.DeleteQbusProducer(producer)
	}
	return nil
}
