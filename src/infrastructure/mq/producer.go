package mq

import (
	"lib/mq/kafka"
	"lib/mq/qbus"
)

type MQProducer interface {
	Start() error
	Close() error
	Produce(topic string, msg string) error
}

func NewMQProducer(kind string, cluster string, logPath string, confPath string, topics []string) MQProducer {
	//choose
	switch kind {
	case "kafka":
		return kafka.NewMQProducer(confPath, topics, logPath)
	case "qbus":
		return qbus.NewMQProducer(cluster, logPath, confPath, topics)
	default:
		return qbus.NewMQProducer(cluster, logPath, confPath, topics)
	}
}
