package mq

import (
	"lib/mq"
	"lib/mq/kafka"
	"lib/mq/qbus"
)

type MQConsumer interface {
	RegisterByTopic(topic string, callback mq.IConsumerCallback) error
	Register(callback mq.IConsumerCallback) error
	Start() error
	Close() error
}

type ConsumerTopicModel struct {
	Group string
	Topic string
}

func NewMQConsumer(kind string, cluster string, confPath string, logPath string, topicModels []*ConsumerTopicModel) MQConsumer {
	//translate mq topics
	mqTopics := translateToMqTopicModel(topicModels)
	//choose
	switch kind {
	case "kafka":
		return kafka.NewMQConsumer(confPath, mqTopics, logPath)
	case "qbus":
		return qbus.NewMQConsumer(cluster, confPath, logPath, mqTopics)
	default:
		return qbus.NewMQConsumer(cluster, confPath, logPath, mqTopics)
	}
}

//translate
func translateToMqTopicModel(topicModels []*ConsumerTopicModel) []*mq.ConsumerTopicModel {
	returns := make([]*mq.ConsumerTopicModel, 0, len(topicModels))
	for _, o := range topicModels {
		item := mq.ConsumerTopicModel{
			Group: o.Group,
			Topic: o.Topic,
		}
		returns = append(returns, &item)
	}
	return returns
}
