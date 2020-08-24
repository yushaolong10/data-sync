package syncmq

import (
	"infrastructure/config"
	"infrastructure/context"
	"infrastructure/mq"
)

//broker consumer
func NewMqConsumer(ctx *context.BaseContext, conf config.MqConfig, messageChan chan<- *TopicMessage) *MqConsumerServer {
	consumer := &MqConsumerServer{
		Ctx:         ctx,
		Kind:        conf.Kind,
		Topics:      conf.Topics,
		Cluster:     conf.Cluster,
		ConfPath:    conf.ConfPath,
		LogPath:     conf.LogPath,
		Group:       conf.Group,
		messageChan: messageChan,
	}
	return consumer
}

type TopicMessage struct {
	TopicName string
	Message   string
}

type MqConsumerServer struct {
	Ctx         *context.BaseContext
	Kind        string
	Topics      []string
	Cluster     string
	ConfPath    string
	LogPath     string
	Group       string
	mqConsumer  mq.MQConsumer
	messageChan chan<- *TopicMessage //消息管道,写入流
}

//callback
func (consumer *MqConsumerServer) DistributeTopic(topicName string, message string) {
	if consumer.Ctx.IsCanceled() {
		return
	}
	msg := TopicMessage{
		TopicName: topicName,
		Message:   message,
	}
	consumer.messageChan <- &msg
}

//start
func (consumer *MqConsumerServer) Start() error {
	//start consumer
	topicModelList := make([]*mq.ConsumerTopicModel, 0)
	for _, v := range consumer.Topics {
		topicModelList = append(topicModelList, &mq.ConsumerTopicModel{consumer.Group, v})
	}
	mqConsumer := mq.NewMQConsumer(consumer.Kind, consumer.Cluster, consumer.ConfPath, consumer.LogPath, topicModelList)
	mqConsumer.Register(consumer)
	if err := mqConsumer.Start(); err != nil {
		return err
	}
	consumer.mqConsumer = mqConsumer
	return nil
}

func (consumer *MqConsumerServer) Close() {
	if consumer == nil {
		return
	}
	if consumer.mqConsumer != nil {
		consumer.mqConsumer.Close()
	}
}
