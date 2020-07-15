package mq

// consumer callback
type IConsumerCallback interface {
	DistributeTopic(topic string, message string)
}

//consumer topic & group
type ConsumerTopicModel struct {
	Group string
	Topic string
}
