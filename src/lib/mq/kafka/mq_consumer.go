package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"io"
	"lib/mq"
	"sync"
)

const (
	GLOBAL_TOPIC_CALLBACK_KEY = "kafka_iot_default_topic"
)

type kafkaContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type KafkaConsumer struct {
	wg                     *sync.WaitGroup
	kafkaCtx               *kafkaContext
	isStart                bool
	mutex                  sync.Mutex
	logPath                string
	version                string
	clientId               string
	brokers                []string
	fileCloser             io.Closer
	callBackMap            map[string]mq.IConsumerCallback
	mapGroupTopics         map[string][]string
	mapConsumerGroupTopics map[sarama.ConsumerGroup][]string
}

func NewMQConsumer(confPath string, topicModels []*mq.ConsumerTopicModel, logPath string) *KafkaConsumer {
	//parse config
	conf := parseKafkaConf(confPath)
	//context
	ctx, cancel := context.WithCancel(context.Background())
	kafkaCtx := &kafkaContext{
		ctx:    ctx,
		cancel: cancel,
	}
	//load
	consumer := &KafkaConsumer{
		kafkaCtx:               kafkaCtx,
		wg:                     new(sync.WaitGroup),
		logPath:                logPath,
		version:                conf.Version,
		clientId:               conf.ClientID,
		brokers:                conf.BrokerList,
		mapGroupTopics:         make(map[string][]string, 0),
		callBackMap:            make(map[string]mq.IConsumerCallback),
		mapConsumerGroupTopics: make(map[sarama.ConsumerGroup][]string),
	}
	//add group topicModels
	for _, item := range topicModels {
		if _, ok := consumer.mapGroupTopics[item.Group]; !ok {
			consumer.mapGroupTopics[item.Group] = make([]string, 0)
		}
		consumer.mapGroupTopics[item.Group] = append(consumer.mapGroupTopics[item.Group], item.Topic)
	}
	return consumer
}

func (mq *KafkaConsumer) RegisterByTopic(topic string, callback mq.IConsumerCallback) error {
	if mq.isStart {
		return fmt.Errorf("[kafka.consumer] mq already start")
	}
	mq.mutex.Lock()
	mq.callBackMap[topic] = callback
	mq.mutex.Unlock()
	return nil
}

func (mq *KafkaConsumer) Register(callback mq.IConsumerCallback) error {
	if mq.isStart {
		return fmt.Errorf("[kafka.consumer] mq already start")
	}
	mq.mutex.Lock()
	mq.callBackMap[GLOBAL_TOPIC_CALLBACK_KEY] = callback
	mq.mutex.Unlock()
	return nil
}

func (mq *KafkaConsumer) Start() error {
	if mq.isStart {
		return fmt.Errorf("[kafka.consumer] mq already start")
	}
	//logger
	var err error
	if mq.fileCloser, err = setRedirectLogger(mq.logPath); err != nil {
		return err
	}
	//version
	kafkaVer, err := sarama.ParseKafkaVersion(mq.version)
	if err != nil {
		return fmt.Errorf("[kafka.consumer] kafka version(%s) parse err:%s", mq.version, err.Error())
	}
	//config
	conf := sarama.NewConfig()
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	conf.Version = kafkaVer
	conf.ClientID = mq.clientId
	//init
	for group, topics := range mq.mapGroupTopics {
		consumerGroup, err := sarama.NewConsumerGroup(mq.brokers, group, conf)
		if err != nil {
			return fmt.Errorf("[kafka.consumer] new group error.broker:%v,group:%s", mq.brokers, group)
		}
		mq.mapConsumerGroupTopics[consumerGroup] = topics
	}
	mq.consumeAll()
	//start
	mq.isStart = true
	return nil
}

func (mq *KafkaConsumer) consumeAll() {
	for consumerGroup, topics := range mq.mapConsumerGroupTopics {
		mq.consumeOne(consumerGroup, topics)
	}
}

func (mq *KafkaConsumer) consumeOne(consumerGroup sarama.ConsumerGroup, topics []string) {
	//group wait
	mq.wg.Add(1)
	//routine
	go func() {
		defer mq.wg.Done()
		for {
			if err := consumerGroup.Consume(mq.kafkaCtx.ctx, topics, newImplGroupHandler(mq)); err != nil {
				sarama.Logger.Printf("[error] consumeOne error. topic:%v,err:%s", topics, err.Error())
				return
			}
			//ctx cancel
			if mq.kafkaCtx.ctx.Err() != nil {
				return
			}
		}
	}()
}

func (mq *KafkaConsumer) Close() error {
	//cancel context
	mq.kafkaCtx.cancel()
	//wait returned
	mq.wg.Wait()
	//close client
	for consumerGroup, topics := range mq.mapConsumerGroupTopics {
		if err := consumerGroup.Close(); err != nil {
			sarama.Logger.Printf("[error] close consumer group error. topic:%v,err:%s", topics, err.Error())
		}
	}
	//close file
	if mq.fileCloser != nil {
		if err := mq.fileCloser.Close(); err != nil {
			return fmt.Errorf("[kafka.consumer] close file err:%s", err.Error())
		}
		mq.fileCloser = nil
	}
	return nil
}

func (mq *KafkaConsumer) getCallBack(topic string) mq.IConsumerCallback {
	defer mq.mutex.Unlock()
	mq.mutex.Lock()
	if callback, ok := mq.callBackMap[topic]; ok {
		return callback
	} else if callback, ok := mq.callBackMap[GLOBAL_TOPIC_CALLBACK_KEY]; ok {
		return callback
	} else {
		return nil
	}

}

//implement group handler interface
type implGroupHandler struct {
	consumer *KafkaConsumer
}

func newImplGroupHandler(consumer *KafkaConsumer) *implGroupHandler {
	handler := &implGroupHandler{
		consumer: consumer,
	}
	return handler
}

//implement Setup
func (handler *implGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

//implement Cleanup
func (handler *implGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

//implement ConsumeClaim
func (handler *implGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		callback := handler.consumer.getCallBack(message.Topic)
		if callback == nil {
			sarama.Logger.Printf("[error] ConsumeClaim error. topic(%s) getCallBack is nil", message.Topic)
			continue
		}
		callback.DistributeTopic(message.Topic, string(message.Value))
		//marked be consumed
		session.MarkMessage(message, "")
		//ctx cancel
		if session.Context().Err() != nil {
			break
		}
	}
	sarama.Logger.Printf("[info] ConsumeClaim context canceled. topic:%s", claim.Topic())
	return nil
}
