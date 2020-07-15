package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"io"
)

type KafkaProducer struct {
	version    string
	clientId   string
	brokers    []string
	logPath    string
	fileCloser io.Closer
	topicProds map[string]sarama.AsyncProducer
}

func NewMQProducer(confPath string, topics []string, logPath string) *KafkaProducer {
	//parse config
	conf := parseKafkaConf(confPath)
	//producer
	mqProducer := &KafkaProducer{
		version:  conf.Version,
		clientId: conf.ClientID,
		brokers:  conf.BrokerList,
		logPath:  logPath,
	}
	mqProducer.topicProds = make(map[string]sarama.AsyncProducer)
	for _, topic := range topics {
		mqProducer.topicProds[topic] = nil
	}
	return mqProducer
}

func (mq *KafkaProducer) Start() error {
	//logger
	var err error
	if mq.fileCloser, err = setRedirectLogger(mq.logPath); err != nil {
		return err
	}
	//version
	kafkaVer, err := sarama.ParseKafkaVersion(mq.version)
	if err != nil {
		return fmt.Errorf("[kafka.producer] kafka version(%s) parse err:%s", mq.version, err.Error())
	}
	//config
	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewRandomPartitioner
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
	conf.Version = kafkaVer
	conf.ClientID = mq.clientId
	//create async producer
	for topic, _ := range mq.topicProds {
		producer, err := sarama.NewAsyncProducer(mq.brokers, conf)
		if err != nil {
			return fmt.Errorf("[kafka.producer] start error,topic=%s,err=%s", topic, err.Error())
		}
		mq.topicProds[topic] = producer
	}
	return nil
}

func (mq *KafkaProducer) Produce(topic string, msg string) error {
	//find
	_, ok := mq.topicProds[topic]
	if !ok {
		return fmt.Errorf("[kafka.producer] produce topic(%s) not exist", topic)
	}
	//input
	mq.topicProds[topic].Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(msg)}
	//check
	select {
	case <-mq.topicProds[topic].Successes():
		return nil
	case err := <-mq.topicProds[topic].Errors():
		return fmt.Errorf("[kafka.producer] async err:%s", err.Error())
	}
	return nil
}

func (mq *KafkaProducer) Close() error {
	for topic, producer := range mq.topicProds {
		if err := producer.Close(); err != nil {
			sarama.Logger.Printf("[error] close producer error. topic:%s,err:%s", topic, err.Error())
		}
	}
	//close file
	if mq.fileCloser != nil {
		if err := mq.fileCloser.Close(); err != nil {
			return fmt.Errorf("[kafka.producer] close file err:%s", err.Error())
		}
		mq.fileCloser = nil
	}
	return nil
}
