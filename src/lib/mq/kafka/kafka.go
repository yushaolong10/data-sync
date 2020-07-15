package kafka

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/Shopify/sarama"
	"io"
	"log"
	"os"
)

// this library support kafka server version from V0.8.2.0 to V2.1.0
//
// according to the current golang version:1.10.5 in our program.
// we choose third-party lib (sarama) at version v1.21.0 as kafka client.
// see detail: https://github.com/Shopify/sarama/tree/v1.21.0

//refer to demo:
//https://ednsquare.com/question/how-to-create-a-kafka-consumer-group-in-golang-with-sarama-------TFhWKM

//config
type KafkaConfig struct {
	Version    string   `toml:"kafka_version"`
	ClientID   string   `toml:"client_id"`
	BrokerList []string `toml:"broker_list"`
}

//parse config
func parseKafkaConf(path string) *KafkaConfig {
	var kafkaConf = KafkaConfig{}
	if _, err := toml.DecodeFile(path, &kafkaConf); err != nil {
		panic(fmt.Sprintf("kafka config(%s) loader fail %s", path, err.Error()))
	}
	return &kafkaConf
}

//set logger
func setRedirectLogger(logPath string) (io.Closer, error) {
	writer, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("[kafka] open file (%s) err:%s", logPath, err.Error())
	}
	//log
	sarama.Logger = log.New(writer, "[kafka] ", log.LstdFlags)
	return writer, nil
}
