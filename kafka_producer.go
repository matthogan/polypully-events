package events

import "github.com/confluentinc/confluent-kafka-go/kafka"

type KafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
	Events() chan kafka.Event
}
