package events

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Init the producer
func (e *Events) init(config *EventsConfig) error {
	if err := validate(config); err != nil {
		return err
	}
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"client.id":         config.ClientId,
		"acks":              config.Acks}
	for k, v := range config.Config {
		kafkaConfig[k] = v
	}
	// default producer is kafka
	if e.Producer == nil {
		producer, err := kafka.NewProducer(&kafkaConfig)
		if err != nil {
			return fmt.Errorf("failed to create producer: %v", err)
		}
		e.Producer = producer
	}
	if e.Producer == nil {
		return fmt.Errorf("failed to create producer")
	}
	slog.Info("producer connected", "config", config)
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.config = config
	go e.handleDeliveryReports()
	return nil
}

// Close the producer
func (e *Events) Close() {
	e.producerMutex.Lock()
	defer e.producerMutex.Unlock()
	if e.cancel != nil {
		e.cancel()
	}
	if e.Producer != nil {
		e.Producer.Close()
	}
	slog.Info("producer shutdown complete")
}

// Notify sends an event
func (e *Events) Notify(event *Event) error {
	e.producerMutex.Lock()
	defer e.producerMutex.Unlock()
	slog.Debug("sending event", "event", event)
	if e.Producer == nil {
		return fmt.Errorf("producer not initialized")
	}
	err := e.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &e.config.Topic, Partition: kafka.PartitionAny},
		Key:            []byte("downloader"), // better partitioning...
		Value:          []byte(event.Value),
		Headers:        e.getHeaders(event),
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to send event: %s", err)
	}
	return nil
}

func (e *Events) getHeaders(event *Event) []kafka.Header {
	headers := []kafka.Header{{Key: "createdTimestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))}}
	if e.config.ProducerId != "" {
		headers = append(headers, kafka.Header{Key: "producerId", Value: []byte(e.config.ProducerId)})
	}
	if event.Type != "" {
		headers = append(headers, kafka.Header{Key: "messageType", Value: []byte(event.Type)})
	}
	if event.CorrelationId != "" {
		headers = append(headers, kafka.Header{Key: "correlationId", Value: []byte(event.CorrelationId)})
	}
	if event.ContentType != "" {
		headers = append(headers, kafka.Header{Key: "contentType", Value: []byte(event.ContentType)})
	} else {
		headers = append(headers, kafka.Header{Key: "contentType", Value: []byte("text/plain")})
	}
	return headers
}

func (e *Events) handleDeliveryReports() {
	if !e.config.Enabled {
		slog.Debug("Events disabled so not listening for delivery reports")
		return
	}
	slog.Debug("Listening for delivery reports")
	for {
		select {
		case <-e.ctx.Done():
			return
		case e, ok := <-e.Producer.Events():
			if !ok {
				return // The Events channel was closed
			}
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					slog.Error("event", "delivery failed", ev.TopicPartition.Error)
				} else {
					slog.Debug(fmt.Sprintf("successfully produced to topic %s partition [%d] @ offset %s %v",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset.String(), ev))
				}
			case kafka.Error:
				slog.Error("event", "failed", ev.Code().String(), "error", ev.Error())
			default:
				slog.Debug("event", "ignored", ev)
			}
		}
	}
}

func validate(config *EventsConfig) error {
	if config.Acks == "" {
		config.Acks = "all"
	}
	if !config.Enabled {
		return nil
	}
	if config.BootstrapServers == "" {
		return fmt.Errorf("bootstrap servers not set")
	}
	if config.ClientId == "" {
		return fmt.Errorf("clientId not set")
	}
	if config.Topic == "" {
		return fmt.Errorf("topic not set")
	}
	return nil
}
