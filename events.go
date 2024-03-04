package events

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var _ EventsApi = (*Events)(nil)

type EventsConfig struct {
	BootstrapServers string            // comma separated list of brokers
	ClientId         string            // identify the client in the logs
	Acks             string            // defaults to all
	Topic            string            // eg. "downloads"
	ProducerId       string            // identify the producer such as node name, etc
	Config           map[string]string // any other kafka producer config
}

type Event struct {
	// any string that makes sense in the context of the client such as "Error"
	Type string
	// context such as an error message
	Value string
	// correlation id to group messages
	CorrelationId string
	// content type such as application/json
	ContentType string
}

type Events struct {
	ctx           context.Context
	cancel        context.CancelFunc
	producer      *kafka.Producer
	producerMutex sync.Mutex
	config        *EventsConfig
}

type EventsApi interface {
	Notify(*Event) error
	Close()
}

func NewServiceEvent(value string) *Event {
	return &Event{Type: "service", ContentType: "text/plain", Value: value}
}

func NewDownloadEvent(value string, correlationId string) *Event {
	return &Event{Type: "download", ContentType: "text/plain",
		Value: value, CorrelationId: correlationId}
}

func NewEvents(config *EventsConfig) (EventsApi, error) {
	events := Events{}
	if err := events.init(config); err != nil {
		return nil, err
	}
	return &events, nil
}

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
	p, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}
	if p == nil {
		return fmt.Errorf("failed to create producer")
	}
	slog.Info("producer connected", "config", config)
	e.ctx, e.cancel = context.WithCancel(context.Background())
	e.producer = p
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
	if e.producer != nil {
		e.producer.Close()
	}
	slog.Info("producer shutdown complete")
}

// Notify sends an event
func (e *Events) Notify(event *Event) error {
	e.producerMutex.Lock()
	defer e.producerMutex.Unlock()
	slog.Debug("sending event", "event", event)
	if e.producer == nil {
		return fmt.Errorf("producer not initialized")
	}
	err := e.producer.Produce(&kafka.Message{
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
	slog.Debug("Listening for delivery reports")
	for {
		select {
		case <-e.ctx.Done():
			return
		case e, ok := <-e.producer.Events():
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
	if config.BootstrapServers == "" {
		return fmt.Errorf("bootstrap servers not set")
	}
	if config.ClientId == "" {
		return fmt.Errorf("clientId not set")
	}
	if config.Acks == "" {
		config.Acks = "all"
	}
	if config.Topic == "" {
		return fmt.Errorf("topic not set")
	}
	return nil
}
