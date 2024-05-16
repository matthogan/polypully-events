package events

import (
	"context"
	"sync"
)

var _ EventsApi = (*Events)(nil)

type EventsConfig struct {
	Enabled          bool              // disabled returns a dummy producer
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
	Producer      KafkaProducer
	producerMutex sync.Mutex
	config        *EventsConfig
}

type EventsApi interface {
	init(*EventsConfig) error
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
	if config.Enabled {
		events := Events{}
		if err := events.init(config); err != nil {
			return nil, err
		}
		return &events, nil
	} else {
		events := Dummy{}
		return &events, nil
	}
}
