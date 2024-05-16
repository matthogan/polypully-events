package events

import (
	"reflect"
	"testing"
)

func TestNewEvents_Dummy(t *testing.T) {
	config := &EventsConfig{}
	events, err := NewEvents(config)
	if err != nil {
		t.Errorf("NewEvents(config) error = %v, expected %v", err, false)
	}
	if reflect.TypeOf(events) != reflect.TypeOf((*Dummy)(nil)) {
		t.Errorf("NewEvents(config) = %v, expected %v",
			reflect.TypeOf(events), reflect.TypeOf((*Dummy)(nil)))
	}
}

func TestNewEvents(t *testing.T) {
	config := &EventsConfig{
		Enabled:          true,
		BootstrapServers: "localhost:9092",
		ClientId:         "test",
		Topic:            "test",
	}
	events, err := NewEvents(config)
	if err != nil {
		t.Errorf("NewEvents(config) error = %v, expected %v", err, false)
	}
	if reflect.TypeOf(events) != reflect.TypeOf((*Events)(nil)) {
		t.Errorf("NewEvents(config) = %v, expected %v",
			reflect.TypeOf(events), reflect.TypeOf((*Events)(nil)))
	}
}

func TestNewServiceEvent(t *testing.T) {
	value := "test"
	event := NewServiceEvent(value)
	if event.Type != "service" {
		t.Errorf("NewServiceEvent(value) = %v, expected %v", event.Type, "service")
	}
	if event.ContentType != "text/plain" {
		t.Errorf("NewServiceEvent(value) = %v, expected %v", event.ContentType, "text/plain")
	}
	if event.Value != value {
		t.Errorf("NewServiceEvent(value) = %v, expected %v", event.Value, value)
	}
}

func TestNewDownloadEvent(t *testing.T) {
	value := "test"
	correlationId := "test"
	event := NewDownloadEvent(value, correlationId)
	if event.Type != "download" {
		t.Errorf("NewDownloadEvent(value, correlationId) = %v, expected %v", event.Type, "download")
	}
	if event.ContentType != "text/plain" {
		t.Errorf("NewDownloadEvent(value, correlationId) = %v, expected %v", event.ContentType, "text/plain")
	}
	if event.Value != value {
		t.Errorf("NewDownloadEvent(value, correlationId) = %v, expected %v", event.Value, value)
	}
	if event.CorrelationId != correlationId {
		t.Errorf("NewDownloadEvent(value, correlationId) = %v, expected %v", event.CorrelationId, correlationId)
	}
}
