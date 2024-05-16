package events

import (
	"log/slog"
)

type Dummy struct {
	Events
}

// Init the producer
func (e *Dummy) init(config *EventsConfig) error {
	slog.Debug("dummy producer init", "config", config)
	return nil
}

// Close the producer
func (e *Dummy) Close() {
	slog.Debug("dummy producer closed")
}

// Notify sends an event
func (e *Dummy) Notify(event *Event) error {
	slog.Debug("dummy event", "event", event)
	return nil
}
