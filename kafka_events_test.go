package events

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	// mocks "github.com/matthogan/polypully-events/internal/mocks"
)

func TestNotify(t *testing.T) {
	ctrl := gomock.NewController(t)
	// Ensure that all expected calls are made
	defer ctrl.Finish()

	mockProducer := NewMockKafkaProducer(ctrl)
	mockProducer.EXPECT().Produce(gomock.Any(), nil).Return(nil).Times(1)

	config := &EventsConfig{}
	e := Events{
		Producer: mockProducer,
		config:   config,
	}
	err := e.init(config)
	if err != nil {
		t.Errorf("init() error = %v, expected %v", err, false)
	}

	err = e.Notify(&Event{Value: "test message"})
	if err != nil {
		t.Errorf("Notify() error = %v, expected %v", err, false)
	}
}

func TestClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	// Ensure that all expected calls are made
	defer ctrl.Finish()

	mockProducer := NewMockKafkaProducer(ctrl)
	mockProducer.EXPECT().Close().Times(1)

	sut := Events{
		Producer: mockProducer,
		cancel:   context.CancelFunc(func() {}),
	}
	sut.Close()
}
