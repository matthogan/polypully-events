package events

import (
	"testing"
)

func TestDummyNotify(t *testing.T) {
	sut := Dummy{}
	if err := sut.init(&EventsConfig{}); err != nil {
		t.Errorf("init() error = %v, expected %v", err, nil)
	}
	actual := sut.Notify(&Event{})
	if actual != nil {
		t.Errorf("Notify() = %v; should be nil", actual)
	}
}

func TestDummyClose(t *testing.T) {
	sut := Dummy{}
	sut.Close()
}
