package shared

import (
	"time"
)

type Error struct {
	Message   string    `json:"message" cassandra:"message"`
	Stack     string    `json:"stack" cassandra:"stack"`
	Timestamp time.Time `json:"timestamp" cassandra:"timestamp"`
	Component string    `json:"component" cassandra:"component"`
	Event     string    `json:"event" cassandra:"event"`
}

type ErrorMessage struct {
	MessageId string  `json:"message_id"`
	Errors    []Error `json:"errors"`
}

func NewError(message string, stack string, component string, event string) *Error {
	return &Error{
		Message:   message,
		Stack:     stack,
		Timestamp: time.Now(),
		Component: component,
		Event:     event,
	}
}

type ErrorProducer struct {
	Component string
}

func NewErrorProducer(component string) *ErrorProducer {
	return &ErrorProducer{
		Component: component,
	}
}

func (e *ErrorProducer) Produce(message string, stack string, event string) *Error {
	return NewError(message, stack, e.Component, event)
}
