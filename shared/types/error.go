package shared

import (
	"time"
)

type Error struct {
	Message   string `json:"message" cassandra:"message"`
	Stack     string `json:"stack" cassandra:"stack"`
	Timestamp uint64 `json:"timestamp" cassandra:"timestamp"`
	Component string `json:"component" cassandra:"component"`
	Event     string `json:"event" cassandra:"event"`
}

type ErrorMessage struct {
	MessageId string  `json:"message_id"`
	Errors    []Error `json:"errors"`
}

func NewError(message string, stack string, component string, event string) *Error {
	return &Error{
		Message:   message,
		Stack:     stack,
		Timestamp: uint64(time.Now().UnixNano()),
		Component: component,
		Event:     event,
	}
}
