package shared

import (
    "time"
)

type Error struct {
	Message   string `json:"message"`
	Stack     string `json:"stack"`
	Timestamp uint64 `json:"timestamp"`
	Component string `json:"component"`
	Event     string `json:"event"`
}

type ErrorMessage struct {
	MessageId string  `json:"message_id"`
	Errors    []Error `json:"errors"`
}

func NewError(message string, stack string, component string, event string) *Error {
    return &Error{
        Message: message,
        Stack: stack,
        Timestamp: uint64(time.UnixNano())
        Component: component,
        Event: event
    }
}
