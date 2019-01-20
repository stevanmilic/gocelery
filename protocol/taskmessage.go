package protocol

import (
	"fmt"

	"github.com/gocelery/gocelery/errors"
)

// TaskMessage format
// TaskMessage Protocol v1 has empty headers
// http://docs.celeryproject.org/en/latest/internals/protocol.html
type TaskMessage struct {
	ContentType     string                 `json:"content-type"`      // default application/json
	ContentEncoding string                 `json:"content-encoding"`  // default utf-8
	Headers         map[string]interface{} `json:"headers,omitempty"` // empty for Message Protocol v1
	Properties      Properties             `json:"properties"`
	Body            string                 `json:"body"` // encoded message body
}

// Properties for TaskMessage
type Properties struct {
	CorrelationID string       `json:"correlation_id"`
	ReplyTo       string       `json:"reply_to,omitempty"` // optional
	BodyEncoding  string       `json:"body_encoding"`      // default base64
	DeliveryTag   string       `json:"delivery_tag"`
	DeliveryMode  int          `json:"delivery_mode"` // default 1
	DeliveryInfo  DeliveryInfo `json:"delivery_info"`

	// priority field used to be in delivery_info for older version
	// for compatibility reason, this field is not parsed
	// Priority      int          `json:"priority"`
}

// DeliveryInfo for TaskMessage Protocol v1 Properties
type DeliveryInfo struct {
	RoutingKey string `json:"routing_key"` // ex) task.succeeded
	Exchange   string `json:"exchange"`    // default celeryev
}

// Validate validates TaskMessage
func (tm *TaskMessage) Validate(contentType string, msgProtocolVer int) error {
	// application/json, x-yaml, x-python-serialize, x-msgpack
	if tm.ContentType != contentType {
		return errors.ErrUnsupportedContentType
	}
	if tm.ContentEncoding != "utf-8" {
		return errors.ErrUnsupportedContentEncoding
	}
	if tm.Properties.BodyEncoding != "base64" {
		return errors.ErrUnsupportedBodyEncoding
	}
	// check if header is empty
	if len(tm.Headers) == 0 && msgProtocolVer != 1 {
		return fmt.Errorf("headers must be empty for message protocol v1")
	}
	if msgProtocolVer == 2 {
		if _, ok := tm.Headers["id"]; !ok {
			return fmt.Errorf("header id is empty!")
		}
		if _, ok := tm.Headers["root_id"]; !ok {
			return fmt.Errorf("root id is empty!")
		}
		if _, ok := tm.Headers["task"]; !ok {
			return fmt.Errorf("task name is empty!")
		}
	}
	return nil
}

// Translate translates task message to runnable task
// based on given message protocol version
func (tm *TaskMessage) Translate(msgProtocolVer int) (*Task, error) {
	return &Task{MsgProtocolVer: msgProtocolVer}, nil
	// if msgProtocolVer == 1 {
	// 	mb, err := v1.Decode(tm.Body)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	return &Task{
	// 		ID:      mb.ID,
	// 		Name:    mb.Task,
	// 		Args:    mb.Args,
	// 		Kwargs:  mb.Kwargs,
	// 		Retries: mb.Retries,
	// 	}, nil
	// }
	// mb, err := v2.Decode(tm.Body)
	// if err != nil {
	// 	return nil, err
	// }
	// return &Task{
	// 	ID:      tm.Headers["id"].(string),
	// 	Name:    tm.Headers["task"].(string),
	// 	Args:    mb.Args,
	// 	Kwargs:  mb.Kwargs,
	// 	Retries: tm.Headers["retries"].(int),
	// }, nil
}

// Task is runnable interface
type Task struct {
	MsgProtocolVer int
	// GetCallbacks []string  // leave for future support
}

func (t *Task) checkVersion() error {
	if t.MsgProtocolVer != 1 && t.MsgProtocolVer != 2 {
		return fmt.Errorf("invalid message protocol version: %d", t.MsgProtocolVer)
	}
	return nil
}

// func (t *Task) GetID() (string,  {}
// func (t *Task) GetName() string                   {}
// func (t *Task) GetArgs() []interface{}            {}
// func (t *Task) GetKwargs() map[string]interface{} {}
// func (t *Task) GetRetries() int {}
