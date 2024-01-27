package main

import (
	"strings"

	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel/propagation"
)

type MessagePCarrier struct {
	msg amqp.Publishing
}

var _ propagation.TextMapCarrier = (*MessagePCarrier)(nil)

func (mc *MessagePCarrier) Get(key string) string {
	for k, values := range mc.msg.Headers {
		if strings.EqualFold(key, k) {
			return values.(string)
		}
	}
	return ""
}

func (mc *MessagePCarrier) Set(key string, value string) {
	mc.msg.Headers[key] = value
}

func (mc *MessagePCarrier) Keys() []string {
	var keys []string
	for key := range mc.msg.Headers {
		keys = append(keys, key)
	}
	return keys
}
