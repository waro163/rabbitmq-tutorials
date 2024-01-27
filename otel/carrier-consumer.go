package main

import (
	"strings"

	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel/propagation"
)

type MessageCarrier struct {
	msg amqp.Delivery
}

var _ propagation.TextMapCarrier = (*MessageCarrier)(nil)

func (mc *MessageCarrier) Get(key string) string {
	for k, values := range mc.msg.Headers {
		if strings.EqualFold(key, k) {
			return values.(string)
		}
	}
	return ""
}

func (mc *MessageCarrier) Set(key string, value string) {
	mc.msg.Headers[key] = value
}

func (mc *MessageCarrier) Keys() []string {
	var keys []string
	for key := range mc.msg.Headers {
		keys = append(keys, key)
	}
	return keys
}
