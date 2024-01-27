package main

import (
	"context"
	"fmt"
	"log"
)

const (
	queueName  = "my-queue"
	exchange   = "my-exchange"
	routingKey = "my-routing-key"
)

func main() {
	// init otel provider
	tp, err := InitTracer("amqp-consumer", TracingConfig{
		CollectorHost: "localhost:4317",
		SamplingRate:  1,
	})
	if err != nil {
		log.Println("init otel trace_provider error", err)
		return
	}
	defer tp.Shutdown(context.Background())

	// new rabbit
	rabbit := NewRabbit("amqp://admin:admin@localhost:5672/test")
	// binding
	if err := rabbit.QueueBind(queueName, exchange, routingKey); err != nil {
		log.Println("queue bind error", err)
		return
	}
	// worker
	go rabbit.Consume(queueName, handleConsumerMsg)
	// input message to producer
	var input string
	for {
		fmt.Println("input your send message")
		fmt.Scanf("%s", &input)
		if err := rabbit.Producer(exchange, routingKey, input); err != nil {
			fmt.Println("produce message error ", err)
			continue
		}
		fmt.Println("produce message success")
	}
}
