package main

import (
	"bytes"
	"flag"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var queue string

func init() {
	flag.StringVar(&queue, "qs", "info_queue", "provide your queue name")
}

func main() {
	flag.Parse()
	rabbit := NewRabbit("amqp://admin:admin@localhost:5672/test")
	rabbit.Consume(queue, doWork)
}

func doWork(msg amqp.Delivery) error {
	log.Printf("get message: %s", msg.Body)
	if bytes.Contains(msg.Body, []byte("error")) {
		msg.Ack(false)
	}
	if bytes.Contains(msg.Body, []byte("wait")) {
		dotCount := bytes.Count(msg.Body, []byte("."))
		time.Sleep(time.Duration(dotCount) * time.Second)
		msg.Ack(true)
	}
	msg.Ack(true)
	return nil
}
