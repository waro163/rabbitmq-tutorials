package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var (
	exchangeName string = "logs_topic"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	amqpURI := "amqp://admin:admin@localhost:5672/test"
	connection, err := amqp.Dial(amqpURI)
	failOnError(err, "dial amqp error")
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	failOnError(err, "open channel error")
	defer channel.Close()

	err = channel.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	failOnError(err, "declare exchange error")

	var routingKey, msg string
	for {
		fmt.Println("输入routing key:")
		_, err = fmt.Scanf("%s", &routingKey)
		if err != nil {
			log.Println("input key error ", err)
			continue
		}
		fmt.Println("输入发送的消息:")
		_, err = fmt.Scanf("%s", &msg)
		if err != nil {
			log.Println("input body error ", err)
			continue
		}
		body, err := json.Marshal(msg)
		if err != nil {
			log.Println("marshal msg error", err)
			continue
		}
		err = channel.Publish(
			exchangeName, // exchange
			routingKey,   // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         body,
			},
		)
		if err != nil {
			log.Println("Failed to publish a message", err)
			continue
		}
		log.Println("send successfully")
	}
}
