package main

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 3) || os.Args[2] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[2:], " ")
	}
	return s
}

func severityFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "info"
	} else {
		s = os.Args[1]
	}
	return s
}

func main() {
	amqpURI := "amqp://admin:admin@localhost:5672/"
	connection, err := amqp.Dial(amqpURI)
	failOnError(err, "dial amqp error")
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	failOnError(err, "open channel error")
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // noWait
		nil,           // arguments
	)
	failOnError(err, "declare exchange error")

	// q, err := channel.QueueDeclare(
	// 	"durable_hello", // name
	// 	true,            // durable
	// 	false,           // delete when unused
	// 	false,           // exclusive
	// 	false,           // no-wait
	// 	nil,             // arguments
	// )
	// failOnError(err, "Failed to declare a queue")

	body := bodyFrom(os.Args)
	data, err := json.Marshal(body)
	err = channel.Publish(
		"logs_direct",         // exchange
		severityFrom(os.Args), // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         data,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
