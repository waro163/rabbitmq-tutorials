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
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
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

	q, err := channel.QueueDeclare(
		"durable_hello", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// err = channel.Publish(
	// 	"",     // exchange
	// 	q.Name, // routing key
	// 	false,  // mandatory
	// 	false,  // immediate
	// 	amqp.Publishing{
	// 		ContentType: "application/json",
	// 		Body:        data, //[]byte(body),
	// 	})
	// failOnError(err, "Failed to publish a message")
	body := bodyFrom(os.Args)
	data, err := json.Marshal(body)
	err = channel.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         data,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
