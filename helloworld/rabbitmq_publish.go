package main

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
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
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := "Hello World!"
	data, err := json.Marshal(body)
	err = channel.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data, //[]byte(body),
		})
	failOnError(err, "Failed to publish a message")
}
