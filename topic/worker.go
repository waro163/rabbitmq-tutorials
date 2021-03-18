package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
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

func run() {
	amqpURI := "amqp://admin:admin@localhost:5672/"
	connection, err := amqp.Dial(amqpURI)
	failOnError(err, "dial amqp error")
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	failOnError(err, "open channel error")
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	failOnError(err, "declare exchange error")

	q, err := channel.QueueDeclare(
		"logs_topic_queue", // name
		false,              // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	if len(os.Args) < 2 {
		fmt.Printf("routing key should add to args\n")
		os.Exit(0)
	}
	for _, key := range os.Args[1:] {
		err = channel.QueueBind(
			q.Name,       // name of the queue
			key,          // bindingKey
			"logs_topic", // sourceExchange
			false,        // noWait
			nil,          // arguments
		)
		failOnError(err, "Failed to bind key to queue")
	}

	// fair dispatch for worker
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	// err = channel.QueueBind(
	// 	q.Name, // name of the queue
	// 	"",     // bindingKey
	// 	"logs", // sourceExchange
	// 	false,  // noWait
	// 	nil,    // arguments
	// )

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// forever := make(chan bool)

	// go func() {
	// 	for d := range msgs {
	// 		log.Printf("Done %s", d.Body)
	// 		dotCount := bytes.Count(d.Body, []byte("."))
	// 		t := time.Duration(dotCount)
	// 		time.Sleep(t * time.Second)
	// 		d.Ack(false)
	// 	}
	// }()

	// log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	// <-forever

	for d := range msgs {
		log.Printf("Done %s", d.Body)
		dotCount := bytes.Count(d.Body, []byte("."))
		t := time.Duration(dotCount)
		time.Sleep(t * time.Second)
		d.Ack(false)
	}
}

func main() {
	run()
	fmt.Printf("press CTRL+C to exit\n")
}
