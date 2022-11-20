package main

import (
	"bytes"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

var (
	queues       string
	routingKeys  string
	exchangeName string = "logs_topic"
)

func init() {
	flag.StringVar(&queues, "qs", "info_queue", "provide your queue, split by ','")
	flag.StringVar(&routingKeys, "rk", "info", "provide your routing key, split by ','")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func convertStr2List(str string) []string {
	return strings.Split(str, ",")
}

func run() {
	flag.Parse()
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

	queueInf := convertStr2List(queues)

	// declare all queue
	for _, queueName := range queueInf {
		_, err := channel.QueueDeclare(
			queueName, // name
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		failOnError(err, "Failed to declare a queue"+queueName)
	}

	routingKeyInf := convertStr2List(routingKeys)

	// bind each routing key to all queue
	for _, queueName := range queueInf {
		for _, routingKey := range routingKeyInf {
			err = channel.QueueBind(
				queueName,    // name of the queue
				routingKey,   // bindingKey
				exchangeName, // sourceExchange
				false,        // noWait
				nil,          // arguments
			)
			failOnError(err, "Failed to bind key "+routingKey+" to queue "+queueName)
		}
	}

	// fair dispatch for worker
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	for _, queueName := range queueInf {
		msgs, err := channel.Consume(
			queueName, // queue
			"",        // consumer
			false,     // auto-ack
			false,     // exclusive
			false,     // no-local
			false,     // no-wait
			nil,       // args
		)
		failOnError(err, "Failed to register a consumer for queue: "+queueName)
		go doWork(queueName, msgs)
	}
	exit := make(chan struct{})
	<-exit
}

func doWork(queueName string, msgs <-chan amqp.Delivery) {
	log.Println(queueName + " begin to consume")
	for msg := range msgs {
		log.Printf("queue name: %s, msg: %s", queueName, msg.Body)

		if bytes.Contains(msg.Body, []byte("error")) {
			msg.Ack(false)
			continue
		}
		if bytes.Contains(msg.Body, []byte("wait")) {
			dotCount := bytes.Count(msg.Body, []byte("."))
			time.Sleep(time.Duration(dotCount) * time.Second)
			msg.Ack(true)
			continue
		}
		msg.Ack(true)
	}
	log.Println(queueName + " had been exit")
}

func main() {
	run()
}
