package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var (
	queue      string
	exchange   string
	routingKey string
)

// producer job
func init() {
	flag.StringVar(&exchange, "ex", "logs_topic", "prodide your exchange name")
	flag.StringVar(&routingKey, "rk", "test", "provide your routing key")
}
func main() {
	flag.Parse()
	rabbit := NewRabbit("amqp://admin:admin@localhost:5672/test")
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

// worker job
// func init() {
// 	flag.StringVar(&queue, "qs", "info_queue", "provide your queue name")
// }

// func main() {
// 	flag.Parse()
// 	rabbit := NewRabbit("amqp://admin:admin@localhost:5672/test")
// 	rabbit.Consume(queue, doWork)
// }

func doWork(msg amqp.Delivery) error {
	log.Printf("get message: %s", msg.Body)
	if bytes.Contains(msg.Body, []byte("error")) {
		msg.Ack(false)
		return nil
	}
	if bytes.Contains(msg.Body, []byte("wait")) {
		dotCount := bytes.Count(msg.Body, []byte("."))
		time.Sleep(time.Duration(dotCount) * time.Second)
		msg.Ack(true)
		return nil
	}
	msg.Ack(true)
	return nil
}
