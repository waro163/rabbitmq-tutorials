package main

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type Rabbit struct {
	url          string
	connection   *amqp.Connection
	errorConn    chan *amqp.Error
	channel      *amqp.Channel
	errorChannel chan *amqp.Error
}

func NewRabbit(url string) *Rabbit {
	return &Rabbit{
		url: url,
	}
}

func (r *Rabbit) connect() error {
	if r.url == "" {
		return errors.New("empty connection url")
	}
	if r.connection != nil && !r.connection.IsClosed() {
		return nil
	}
	log.Println("connect: begin dial")
	connection, err := amqp.Dial(r.url)
	// connection, err := amqp.DialConfig(r.url, amqp.Config{
	// 	Vhost: r.vhost,
	// 	Properties: amqp.Table{
	// 		"connection_name": "turorials",
	// 	},
	// })
	if err != nil {
		return err
	}
	log.Println("connect: dial succ")
	r.errorConn = make(chan *amqp.Error)
	r.connection = connection
	connection.NotifyClose(r.errorConn)
	return nil
}

func (r *Rabbit) getChannel() error {
	if r.connection == nil {
		if err := r.connect(); err != nil {
			return err
		}
	}
	if r.channel != nil {
		return nil
	}
	log.Println("get channel: begin get")
	channel, err := r.connection.Channel()
	if err != nil {
		return err
	}

	if err = channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return err
	}
	log.Println("get channel: get succ")
	r.errorChannel = make(chan *amqp.Error)
	r.channel = channel
	channel.NotifyClose(r.errorChannel)
	return nil
}

func (r *Rabbit) shutDown() {
	r.channel.Close()
	r.connection.Close()
}

func (r *Rabbit) consumeMsg(queueName string, fn func(amqp.Delivery) error) error {
	_, err := r.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return err
	}
	msgs, err := r.channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return err
	}
	for {
		select {
		case <-r.errorConn:
			r.connection = nil
			log.Println("connection closed")
			return errors.New("connection close")
		case <-r.errorChannel:
			r.channel = nil
			log.Println("channel closed")
			return errors.New("channel close")
		case msg := <-msgs:
			go fn(msg)
		}
	}
	return nil
}

func (r *Rabbit) Consume(queueName string, fn func(amqp.Delivery) error) error {
	for {
		log.Println("begin connect")
		if err := r.connect(); err != nil {
			log.Println("connect error", err)
			time.Sleep(time.Second)
			continue
		}
		log.Println("begin get channel")
		if err := r.getChannel(); err != nil {
			log.Println("get channel error", err)
			time.Sleep(time.Second)
			continue
		}
		r.consumeMsg(queueName, fn)
	}
}

func (r *Rabbit) Producer(exchangeName, routingKey string, msg interface{}) error {
	for i := 0; i < 30; i++ {
		if err := r.connect(); err != nil {
			log.Printf("%dth reconnect failed, %s\n", i, err)
			time.Sleep(time.Second)
			continue
		}
		if err := r.getChannel(); err != nil {
			log.Printf("%dth get channel failed, %s\n", i, err)
			continue
		}
		if err := r.channel.ExchangeDeclare(
			exchangeName, // name
			"topic",      // type
			true,         // durable
			false,        // auto-deleted
			false,        // internal
			false,        // noWait
			nil,          // arguments
		); err != nil {
			log.Println("declear exchange error: ", err)
			return err
		}
		body, err := json.Marshal(msg)
		if err != nil {
			log.Println("marshal msg error", err)
			return err
		}
		if err := r.channel.Publish(
			exchangeName, // exchange
			routingKey,   // routing key
			false,        // mandatory
			false,        // immediate
			amqp.Publishing{
				Headers:      amqp.Table{"app": "go-client"},
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/json",
				Body:         body,
			},
		); err != nil {
			log.Println("publish message error: ", err, ", will reconnect again")
			r.connection = nil
			r.channel = nil
			continue
		}
		return nil
	}
	// all retry failed
	log.Println("all retry failed, please check error log")
	return errors.New("publish message failed")
}
