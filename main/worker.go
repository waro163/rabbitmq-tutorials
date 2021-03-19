package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/streadway/amqp"
)

type MessageBoby struct {
	Name   string                 `json:"name"`
	Args   []interface{}          `json:"args"`
	Kwargs map[string]interface{} `json:"kwargs"`
}

type f func(args []interface{}, kwargs map[string]interface{}) error

func testparam(args []interface{}, kwargs map[string]interface{}) error {
	log.Printf("--------entry into function--------\n")
	log.Printf("args %v\n", args)
	log.Printf("kwargs %v\n", kwargs)
	return nil
}

var funcMap = map[string]f{
	"test7": testparam,
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func setParentOptions(taskid interface{}, taskname string) {
	parentOptions := struct {
		ID   interface{} `json:"id"`
		Code string      `json:"code"`
	}{
		ID:   taskid,
		Code: taskname,
	}
	data, err := json.Marshal(parentOptions)
	if err != nil {
		log.Printf("set parent options error %s\n", err)
	}
	os.Setenv("SPARROW_TASK_PARENT_OPTIONS", string(data))
}

func getTaskInfo(headers amqp.Table, body MessageBoby) string {
	deliveryInfo := headers["delivery_info"].(map[string]interface{})
	parentOptions := headers["parent_options"].(map[string]interface{})
	taskInfo := struct {
		ID          interface{}            `json:"id"`
		Name        string                 `json:"name"`
		TaskArgs    []interface{}          `json:"task_args"`
		TaskKwargs  map[string]interface{} `json:"task_kwargs"`
		Origin      interface{}            `json:"origin"`
		CreatedTime interface{}            `json:"created_time"`
		Exchange    interface{}            `json:"exchange"`
		RoutingKey  interface{}            `json:"routing_key"`
		IsSent      bool                   `json:"is_sent"`
		ParentID    interface{}            `json:"parent_id"`
		ParentCode  interface{}            `json:"parent_code"`
	}{
		ID:          headers["task_id"],
		Name:        body.Name,
		TaskArgs:    body.Args,
		TaskKwargs:  body.Kwargs,
		Origin:      headers["origin"],
		CreatedTime: headers["created_time"],
		Exchange:    deliveryInfo["exchange"],
		IsSent:      true,
		ParentID:    parentOptions["id"],
		ParentCode:  parentOptions["code"],
	}
	data, err := json.Marshal(taskInfo)
	if err != nil {
		log.Printf("marshal taskInfo error %s\n", err)
	}
	return string(data)
}

func updateTaskResult(taskid interface{}, queueName string, status string, result string, traceback string, taskInfo string) {
	data := struct {
		TaskID    interface{} `json:"task_id"`
		Consumer  string      `json:"consumer"`
		Status    string      `json:"status"`
		Result    string      `json:"result"`
		TraceBack string      `json:"traceback"`
		TaskInfo  string      `json:"task_info"`
	}{
		TaskID:    taskid,
		Consumer:  queueName,
		Status:    status,
		Result:    result,
		TraceBack: traceback,
		TaskInfo:  taskInfo,
	}
	log.Printf("request data %v\n", data)
}

func run(queueName string) {
	// amqpURI := "amqp://hg_test:jft87JheHe23@localhost:8888/sparrow_test"
	// amqpURI := "amqp://hg_test:jft87JheHe23@192.168.43.72:5672/sparrow_test"
	amqpURI := "amqp://hg_test:jft87JheHe23@39.103.7.185:5672/sparrow_test"
	connection, err := amqp.Dial(amqpURI)
	failOnError(err, "dial amqp error")
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	failOnError(err, "open channel error")
	defer channel.Close()

	// err = channel.ExchangeDeclare(
	// 	"logs_topic", // name
	// 	"topic",      // type
	// 	true,         // durable
	// 	false,        // auto-deleted
	// 	false,        // internal
	// 	false,        // noWait
	// 	nil,          // arguments
	// )
	// failOnError(err, "declare exchange error")

	q, err := channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// for _, key := range os.Args[1:] {
	// 	err = channel.QueueBind(
	// 		q.Name,       // name of the queue
	// 		key,          // bindingKey
	// 		"logs_topic", // sourceExchange
	// 		false,        // noWait
	// 		nil,          // arguments
	// 	)
	// 	failOnError(err, "Failed to bind key to queue")
	// }

	// fair dispatch for worker
	err = channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
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
		// logout all data
		log.Printf("reveive raw msg: %v\n", d)
		log.Printf("header is %v\n", d.Headers)

		data, err := base64.StdEncoding.DecodeString(string(d.Body))
		if err != nil {
			log.Printf("base64 decode body error: %s", err)
			continue
		}
		log.Printf("data is %v\n", string(data))

		// logout info for search
		log.Printf("Delivery tag: %d Message body: %s Message Header: %v Task_id: %v\n", d.DeliveryTag, data, d.Headers, d.Headers["task_id"])

		var messageBody MessageBoby
		err = json.Unmarshal(data, &messageBody)
		if err != nil {
			log.Printf("unmarshal message body error: %s\n", err)
			continue
		}
		log.Printf("message body %v\n", messageBody)

		setParentOptions(d.Headers["task_id"], messageBody.Name)

		// execute message code func
		funcDo, ok := funcMap[messageBody.Name]
		if !ok {
			log.Printf("not found '%s' function\n", messageBody.Name)
			continue
		}
		err = funcDo(messageBody.Args, messageBody.Kwargs)
		status := "SUCCESS"
		var traceback string
		if err != nil {
			status = "FAILURE"
			traceback = err.Error()
			log.Printf("exec %s function error\n", messageBody.Name)
		}
		taskInfo := getTaskInfo(d.Headers, messageBody)
		updateTaskResult(d.Headers["task_id"], queueName, status, "", traceback, taskInfo)
		// d.Ack(false)
	}
}

func main() {
	run("test9")
	fmt.Printf("press CTRL+C to exit\n")
}
