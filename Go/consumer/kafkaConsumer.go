package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type myRecord struct {
	key string
	value string
}

/*
In this example, we set up a consumer that consumes messages on a single thread (to assure total ordering of consumption)
After consumption, we send the information to doSomething, which further processes our message into something usable.
Message processing should be done asynchronously to not block the main consumption thread. Golang makes it very easy to
do this with channels and goroutines. You can maintain a main consumer and then send over messages to another thread
for processing (which may or may not send to another thread according to partition, to maintain total ordering per
partition). This example does not care about total ordering per partition, so it discards that info and just builds
an in memory count of values seen.
*/
var topicForConsumption = "goTopic"

var internalLog = map[string]int64{}

func getConsumer (configs *kafka.ConfigMap) (*kafka.Consumer) {
	// Instantiate a new consumer
	myConsumer, err := kafka.NewConsumer(configs)
	if err != nil {
		// Catch errors while initializing
		fmt.Println(err)
		panic ("Error in creating producer")
	}

	return myConsumer
}

func main() {
	// Configurations for our consumer
	configs := kafka.ConfigMap{
		"bootstrap.servers" : "<ccloud-bootstrap-servers>",
		"security.protocol" : "SASL_SSL",
		"sasl.mechanism" : "PLAIN",
		"sasl.username" : "<ccloud-api-key>",
		"sasl.password" : "<ccloud-api-secret>",
		"ssl.ca.location" : "probe",
		"group.id":          "aGroupOfConsumers",
		"auto.offset.reset": "earliest",
	}

	myConsumer := getConsumer(&configs)
	// Subscribe to a topic
	_ = myConsumer.Subscribe(topicForConsumption, nil)

	// Create a channel where we can send records to for further processing
	deliveredRecords := make(chan myRecord)
	// Start goroutine to process any received messages
	go doSomething(deliveredRecords)

	go func() { //Background thread to read our messages
		for {
			singleMessage , err := myConsumer.ReadMessage(-1)
			if err != nil {
				log.Printf("Error Consuming! : %v", err)
			} else {
				fmt.Printf("Message from %s and partition %d with offset %d: { key: %s, Value: %s }\n",
					singleMessage.TopicPartition,
					singleMessage.TopicPartition.Partition,
					singleMessage.TopicPartition.Offset,
					string(singleMessage.Key),
					string(singleMessage.Value))
			}

			// Build an internal record with info we care about for further processing
			tempRecord := myRecord{
				key:   string(singleMessage.Key),
				value: string(singleMessage.Value),
			}
			// Further processing can and should be handled asynchronously
			// Here, we send the required info into a channel where we have previously
			// declared a goroutine to listen on
			deliveredRecords <- tempRecord
		}
	}()

	// Wait to build some state
	time.Sleep(time.Duration(5) * time.Second)

	// Close the channel to ensure an end of processing for doSomething async function
	close(deliveredRecords)
	fmt.Printf("Final Internal Memory: %v\n", internalLog)
}

// Example of "further processing"
// Here, we just take the record value and count how many times we've seen it before
// Nothing fancy, just to illustrate processing should happen asynchronously to consumption
// Make sure you keep order of messages per partition (if that matters to you) by passing the partition/offset of the message
// while processing. Since we only have one thread consuming, we can assume total ordering of messages per partition.
func doSomething (deliveredRecords <- chan myRecord) {
	for record := range deliveredRecords {
		currentValue, exists := internalLog[record.value]
		if exists {
			internalLog[record.value] = currentValue + 1
		} else {
			internalLog[record.value] = 1
		}
	}
}
