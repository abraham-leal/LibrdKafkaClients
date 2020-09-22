package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

/*
In this example, we set up a producer that listens on a channel for records to send to Kafka
This channel only accepts a record with a key and value that are made of strings, but of course, you could make it accept
any type of data structure and handle it. This example uses myRecord as the enforcer of types for our record, it uses
getProducer as the producer factory, it uses startProduction as the goroutine listening for messages in the channel, and
it uses reportHandler as the goroutine listening for broker responses to handle them the right way.

This producer uses triple queueing:
1. Queue messages for proper transformation into a kafka message
2. Queue messages for backpressure into the internal aggregator
3. Queue in the aggregator before send
 */
var topicForProduction = "goTopic"

type myRecord struct {
	key string
	value string
}

func getProducer (configs *kafka.ConfigMap) (*kafka.Producer) {
	// Instantiate a new producer
	myProducer, err := kafka.NewProducer(configs)
	if err != nil {
		// Catch errors while initializing
		fmt.Println(err)
		panic ("Error in creating producer")
	}

	return myProducer
}

func main() {
	// Configurations for our producer
	configs := kafka.ConfigMap{
		"bootstrap.servers" : "<ccloud-bootstrap-servers>",
		"security.protocol" : "SASL_SSL",
		"sasl.mechanism" : "PLAIN",
		"sasl.username" : "<ccloud-api-key>",
		"sasl.password" : "<ccloud-api-secret>",
		"ssl.ca.location" : "probe",
	}

	myProducer := getProducer(&configs)
	// Make sure to close the producer when processing is done
	defer myProducer.Close()
	// Async report handler
	go reportHandler(myProducer)
	// Start channel to send records to asynchronously
	recordChannel := make(chan myRecord)
	// Start our producer in the background, we can now send it records through the channel for production
	go startProduction(recordChannel, myProducer)

	recordsIWantToSend := []myRecord{
		{"", "hello"},
		{"","my"},
		{"","name"},
		{"", "is", },
		{"", "Carl"},
	}

	//Send all records
	for _, record := range recordsIWantToSend {
		recordChannel <- record
	}

	// Assure the producer flushes before we exit
	myProducer.Flush(5000)
	close(recordChannel)
}

func startProduction (productionBacklog <- chan myRecord, myProducer *kafka.Producer) {
	for msg := range productionBacklog {
		// Set headers for metadata of records
		headerForMessages :=[]kafka.Header{
			{
				Key:   "Sent By", // Set key
				Value: []byte("Abraham"), // Set value as byte array
			},
			{
				Key:   "Why", // Set key
				Value: []byte("For the lolz"), // Set value as byte array
			},
		}

		//Create a Kafka Message, with NO key
		myMessage := kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicForProduction,
				Partition: kafka.PartitionAny,
			},
			Key: 			[]byte(msg.key),
			Value:          []byte(msg.value), // Turn your message into a byte array
			Headers:        headerForMessages,
		}

		log.Printf("Producing message %s to topic %s", msg, topicForProduction)
		//Send message to the queue
		myProducer.ProduceChannel() <- &myMessage
	}
}

func reportHandler (myProducer *kafka.Producer) {
	// For every event in the Events() channel
	for e := range myProducer.Events() {
		// Switch the types (enter them for consideration)
		switch ev := e.(type) {
		// If the type of the event is a reference to a kafka message
		case *kafka.Message:
			// And it contains an error: Log failure
			if ev.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", ev.TopicPartition)
			} else {
				// else: Log success
				log.Printf("Delivered message to %v", ev.TopicPartition)
			}
		}
	}
}
