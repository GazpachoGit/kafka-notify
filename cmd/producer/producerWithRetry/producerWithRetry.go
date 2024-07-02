package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

var ErrUserNotFoundInProducer = errors.New("user not found")

type ProducerWithRetry struct {
	producer        sarama.SyncProducer
	maxRetries      int
	deadLetterQueue []string
}

func (p *ProducerWithRetry) sendMessage(message string) {
	var notification = &models.Notification{}
	err := json.Unmarshal([]byte(message), notification)
	if err != nil {
		p.sendToDLQ(message, err)
		return
	}
	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(notification.To)),
		Value: sarama.StringEncoder(message),
	}
	for i := 0; i < p.maxRetries; i++ {
		_, _, err = p.producer.SendMessage(msg)
		if err == nil {
			break
		}
	}
	if err != nil {
		p.sendToDLQ(message, err)
	}
}

func (p *ProducerWithRetry) sendToDLQ(message string, err error) {
	p.deadLetterQueue = append(p.deadLetterQueue, message)
	fmt.Println("new message in the Deal letter queue. Reason: ", err)
}

func SetupProducer() (ProducerWithRetry, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	coreProducer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},
		config)
	if err != nil {
		return ProducerWithRetry{}, fmt.Errorf("failed to setup producer: %w", err)
	}
	producerWithRetry := ProducerWithRetry{
		producer:        coreProducer,
		maxRetries:      3,
		deadLetterQueue: make([]string, 0, 10),
	}
	return producerWithRetry, nil
}

func main() {

	producer, err := SetupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.producer.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Producer")
	fmt.Println("---------------------")

	for {
		fmt.Print("Enter JSON notification -> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		message := strings.Replace(text, "\n", "", -1)

		producer.sendMessage(message)
	}
}
