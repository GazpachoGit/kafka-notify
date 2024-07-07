package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"kafka-notify/pkg/signal"
	"kafka-notify/pkg/storage"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
	dbConnStr          = "postgres://puser:ppassword@localhost:6432/notifyDB?sslmode=disable"
)

var ErrNoMessagesFound = errors.New("no messages found")

type Consumer struct {
	storage *storage.Storage
	signal  *signal.Signal
}

func NewConsumer(storage *storage.Storage, signal *signal.Signal) *Consumer {
	return &Consumer{
		storage,
		signal,
	}
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		userID := string(msg.Key)
		var notification models.Notification
		err := json.Unmarshal(msg.Value, &notification)
		if err != nil {
			log.Printf("failed to unmarshal notification: %v", err)
			continue
		}
		consumer.storage.Push(userID, notification)
		err = consumer.signal.Publish(userID)
		if err != nil {
			fmt.Println(err)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context, storage *storage.Storage, signal *signal.Signal) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("initialization error: %v", err)
	}
	defer consumerGroup.Close()

	consumer := NewConsumer(storage, signal)

	for {
		err := consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
		if err != nil {
			log.Printf("error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func startDefaultListener(storage *storage.Storage, signal *signal.Signal) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Consumer")
	fmt.Println("---------------------")
	for {
		fmt.Print("Input userID -> ")
		text, _ := reader.ReadString('\n')
		userID := strings.Replace(text, "\n", "", -1)
		userID = strings.Replace(userID, "\r", "", -1)
		if len(userID) == 0 {
			fmt.Println("Invalid userID")
			continue
		}
		//get existing messages sent before subscription
		messageBacklog, err := storage.PopAll(userID)
		if err == nil {
			for _, m := range messageBacklog {
				fmt.Println("New message: ", m.Message)
			}
		}

		msgs, unsubscribe, err := signal.Subscribe(userID)
		if err != nil {
			fmt.Println("Error: ", err)
			continue
		}
		stopChanel := make(chan struct{})
		go stopHandler(stopChanel)
	L:
		for {
			select {
			case <-msgs:
				message, _ := storage.Pop(userID)
				fmt.Println("New message: ", message.Message)
			case <-stopChanel:
				unsubscribe()
				break L
			}
		}
	}
}

func stopHandler(ch chan struct{}) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		fmt.Println("Stop listen")
		ch <- struct{}{}
		break
	}
}

func main() {
	storage, err := storage.NewStorage(50, 3, dbConnStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer storage.Close()
	signal := signal.NewSignal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go setupConsumerGroup(ctx, storage, signal)
	startDefaultListener(storage, signal)

	// time.Sleep(5 * time.Second)
	// msgs, _, err := signal.Subscribe("1")
	// if err != nil {
	// 	fmt.Println("Error: ", err)
	// 	return
	// }
	// for range msgs {
	// 	message, _ := storage.Pop("1")
	// 	fmt.Println("New message: ", message.Message)
	// }
}
