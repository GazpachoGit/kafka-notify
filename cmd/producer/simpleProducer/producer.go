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

func findUserByID(id int, users []models.User) (models.User, error) {
	for _, u := range users {
		if u.ID == id {
			return u, nil
		}
	}
	return models.User{}, ErrUserNotFoundInProducer
}

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, notification *models.Notification) error {
	toUser, err := findUserByID(notification.To, users)
	if err != nil {
		return err
	}
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON),
	}
	_, _, err = producer.SendMessage(msg)
	return err
}

func sendMessageHandler2(producer sarama.SyncProducer, message string, users []models.User) error {
	var notification = &models.Notification{}
	err := json.Unmarshal([]byte(message), notification)
	if err != nil {
		return err
	}
	err = sendKafkaMessage(producer, users, notification)
	if err != nil {
		return err
	}
	return nil
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},
		config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Joka"},
		{ID: 2, Name: "Chook"},
		{ID: 3, Name: "Biba"},
		{ID: 4, Name: "Loopa"},
	}

	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Producer")
	fmt.Println("---------------------")

	for {
		fmt.Print("Enter JSON notification -> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		message := strings.Replace(text, "\n", "", -1)

		err := sendMessageHandler2(producer, message, users)
		if err != nil {
			fmt.Println("Error: ", err)
		}
	}
}
