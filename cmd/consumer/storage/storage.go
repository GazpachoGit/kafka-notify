package storage

import (
	"errors"
	"kafka-notify/pkg/models"
	"sync"
)

var ErrEmpty = errors.New("no messages found")

type Storage struct {
	data *sync.Map
	size int
}

func NewStorage(s int) *Storage {
	return &Storage{new(sync.Map), s}
}

func (s *Storage) get(clientID string) chan models.Notification {
	c, _ := s.data.LoadOrStore(clientID, make(chan models.Notification, s.size))
	return c.(chan models.Notification)
}

func (s *Storage) Push(clientID string, message models.Notification) {
	c := s.get(clientID)
	if len(c) == s.size {
		<-c
	}
	c <- message
}

func (s *Storage) Pop(clientID string) (models.Notification, error) {
	c := s.get(clientID)
	select {
	case item := <-c:
		return item, nil
	default:
		return models.Notification{}, ErrEmpty
	}
}

func (s *Storage) PopAll(clientID string) ([]models.Notification, error) {
	c := s.get(clientID)
	length := len(c)
	if length > 0 {
		out := make([]models.Notification, 0, length)
		for i := 0; i < length; i++ {
			out = append(out, <-c)
		}
		return out, nil
	}
	return nil, ErrEmpty
}
