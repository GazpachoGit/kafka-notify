package storage

import (
	"context"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"sync"
)

var ErrEmpty = errors.New("no messages found")

type Storage struct {
	data      *sync.Map
	size      int
	dbChanel  chan models.Notification
	dbStorage *PgDB
}

func NewStorage(s int, dbQueueSize int, dbConnStr string) (*Storage, error) {
	dbStorage, err := InitDB(context.Background(), dbConnStr)
	if err != nil {
		return nil, err
	}
	dbChanel := make(chan models.Notification, dbQueueSize)
	go func() {
		buff := make([]models.Notification, 0, dbQueueSize)
		for v := range dbChanel {
			buff = append(buff, v)
			if len(buff) == dbQueueSize {
				_, err := dbStorage.InsertMessages(buff)
				if err != nil {
					fmt.Println(err)
				}
				buff = buff[:0]
			}
		}
	}()
	return &Storage{
		data:      new(sync.Map),
		size:      s,
		dbChanel:  dbChanel,
		dbStorage: dbStorage,
	}, nil
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
	s.dbChanel <- message
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

func (s *Storage) Close() {
	s.dbStorage.Close()
	close(s.dbChanel)
}
