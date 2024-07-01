package signal

import (
	"errors"
	"sync"
)

var ErrEmpty = errors.New("no receivers found")

type topic struct {
	subscribers []chan struct{}
	mu          *sync.Mutex
}

type Signal struct {
	topics  *sync.Map
	counter int
}

func NewSignal() *Signal {
	return &Signal{new(sync.Map), 0}
}

func (s *Signal) Subscribe(clientID string) (<-chan struct{}, func(), error) {
	t, _ := s.topics.LoadOrStore(clientID, &topic{mu: new(sync.Mutex)})
	s.counter++
	topic := t.(*topic)
	topic.mu.Lock()
	defer topic.mu.Unlock()
	c := make(chan struct{})
	topic.subscribers = make([]chan struct{}, 0, 5)
	topic.subscribers = append(topic.subscribers, c)
	return c, func() {
		topic.mu.Lock()
		defer topic.mu.Unlock()
		for i := 0; i < len(topic.subscribers); i++ {
			if topic.subscribers[i] == c {
				topic.subscribers = append(topic.subscribers[:i], topic.subscribers[i+1:]...)
			}
		}
	}, nil
}

func (s *Signal) Publish(clientID string) error {
	t, ok := s.topics.Load(clientID)
	if !ok {
		return ErrEmpty
	}
	//fmt.Println("topic found")
	topic := t.(*topic)
	for _, c := range topic.subscribers {
		c <- struct{}{}
	}
	return nil
}
