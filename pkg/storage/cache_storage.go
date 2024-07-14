package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"time"

	"github.com/go-redis/redis"
)

const (
	RedisPassword  = "predis"
	RedisAddress   = "localhost:6379"
	ExpirationTime = 20 * time.Second
)

type Redis struct {
	redisClient *redis.Client
}

var ErrCacheNotFound = errors.New("cached message not found")

func NewRedis() (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     RedisAddress,
		Password: RedisPassword,
	})
	pong, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	fmt.Println("Connected to Redis:", pong)
	return &Redis{client}, nil
}

func (r *Redis) SetCache(key string, rawValue *models.Notification) error {
	value, err := json.Marshal(rawValue)
	if err != nil {
		return err
	}
	err = r.redisClient.Set(key, value, ExpirationTime).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) GetCache(key string) (*models.Notification, error) {
	value, err := r.redisClient.Get(key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheNotFound
		}
		return nil, err
	}

	var m = &models.Notification{}
	err = json.Unmarshal([]byte(value), m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (r *Redis) Close() {
	r.redisClient.Close()
}
