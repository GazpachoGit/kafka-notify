package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"kafka-notify/pkg/models"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

const (
	RedisPassword  = "predis"
	RedisAddress   = "localhost:6379"
	ExpirationTime = 1 * time.Minute
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

func InitHotCache(cache *Redis, db *PgDB) error {
	dbSize, err := cache.redisClient.Do("DBSIZE").Int64()
	if dbSize > 0 {
		//cache already hot
		return nil
	}
	msgs, err := db.GetAllMessages()
	if err != nil {
		return err
	}
	arr := make([]interface{}, 0, 10)
	arr = append(arr, "MSET")
	for _, v := range msgs {
		value, err := json.Marshal(v.MapToNotification())
		if err != nil {
			return err
		}
		key := strconv.Itoa(v.ID)
		arr = append(arr, key, value)
		if len(arr) == cap(arr) {
			if err = cache.redisClient.Do(arr...).Err(); err != nil {
				return err
			}
			arr = arr[:1]
		}
	}
	if len(arr) > 1 {
		if err = cache.redisClient.Do(arr...).Err(); err != nil {
			return err
		}
	}
	return nil
}
