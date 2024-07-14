package main

import (
	"context"
	"errors"
	"fmt"
	"kafka-notify/pkg/storage"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

const (
	dbConnStr = "postgres://puser:ppassword@localhost:6432/notifyDB?sslmode=disable"
)

func handleNotifications(cache *storage.Redis, dbStorage *storage.PgDB) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		messageIDStr := ctx.Param("UserID")
		messageID, err := strconv.Atoi(messageIDStr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "incorrect notification id format"})
			return
		}
		if messageIDStr == "" {
			ctx.JSON(http.StatusNotFound, gin.H{"message": "object not found in the database"})
			return
		}
		v, err := cache.GetCache(messageIDStr)
		if err != nil {
			if !errors.Is(err, storage.ErrCacheNotFound) {
				ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
				return
			}
			//not found in cache
			v, err = dbStorage.GetMessage(messageID)
			if err != nil {
				ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
				return
			}
			cache.SetCache(messageIDStr, v)
		}
		//found in cache
		ctx.JSON(http.StatusOK, gin.H{"notifications": v})
	}
}

func main() {
	cache, err := storage.NewRedis()
	if err != nil {
		fmt.Println("Redis connection fail: ", err)
		return
	}
	defer cache.Close()
	dbStorage, err := storage.InitDB(context.Background(), dbConnStr)
	if err != nil {
		fmt.Println("DB connection fail: ", err)
		return
	}
	defer dbStorage.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.GET("notifications/:userID", handleNotifications(cache, dbStorage))

	fmt.Printf("Cached service started")

	if err := router.Run("8080"); err != nil {
		fmt.Printf("failed to run the server: %v", err)
	}
}
