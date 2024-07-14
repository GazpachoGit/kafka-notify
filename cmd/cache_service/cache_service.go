package main

import (
	"context"
	"fmt"
	middleware "kafka-notify/pkg/middleware/cache_middleware"
	"kafka-notify/pkg/models"
	"kafka-notify/pkg/storage"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

const (
	Port      = ":8080"
	DBConnStr = "postgres://puser:ppassword@localhost:6432/notifyDB?sslmode=disable"
)

func handleGetNotification(dbStorage *storage.PgDB) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		messageIDStr := ctx.Param("userID")
		messageID, err := strconv.Atoi(messageIDStr)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "incorrect notification id format"})
			return
		}
		v, err := dbStorage.GetMessage(messageID)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			return
		}
		//found in cache
		ctx.JSON(http.StatusOK, *v)
	}
}

func handleSetNotification(cache *storage.Redis, dbStorage *storage.PgDB) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		var newNote models.Notification
		if err := ctx.ShouldBindJSON(&newNote); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": "incorrect body format"})
			return
		}
		ids, err := dbStorage.InsertMessages([]models.Notification{newNote})
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
			return
		}
		id := ids[0]
		err = cache.SetCache(strconv.Itoa(id), &newNote)
		if err != nil {
			log.Println("Error. fail to Set cache: ", err)
		}
		ctx.JSON(http.StatusOK, gin.H{"id": id})
	}
}

func main() {
	cache, err := storage.NewRedis()
	if err != nil {
		fmt.Println("Redis connection fail: ", err)
		return
	}
	defer cache.Close()
	dbStorage, err := storage.InitDB(context.Background(), DBConnStr)
	if err != nil {
		fmt.Println("DB connection fail: ", err)
		return
	}
	defer dbStorage.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	router.GET("/notifications/:userID", middleware.CacheMiddlewareHandler(cache), handleGetNotification(dbStorage))
	router.POST("/notifications", handleSetNotification(cache, dbStorage))

	fmt.Println("Cached service started")

	if err := router.Run(Port); err != nil {
		fmt.Printf("failed to run the server: %v", err)
	}
}
