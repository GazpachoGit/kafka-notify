package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"kafka-notify/pkg/models"
	"kafka-notify/pkg/storage"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type bodyLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w bodyLogWriter) Write(b []byte) (int, error) {
	w.body.Write(b)
	return w.ResponseWriter.Write(b)
}

func CacheMiddlewareHandler(cache *storage.Redis, URLParam string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		messageIDStr := ctx.Param(URLParam)
		if messageIDStr == "" {
			ctx.JSON(http.StatusNotFound, gin.H{"message": "object not found in the database"})
			ctx.Abort()
		}
		v, err := cache.GetCache(messageIDStr)

		if err != nil {
			if !errors.Is(err, storage.ErrCacheNotFound) {
				log.Println("Error. fail to Get cache: ", err)
			}
			//not found in cache
			ctx.Header("Cache-Status", "MISS")
			blw := &bodyLogWriter{body: bytes.NewBufferString(""), ResponseWriter: ctx.Writer}
			ctx.Writer = blw
			ctx.Next()
			statusCode := ctx.Writer.Status()
			if statusCode == http.StatusOK {
				body := blw.body
				note := &models.Notification{}
				//for debug
				noteStr := body.String()
				err = json.Unmarshal([]byte(noteStr), note)
				if err != nil {
					log.Println("Error. fail to Unmarshal response to the Notification type: ", err)
					return
				}
				err = cache.SetCache(messageIDStr, note)
				if err != nil {
					log.Println("Error. fail to Set cache: ", err)
				}
			}
		} else {
			//found in cache
			ctx.Header("Cache-Status", "HIT")
			ctx.JSON(http.StatusOK, *v)
			ctx.Abort()
		}
	}
}
