# Test app(kafka, redis, postgres)

## Prerequisites

1. Go language package installed
2. Docker installed
3. From the project root directory in the prompt run command:

```
docker-compose --project-name="kafka-notify" up -d
```

4. Wait and check that all containers are up and running. You can use the command

```
docker container ls
```

## Cached service

### Description

The web service is running on the port :8082 and has 2 endpoints:
| Method | URL | Request Body | Response body |
| - | - | - | - |
| POST | localhost:8082/notifications | {"to": 1, "message":"New"} | {"id": 1} |
| GET | localhost:8082/notifications/1 | - | {"to": 1, "message":"New"} |

### Start

From the project root dir run command:

```
go run ./cmd/cache_service/cache_service.go
```

You can you the Postman for the test

## Producer

### Description

Producer is a console app which reads the JSON messages and sends them to the kafka.  
The message format example:

```
{"to": 2, "message":"New"}
```

## Consumer

### Description

Consumer is a console app which reads messages for a sertain user(depends on the user id). All consumed messages are stored the DB

## Start producer and consumer

1. From the project root dir run the command:

```
go run ./cmd/consumer/consumer.go
```

2. Input the user id (example 1) (the "to" field of the producer message).

```
Input userID -> 1
```

3. From the project root dir run the command:

```
go run ./cmd/producer/producer_with_retry/producer_with_retry.go
```

4. Send messages by the Producer

```
Enter JSON message -> {"to":1, "message":"blablabla"}
```

5. In the consumer windows you should get the message filed of the notification

```
New message:  blablabla
```

6. You can change the user id value by press the "Enter" key in the consumer window
