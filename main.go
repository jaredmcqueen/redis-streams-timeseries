package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/redis-streams-timeseries/util"
)

var tsdbCounter int32

func init() {
	rand.Seed(time.Now().UnixNano())
}

func redisConsumer(batchChan chan<- []map[string]interface{}, endpoint string, startID string, batchSize int64) {
	rctx := context.Background()
	log.Println("connecting to redis endpoint", endpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})

	// test redis connection
	_, err := rdb.Ping(rctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}
	log.Println("connected")

	pit := startID
	for {
		trades, err := rdb.XRead(rctx, &redis.XReadArgs{
			Streams: []string{"trades", pit},
			Count:   batchSize,
			Block:   0,
		}).Result()
		if err != nil {
			log.Fatal("error XRead: ", err)
		}

		bigBatch := make([]map[string]interface{}, 0, batchSize)

		for _, stream := range trades {
			for _, message := range stream.Messages {
				bigBatch = append(bigBatch, message.Values)
				pit = message.ID
			}
		}
		batchChan <- bigBatch
	}
}

func timeseriesWriter(batchChan <-chan []map[string]interface{}, endpoint string) {
	rctx := context.Background()
	log.Println("connecting to redis endpoint", endpoint)
	rdb := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})

	// test redis connection
	_, err := rdb.Ping(rctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis", err)
	}
	log.Println("connected")

	for {
		select {
		case batch := <-batchChan:
			pipe := rdb.Pipeline()
			for _, v := range batch {
				tsdbCounter++
				pipe.Do(rctx,
					"TS.ADD",
					//key
					fmt.Sprintf("trades.%v.price", v["S"]),
					//ts
					"*",
					//value
					fmt.Sprintf("%v", v["p"]),
					"LABELS",
					"symbol", v["S"],
					"type", "trade",
				)
			}
			_, err := pipe.Exec(rctx)
			if err != nil {
				log.Println("error execing pipeline", err)
			}
		}
	}
}

func main() {
	config, err := util.LoadConfig(".")
	if err != nil {
		log.Fatal("could not load config", err)
	}

	batchChan := make(chan []map[string]interface{}, 100)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go redisConsumer(batchChan, config.RedisEndpoint, config.StartID, config.TimescaleDBBatchSize)

	for i := 0; i < config.TimescaleDBWorkers; i++ {
		go timeseriesWriter(batchChan, config.RedisEndpoint)
	}

	go func() {
		for {
			log.Println("events per second", tsdbCounter, "cache", len(batchChan))
			tsdbCounter = 0
			time.Sleep(time.Second)
		}
	}()

	<-signalChan
	log.Println("exiting app")
}
