package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jaredmcqueen/redis-streams-timeseries/util"
)

var tsdbCounter int32

func redisConsumer(batchChan chan<- []map[string]interface{}, endpoint string, startID string) {
	rctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})

	// test redis connection
	_, err := rdb.Ping(rctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis streams endpoint", err)
	}
	log.Println("connected to redis streams endpoint", endpoint)

	pit := startID
	for {
		trades, err := rdb.XRead(rctx, &redis.XReadArgs{
			Streams: []string{"trades", pit},
			Count:   10_000,
		}).Result()
		if err != nil {
			log.Fatal("error XRead: ", err)
		}

		bigBatch := make([]map[string]interface{}, 0, len(trades))

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
	rdb := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})

	// test redis connection
	_, err := rdb.Ping(rctx).Result()
	if err != nil {
		log.Fatal("could not connect to redis timeseries endpoint", err)
	}
	log.Println("connected to redis timeseries endpoint", endpoint)

	for {
		select {
		case batch := <-batchChan:
			pipe := rdb.Pipeline()

			// make a set to store unique symbols
			symbolSet := make(map[string]bool)

			for _, v := range batch {
				tsdbCounter++
				symbolSet[fmt.Sprintf("%s", v["S"])] = true

				// "t": fmt.Sprintf("%v", t.Timestamp.UnixMilli()),
				// "S": t.Symbol,
				// "p": fmt.Sprintf("%v", t.Price),
				// "i": fmt.Sprintf("%v", t.ID),
				// "s": fmt.Sprintf("%v", t.Size),
				// "c": fmt.Sprintf("%v", t.Conditions),
				// "x": t.Exchange,
				// "z": t.Tape,

				pipe.Do(rctx,
					"TS.ADD",
					//key
					fmt.Sprintf("trades:%v:price", v["S"]),
					//time
					v["t"],
					//value
					fmt.Sprintf("%v", v["p"]),
					"ON_DUPLICATE",
					"FIRST",
					"LABELS",
					"type", "price",
					"symbol", v["S"],
					"conditions", v["c"],
					"exchange", v["x"],
					"tape", v["z"],
				)
				pipe.Do(rctx,
					"TS.ADD",
					//key
					fmt.Sprintf("trades:%v:size", v["S"]),
					//ts
					v["t"],
					//value
					fmt.Sprintf("%v", v["s"]),
					"ON_DUPLICATE",
					"FIRST",
					"LABELS",
					"type", "size",
					"symbol", v["S"],
					"conditions", v["c"],
					"exchange", v["x"],
					"tape", v["z"],
				)
			}

			for k := range symbolSet {
				pipe.SAdd(rctx, "symbols", k)
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

	batchChan := make(chan []map[string]interface{}, 1_000_000)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go redisConsumer(batchChan, config.RedisStreamsEndpoint, config.StartID)

	for i := 0; i < config.Workers; i++ {
		go timeseriesWriter(batchChan, config.RedisTimeseriesEndpoint)
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
