package main

import (
	"net/http"
	"gopkg.in/redis.v3"
	"ratadns-gopher/sse"
	"ratadns-gopher/servers"
	"encoding/json"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"ratadns-gopher/util"
)

func NewRedisClient(address string) (client *redis.Client) {
	client = redis.NewClient(&redis.Options{
		Addr:     address,
	})
	return
}

func serversLocation(rw http.ResponseWriter, rq *http.Request) {
	serversLocation := make(map[string]servers.Location)

	serversLocation["beaucheff"] = servers.Location{Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }
	serversLocation["blanco"] = servers.Location{Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }

	encoder := json.NewEncoder(rw)
	encoder.Encode(serversLocation)
}

func runSseServer(redisClient *redis.Client, serverFunction func(eventManager *sse.EventManager, client *redis.Client, l *lumberjack.Logger, c util.Configuration), url string, l *lumberjack.Logger, config util.Configuration) {
	eventManager := sse.NewEventManager()
	go eventManager.Listen()
	serverFunction(eventManager, redisClient, l, config)

	sseServer := sse.NewSSEServer(eventManager, func(in []byte) []byte {
		return in
	})
	http.Handle(url, sseServer)
}

func main() {
	config := util.InitConfig()
	l := &lumberjack.Logger{
		Filename:   config.Log.FileName,
		MaxSize:    config.Log.MaxSize, // megabytes
		MaxBackups: config.Log.MaxBackups,
		MaxAge:     config.Log.MaxAge, //days
	}

	log.SetOutput(l)
	redisClient := NewRedisClient(config.Redis.Address)

	runSseServer(redisClient, servers.ServDataEvent, "/servData", l, config)
	runSseServer(redisClient, servers.GeoEvent, "/geo", l, config)
	runSseServer(redisClient, servers.TopKEvent, "/sse", l, config)

	http.HandleFunc("/serversLocation", serversLocation)
	http.ListenAndServe(":8080", nil)
}