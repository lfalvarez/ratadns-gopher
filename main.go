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
	serversLocation := make(map[string]util.Location)

	serversLocation["beaucheff"] = util.Location{Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }
	serversLocation["blanco"] = util.Location{Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }

	encoder := json.NewEncoder(rw)
	encoder.Encode(serversLocation)
}

func encodeServers(c util.Configuration){
	serversLocation := make(map[string]util.Location)

	for _, value := range c.Servers {
		serversLocation[value.Name] = value.Data
	}
}

func runSseServer(redisClient *redis.Client, serverFunction func(channel chan []byte, client *redis.Client, l *lumberjack.Logger, c util.Configuration), url string, l *lumberjack.Logger, config util.Configuration) {
	channel := make(chan []byte)
	serverFunction(channel, redisClient, l, config)

	sseServer := sse.NewSSEServer(channel)
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

	encodeServers(config)

	runSseServer(redisClient, servers.ServDataEvent, "/servData", l, config)
	runSseServer(redisClient, servers.GeoEvent, "/geo", l, config)
	runSseServer(redisClient, servers.TopKEvent, "/sse", l, config)

	http.HandleFunc("/serversLocation", serversLocation)
	http.ListenAndServe(":8080", nil)
}