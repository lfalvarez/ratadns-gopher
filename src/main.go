package main

import (
	"net/http"
	"gopkg.in/redis.v3"
	"./sse"
	"./servers"
	"encoding/json"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
)

func NewRedisClient() (client *redis.Client) {
	client = redis.NewClient(&redis.Options{
		Addr:     "172.17.66.212:6379", // TODO: Exportar a archivo de configuración
	})
	return
}

func serversLocation(rw http.ResponseWriter, rq *http.Request) {
	// TODO: This is a mock, we must do a well structured way to do this.
	// Maybe a configuration file?
	serversLocation := make(map[string]servers.Location)
	serversLocation["beaucheff"] = servers.Location{Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }
	serversLocation["blanco"] = servers.Location{Longitude: -70.663777, Latitude: -33.463254, CountryName: "Chile" }

	encoder := json.NewEncoder(rw)
	encoder.Encode(serversLocation)
}

func runSseServer(redisClient *redis.Client, serverFunction func(eventManager *sse.EventManager, client *redis.Client, l *lumberjack.Logger), url string, l *lumberjack.Logger) {
	eventManager := sse.NewEventManager()
	go eventManager.Listen()
	serverFunction(eventManager, redisClient, l)

	sseServer := sse.NewSSEServer(eventManager, func(in []byte) []byte {
		return in
	})
	http.Handle(url, sseServer)
}

func main() {
	l := &lumberjack.Logger{//TODO: move this configurations to a config file
		Filename:   "/var/log/gopher/log.log",
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, //days
	}
	log.SetOutput(l)
	redisClient := NewRedisClient()

	runSseServer(redisClient, servers.ServDataEvent, "/servData", l)
	runSseServer(redisClient, servers.GeoEvent, "/geo", l)
	runSseServer(redisClient, servers.TopKEvent, "/sse", l)
	http.HandleFunc("/serversLocation", serversLocation)
	http.ListenAndServe(":8080", nil)
}