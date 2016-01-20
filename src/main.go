package main

import (
	"net/http"
	"gopkg.in/redis.v3"
	"./sse"
	"./servers"
	"encoding/json"
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

func main() {
	redisClient := NewRedisClient()

	//servData
	servDataManager := sse.NewEventManager()
	go servDataManager.Listen()
	servers.ServDataEvent(servDataManager, redisClient)

	servDataServer := sse.NewSSEServer(servDataManager, func(in []byte) []byte {
		return in
	})

	//geo
	geoManager := sse.NewEventManager()
	go geoManager.Listen()
	servers.GeoEvent(geoManager, redisClient)

	geoServer := sse.NewSSEServer(geoManager, func(in []byte) []byte {
		return in
	})

	http.HandleFunc("/serversLocation", serversLocation)
	http.Handle("/servData", servDataServer)
	http.Handle("/geo", geoServer)
	http.ListenAndServe(":8080", nil)
}
