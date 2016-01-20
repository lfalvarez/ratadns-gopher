package servers
import (
	"gopkg.in/redis.v3"
	"../sse"
	"sync"
	"../util"
	"net/http"
	"io/ioutil"
	"encoding/json"
)

func GeoEvent(eventManager *sse.EventManager, client *redis.Client) {
	/*malformed, err := client.Subscribe("QueriesWithUnderscoredName")
	if err != nil {
		panic(err)
	}*/
	summary, err := client.Subscribe("QueriesSummary")
	if err != nil {panic(err)}
	clientsNumber := 0
	c1 := make(chan bool)
	// c2 := make(chan bool)
	lock := &sync.Mutex{}
	go func() {
		for {
			if clientsNumber > 0 {
				jsonMsg, err := summary.ReceiveMessage()
				if err != nil {
					panic(err)
				}
				var msg Message
				err = msg.UnmarshalJSON([]byte(jsonMsg.Payload))
				if err != nil {panic(err)}
				for _, summaryEntry := range *msg.Payload.(*QueriesSummary) {
					ip := util.HexToIp(summaryEntry.Ip)
					summaryEntry.Ip = ip
					res, err := http.Get("http://172.17.66.212:8080/json/" + ip)
					if err != nil {panic(err)}
					body, err := ioutil.ReadAll(res.Body)
					var geoData Location
					json.Unmarshal(body, &geoData)
					summaryEntry.Location = geoData
				}
				outputBytes, err := msg.MarshalJSON()
				if err != nil {panic(err)}
				lock.Lock()
				if clientsNumber > 0 {
					eventManager.InputChannel <- outputBytes
				}
				lock.Unlock()
			} else {
				<-c1
			}
		}
	}()
	/*
	go func() {
		for {
			if clientsNumber >0 {
				jsonMsg, err := malformed.ReceiveMessage()
				if err != nil {
					panic(err)
				}
				var malformedMsg Message
				err = malformedMsg.UnmarshalJSON([]byte(jsonMsg.Payload))
				outputMsg := &QueriesSummary {}
				for _, queries := range malformedMsg.Payload.(*QueriesWithUnderscoredName) {
					for _, query := range queries {
						ip := util.HexToIp(queries)
						res, err := http.Get("http://172.17.66.212:8080/json/" + ip)
						if err != nil {
							panic(err)
						}
						body, err := ioutil.ReadAll(res.Body)

						var geoData Location
						json.Unmarshal(body, &geoData)

						append(outputMsg, &{Ip: ip, Queries: nil, Location: geodata })
					}
				}
				if err != nil {panic(err)}
				lock.Lock()
				if clientsNumber >0 {
					eventManager.InputChannel <-[]byte(malformedMsg.Payload)
				}
				lock.Unlock()
			} else {
				<-c2
			}
		}
	}()
	*/
	go util.SynchronizeNbOfClients(lock, &clientsNumber, eventManager.ClientConnected, c1)
}