package servers

import (
	"gopkg.in/redis.v3"
	"ratadns-gopher/sse"
	"ratadns-gopher/util"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"gopkg.in/natefinch/lumberjack.v2"
)

//GeoEvent receives a message of the redis channel QueriesSummary, adds the location of the message on it and sends it
//to a HTML5 SSE.
func GeoEvent(eventManager *sse.EventManager, client *redis.Client, l *lumberjack.Logger, c util.Configuration) {
	/*malformed, err := client.Subscribe("QueriesWithUnderscoredName")//TODO
	if err != nil {
		panic(err)
	}*/
	summary, err := client.Subscribe("QueriesSummary")
	if err != nil {panic(err)}
	go func() {
		for {
			jsonMsg, err := summary.ReceiveMessage()
			if err != nil {
				fmt.Println(err) //TODO: logger
				continue
			}
			var msg Message
			err = msg.UnmarshalJSON([]byte(jsonMsg.Payload))
			if err != nil {panic(err)}
			for _, summaryEntry := range *msg.Payload.(*QueriesSummary) {
				ip := util.HexToIp(summaryEntry.Ip)
				summaryEntry.Ip = ip
				res, err := http.Get(c.Geo.Address + ip)
				if err != nil {panic(err)} //TODO: logger
				body, err := ioutil.ReadAll(res.Body)
				var geoData Location
				json.Unmarshal(body, &geoData)
				summaryEntry.Location = geoData
			}
			outputBytes, err := msg.MarshalJSON()
			if err != nil {panic(err)} //TODO: logger
			eventManager.InputChannel <- outputBytes
		}
	}()
	/*//TODO: check if this will be used.
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
}