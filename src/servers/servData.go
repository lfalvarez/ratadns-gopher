package servers

import (
	"../sse"
	"gopkg.in/redis.v3"
)

func ServDataEvent(eventManager *sse.EventManager, client *redis.Client) {
	qps, err := client.Subscribe("QueriesPerSecond")
	if err != nil {panic(err)}
	aps, err := client.Subscribe("AnswersPerSecond")
	if err != nil {panic(err)}
	go func() {
		for {
			msg, err := qps.ReceiveMessage()
			if err != nil {
				panic(err)
			}
			eventManager.InputChannel <- []byte(msg.Payload)
		}
	}()
	go func() {
		for {
			msg, err := aps.ReceiveMessage()
			if err != nil {
				panic(err)
			}
			eventManager.InputChannel <- []byte(msg.Payload)
		}
	}()
}
