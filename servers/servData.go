package servers

import (
	"ratadns-gopher/sse"
	"gopkg.in/redis.v3"
	"gopkg.in/natefinch/lumberjack.v2"
	"ratadns-gopher/util"
)

//ServDataEvent reads messages from redis channels QueriesPerSecond and AnswersPerSecond and writes them to
//a HTML5 SSE.
func ServDataEvent(eventManager *sse.EventManager, client *redis.Client, l *lumberjack.Logger, c util.Configuration) {
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
