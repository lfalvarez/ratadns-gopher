package servers

import (
	"../sse"
	"gopkg.in/redis.v3"
	"sync"
	"../util"
)

func ServDataEvent(eventManager *sse.EventManager, client *redis.Client) {
	qps, err := client.Subscribe("QueriesPerSecond")
	if err != nil {
		panic(err)
	}

	aps, err := client.Subscribe("AnswersPerSecond")
	if err != nil {
		panic(err)
	}

	clientsNumber := 0
	c1 := make(chan bool)
	c2 := make(chan bool)
	lock := &sync.Mutex {}

	go func() {
		for {
			if clientsNumber > 0 {
				msg, err := qps.ReceiveMessage()
				if err != nil {
					panic(err)
				}
				lock.Lock()
				if clientsNumber > 0 {
					eventManager.InputChannel <- []byte(msg.Payload) //Block this write so the channel will be buffered only if one or more clients are connected
				}
				lock.Unlock()
			} else {
				<- c1
			}
		}
	}()

	go func() {
		for {
			if clientsNumber > 0 {
				msg, err := aps.ReceiveMessage()
				if err != nil {
					panic(err)
				}
				lock.Lock()
				if clientsNumber > 0 {
					eventManager.InputChannel <- []byte(msg.Payload) //Block this write so the channel will be buffered only if one or more clients are connected
				}
				lock.Unlock()
			} else {
				<- c2
			}
		}
	}()

	go util.SynchronizeNbOfClients(lock, &clientsNumber, eventManager.ClientConnected, c1, c2)
}
