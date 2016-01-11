package main

import (
	"sse"
	"time"
	"fmt"
	"net/http"
)

func main() {

	eventManager := sse.NewEventManager()

	go eventManager.Listen()

	go func() {
		for {
			time.Sleep(time.Second * 2)
			eventString := fmt.Sprintf("the time is %v", time.Now())
			eventManager.InputChannel <- []byte(eventString)
		}
	}()

	sseServer := sse.NewSSEServer(eventManager, func(in []byte) []byte {
		return in
	})

	sseServer2 := sse.NewSSEServer(eventManager, func(in []byte) []byte {
		return in[:len(in)/2]
	})

	http.Handle("/", sseServer)

	http.Handle("/1", sseServer2)

	http.ListenAndServe(":8080", nil)
}