package sse

import (
	"net/http"
	"fmt"
)

//TODO:
type Listener struct{
	//Channel to where the processed data will be written
	OutputChannel chan []byte

	//Function to process the event data
	processFunction func(eventData [] byte) []byte
}
func NewListener(processFunction func (eventData [] byte) []byte) *Listener {
	listener := new(Listener)
	listener.OutputChannel = make(chan []byte)
	listener.processFunction = processFunction
	return listener
}

//TODO:
type EventManager struct{
	//Set of Listeners to which the events will be processed
	listeners map[*Listener]bool

	//Input channel of events
	InputChannel chan []byte
}

//TODO
func NewEventManager() *EventManager {
	eventManager := new(EventManager)
	eventManager.listeners = make(map[*Listener]bool)
	eventManager.InputChannel = make(chan []byte)

	return eventManager
}

//TODO:
func (event *EventManager) AddListener( listener *Listener){
	event.listeners[listener] = true
}

func (event *EventManager) RemoveListener(listener *Listener) {
	delete(event.listeners, listener)
}

//TODO:
func (event *EventManager) Listen(){
	for{
		data := <-event.InputChannel
		for listener := range event.listeners{
			listener.OutputChannel <- listener.processFunction(data)
		}
	}
}

type SSEServer struct {
	eventManager *EventManager
	processFunction func(eventData [] byte) []byte
}

func NewSSEServer(eventManager *EventManager, processFunction func(in []byte) []byte) *SSEServer{
	sseServer := new(SSEServer)
	sseServer.eventManager = eventManager
	sseServer.processFunction = processFunction

	return sseServer
}


func (server *SSEServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Make sure that the writer supports flushing.
	//
	flusher, ok := writer.(http.Flusher)

	if !ok {
		http.Error(writer, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	listener := NewListener(server.processFunction);

	server.eventManager.AddListener(listener)

	defer func() {
		server.eventManager.RemoveListener(listener)
		close(listener.OutputChannel)
	}()


	// Listen to connection close and un-register messageChan
	notify := writer.(http.CloseNotifier).CloseNotify()
	go func() {
		<-notify
		server.eventManager.RemoveListener(listener)
		close(listener.OutputChannel)
	}()

	for{
		fmt.Fprintf(writer, "data: %s\n\n", <-listener.OutputChannel)

		flusher.Flush()
	}

}