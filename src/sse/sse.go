package sse

import (
	"net/http"
	"fmt"
)

//Listener struct stores an OutPut channel to write messages after processing them with the ProcessFunction.
type Listener struct{
	//Channel to where the processed data will be written
	OutputChannel chan []byte

	//Function to process the event data
	processFunction func(eventData [] byte) []byte
}

//NewListener initialize a Listener with the specified process function returning a pointer to the Listener.
func NewListener(processFunction func (eventData [] byte) []byte) *Listener {
	listener := new(Listener)
	listener.OutputChannel = make(chan []byte)
	listener.processFunction = processFunction
	return listener
}

//EventManager struct that has a map of pointer of Listener to booleans to check if they are being used and the channel
//where to those Listeners will write. //TODO is the map really necessary?
type EventManager struct{
	//Set of Listeners to which the events will be processed
	listeners map[*Listener]bool

	//Input channel of events
	InputChannel chan []byte
}

//NewEventManager initialize an EventManager with default values and return a pointer to the EventManager.
func NewEventManager() *EventManager {
	eventManager := new(EventManager)
	eventManager.listeners = make(map[*Listener]bool)
	eventManager.InputChannel = make(chan []byte)
	return eventManager
}

//AddListener set the values of the pointer of a listener to true (so it's being used now).
func (event *EventManager) AddListener( listener *Listener){
	event.listeners[listener] = true
}

//RemoveListener removes the Listener of the map.
func (event *EventManager) RemoveListener(listener *Listener) {
	delete(event.listeners, listener)
}

//Listen starts a never ending for that reads a message of input channel and, for every Listener in listeners,
// writes the message in the Listener OutputChannel.
func (event *EventManager) Listen(){
	for{
		data := <-event.InputChannel
		for listener := range event.listeners{
			listener.OutputChannel <- listener.processFunction(data)
		}
	}
}

//SSEServer struct has an EventManager where to read messages and a function to process the message and
//then writes the message to a HTML5 SSE.
type SSEServer struct {
	eventManager *EventManager
	processFunction func(eventData [] byte) []byte
}

//NewSSEServer initialize a SSESever with the default values.
func NewSSEServer(eventManager *EventManager, processFunction func(in []byte) []byte) *SSEServer{
	sseServer := new(SSEServer)
	sseServer.eventManager = eventManager
	sseServer.processFunction = processFunction

	return sseServer
}

//ServeHTTP writes the messages of the SSEServer OutputChannel and writes it to an HTML5 SSE.
func (server *SSEServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// Make sure that the writer supports flushing.
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("Access-Control-Allow-Origin", "*")

	listener := NewListener(server.processFunction)

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