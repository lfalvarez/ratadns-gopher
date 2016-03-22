package sse

import (
	"net/http"
	"fmt"
)

//SSEServer struct has an EventManager where to read messages and a function to process the message and
//then writes the message to a HTML5 SSE.
type SSEServer struct {
	channel chan [] byte
}

//NewSSEServer initialize a SSESever with the default values.
func NewSSEServer(channel chan []byte) *SSEServer{
	sseServer := new(SSEServer)
	sseServer.channel = channel

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

	for{
		fmt.Fprintf(writer, "data: %s\n\n", <-server.channel)
		flusher.Flush()
	}

}