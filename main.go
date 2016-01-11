package main

import (
"net/http"
"sse"
)



func main() {
	http.HandleFunc("/sse/", sse.Handler)
	http.ListenAndServe(":8080", nil)
}