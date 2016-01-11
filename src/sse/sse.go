package sse

import (
	"net/http"
	"fmt"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome, %s!", r.URL.Path[5:])
}