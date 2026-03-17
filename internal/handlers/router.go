package handlers

import "net/http"

func NewRouter(connectionHandler *ConnectionHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /go/connections/test", connectionHandler.TestConnection)
	return mux
}
