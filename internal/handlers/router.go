package handlers

import "net/http"

func NewRouter(connectionHandler *ConnectionHandler, uploadHandler *UploadHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /connections/test", connectionHandler.TestConnection)
	mux.HandleFunc("POST /storage/upload", uploadHandler.UploadFile)
	return mux
}
