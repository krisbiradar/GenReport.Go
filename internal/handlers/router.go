package handlers

import "net/http"

func NewRouter(connectionHandler *ConnectionHandler, uploadHandler *UploadHandler, queryValidationHandler *QueryValidationHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /connections/test", connectionHandler.TestConnection)
	mux.HandleFunc("POST /storage/upload", uploadHandler.UploadFile)
	mux.HandleFunc("POST /queries/validate", queryValidationHandler.ValidateQuery)
	return mux
}
