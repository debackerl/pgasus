package main

import (
	"log"
	"net/http"
)

type catchingHandler struct {
	next http.Handler
}

func CatchingHandler(h http.Handler) http.Handler {
	return catchingHandler{h}
}

func (h catchingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("Error while processing request:", r)

			w.WriteHeader(http.StatusInternalServerError)

			if err, ok := r.(error); ok {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.Write([]byte(err.Error()))
			}
		}
	}()

	h.next.ServeHTTP(w, req)
}
