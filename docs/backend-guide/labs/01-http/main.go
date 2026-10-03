package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "nanoim"})
	})
	server := &http.Server{Addr: "127.0.0.1:18100", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Println("实验 01：http://localhost:18100/health")
	log.Fatal(server.ListenAndServe())
}
