package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	username := r.URL.Query().Get("username")

	if room == "" || username == "" {
		http.Error(w, "room and username are required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	client := NewClient(hub, conn, room, username)
	hub.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	rdb := NewRedisClient(redisAddr)
	hub := NewHub(rdb)
	go hub.Run()

	http.Handle("/", http.FileServer(http.Dir("static")))
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})

	log.Printf("Server starting on :%s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
