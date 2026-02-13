package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
)

type Hub struct {
	rooms      map[string]*Room
	mu         sync.RWMutex
	register   chan *Client
	unregister chan *Client
	redis      *RedisClient
	ctx        context.Context
}

func NewHub(redis *RedisClient) *Hub {
	return &Hub{
		rooms:      make(map[string]*Room),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		redis:      redis,
		ctx:        context.Background(),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.handleRegister(client)
		case client := <-h.unregister:
			h.handleUnregister(client)
		}
	}
}

func (h *Hub) getOrCreateRoom(name string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()

	if room, ok := h.rooms[name]; ok {
		return room
	}

	room := NewRoom(name)
	room.StartSubscription(h.ctx, h.redis)
	h.rooms[name] = room
	log.Printf("Room created: %s", name)
	return room
}

func (h *Hub) handleRegister(client *Client) {
	room := h.getOrCreateRoom(client.room)
	room.AddClient(client)

	ctx := context.Background()

	// Send message history to the new client
	history, err := h.redis.GetHistory(ctx, client.room)
	if err != nil {
		log.Printf("Error fetching history: %v", err)
	} else if len(history) > 0 {
		payload := HistoryPayload{
			Type:     MsgTypeHistory,
			Room:     client.room,
			Messages: history,
		}
		client.sendJSON(payload)
	}

	// Update presence
	h.redis.UpdatePresence(ctx, client.room, client.username)

	// Broadcast join message via Redis
	joinMsg := newMessage(MsgTypeJoin, client.room, client.username, client.username+" joined the room")
	h.redis.Publish(ctx, client.room, joinMsg)
	h.redis.AddToHistory(ctx, client.room, joinMsg)

	// Broadcast updated presence
	room.BroadcastPresence(h.redis)

	// Start heartbeat for this client
	go h.heartbeat(client)

	log.Printf("User %s joined room %s", client.username, client.room)
}

func (h *Hub) handleUnregister(client *Client) {
	h.mu.RLock()
	room, ok := h.rooms[client.room]
	h.mu.RUnlock()

	if !ok {
		return
	}

	room.RemoveClient(client)
	close(client.send)

	ctx := context.Background()

	// Remove presence
	h.redis.RemovePresence(ctx, client.room, client.username)

	// Broadcast leave message
	leaveMsg := newMessage(MsgTypeLeave, client.room, client.username, client.username+" left the room")
	h.redis.Publish(ctx, client.room, leaveMsg)
	h.redis.AddToHistory(ctx, client.room, leaveMsg)

	// Broadcast updated presence
	room.BroadcastPresence(h.redis)

	log.Printf("User %s left room %s", client.username, client.room)

	// Clean up empty rooms
	if room.IsEmpty() {
		h.mu.Lock()
		room.StopSubscription()
		delete(h.rooms, client.room)
		h.mu.Unlock()
		log.Printf("Room deleted: %s", client.room)
	}
}

// heartbeat periodically updates user presence in Redis.
func (h *Hub) heartbeat(client *Client) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if client is still connected (send channel open)
			select {
			case _, ok := <-client.send:
				if !ok {
					return
				}
			default:
			}

			ctx := context.Background()
			h.redis.UpdatePresence(ctx, client.room, client.username)

			h.mu.RLock()
			room, ok := h.rooms[client.room]
			h.mu.RUnlock()
			if !ok {
				return
			}

			// Publish presence update via Redis so all instances see it
			users, err := h.redis.GetPresence(ctx, client.room)
			if err != nil {
				continue
			}
			presenceMsg := Message{
				Type:  MsgTypePresence,
				Room:  client.room,
				Users: users,
			}
			data, _ := json.Marshal(presenceMsg)
			room.Broadcast(data)
		}
	}
}
