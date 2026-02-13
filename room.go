package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
)

type Room struct {
	name    string
	clients map[*Client]bool
	mu      sync.RWMutex
	pubsub  *redis.PubSub
	cancel  context.CancelFunc
}

func NewRoom(name string) *Room {
	return &Room{
		name:    name,
		clients: make(map[*Client]bool),
	}
}

func (r *Room) AddClient(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c] = true
}

func (r *Room) RemoveClient(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, c)
}

func (r *Room) IsEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients) == 0
}

// Broadcast sends a raw JSON message to all clients in the room.
func (r *Room) Broadcast(data []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for client := range r.clients {
		select {
		case client.send <- data:
		default:
			// Skip slow clients
		}
	}
}

// StartSubscription listens to the Redis channel for this room
// and broadcasts received messages to local clients.
func (r *Room) StartSubscription(ctx context.Context, rdb *RedisClient) {
	subCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.pubsub = rdb.Subscribe(subCtx, r.name)

	go func() {
		ch := r.pubsub.Channel()
		for {
			select {
			case <-subCtx.Done():
				return
			case redisMsg, ok := <-ch:
				if !ok {
					return
				}
				r.Broadcast([]byte(redisMsg.Payload))
			}
		}
	}()
}

// StopSubscription unsubscribes from the Redis channel.
func (r *Room) StopSubscription() {
	if r.cancel != nil {
		r.cancel()
	}
	if r.pubsub != nil {
		if err := r.pubsub.Close(); err != nil {
			log.Printf("Error closing pubsub for room %s: %v", r.name, err)
		}
	}
}

// BroadcastPresence sends the current presence list to all room clients.
func (r *Room) BroadcastPresence(rdb *RedisClient) {
	ctx := context.Background()
	users, err := rdb.GetPresence(ctx, r.name)
	if err != nil {
		log.Printf("Error getting presence for room %s: %v", r.name, err)
		return
	}

	msg := Message{
		Type:  MsgTypePresence,
		Room:  r.name,
		Users: users,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	r.Broadcast(data)
}
