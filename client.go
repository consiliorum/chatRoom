package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
)

type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan []byte
	room     string
	username string
}

func NewClient(hub *Hub, conn *websocket.Conn, room, username string) *Client {
	return &Client{
		hub:      hub,
		conn:     conn,
		send:     make(chan []byte, 256),
		room:     room,
		username: username,
	}
}

// readPump reads messages from the WebSocket connection.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, rawMsg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		var incoming Message
		if err := json.Unmarshal(rawMsg, &incoming); err != nil {
			log.Printf("Invalid message from %s: %v", c.username, err)
			continue
		}

		msg := newMessage(MsgTypeMessage, c.room, c.username, incoming.Content)

		ctx := context.Background()
		if err := c.hub.redis.Publish(ctx, c.room, msg); err != nil {
			log.Printf("Redis publish error: %v", err)
		}
		if err := c.hub.redis.AddToHistory(ctx, c.room, msg); err != nil {
			log.Printf("Redis history error: %v", err)
		}
	}
}

// writePump writes messages to the WebSocket connection.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// sendJSON marshals and sends a JSON message to this client.
func (c *Client) sendJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("JSON marshal error: %v", err)
		return
	}
	select {
	case c.send <- data:
	default:
		// Client send buffer full, drop message
	}
}
