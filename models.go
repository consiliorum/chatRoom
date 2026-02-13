package main

import "time"

const (
	MsgTypeMessage  = "message"
	MsgTypeJoin     = "join"
	MsgTypeLeave    = "leave"
	MsgTypePresence = "presence"
	MsgTypeHistory  = "history"
)

type Message struct {
	Type      string   `json:"type"`
	Room      string   `json:"room"`
	Username  string   `json:"username"`
	Content   string   `json:"content"`
	Timestamp string   `json:"timestamp"`
	Users     []string `json:"users,omitempty"`
}

type HistoryPayload struct {
	Type     string    `json:"type"`
	Room     string    `json:"room"`
	Messages []Message `json:"messages"`
}

func newMessage(msgType, room, username, content string) Message {
	return Message{
		Type:      msgType,
		Room:      room,
		Username:  username,
		Content:   content,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}
