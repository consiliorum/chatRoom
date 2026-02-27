# Chat Room

A real-time chat application built with Go, WebSockets, and Redis.

## Features

- **Real-time messaging** via WebSockets
- **Rooms** — join any room by name, rooms are created on demand
- **Message history** — last 100 messages per room, delivered on join
- **User presence** — live members panel with colour-coded avatars showing who's online
- **Horizontally scalable** — Redis pub/sub enables multiple server instances

## Architecture

```
Browser ←WebSocket→ Go Server ←Pub/Sub→ Redis
                         ↕
                    Redis (history + presence)
```

- `main.go` — HTTP server, static files, WebSocket upgrade endpoint
- `hub.go` — Central hub managing rooms, client registration, heartbeats
- `client.go` — WebSocket read/write pumps with ping/pong keepalive
- `room.go` — Per-room client set, Redis subscription, broadcasting
- `redis.go` — Redis wrapper for pub/sub, message history, and presence
- `models.go` — Message type definitions
- `static/index.html` — Single-page chat UI (dark theme, three-column layout, avatars, room switching)

## Prerequisites

- [Go](https://go.dev/) 1.21+
- [Redis](https://redis.io/) 7+

### macOS (Homebrew)

```bash
brew install go redis
brew services start redis
```

## Running

```bash
git clone https://github.com/linus/chatRoom.git
cd chatRoom
go run .
```

Open http://localhost:8080 in your browser.

## Configuration

| Environment Variable | Default        | Description       |
|---------------------|----------------|-------------------|
| `PORT`              | `8080`         | HTTP server port  |
| `REDIS_ADDR`        | `localhost:6379` | Redis address   |

## Message Protocol

Messages are JSON over WebSocket:

```json
{
  "type": "message|join|leave|presence|history",
  "room": "general",
  "username": "alice",
  "content": "Hello!",
  "timestamp": "2024-01-01T00:00:00Z",
  "users": ["alice", "bob"]
}
```

## License

MIT
