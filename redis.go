package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(addr string) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")
	return &RedisClient{client: rdb}
}

func (r *RedisClient) channelName(room string) string {
	return fmt.Sprintf("chat:room:%s", room)
}

func (r *RedisClient) historyKey(room string) string {
	return fmt.Sprintf("history:%s", room)
}

func (r *RedisClient) presenceKey(room string) string {
	return fmt.Sprintf("presence:%s", room)
}

// Publish sends a message to the Redis channel for a room.
func (r *RedisClient) Publish(ctx context.Context, room string, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return r.client.Publish(ctx, r.channelName(room), data).Err()
}

// Subscribe returns a channel that receives messages for a room.
func (r *RedisClient) Subscribe(ctx context.Context, room string) *redis.PubSub {
	return r.client.Subscribe(ctx, r.channelName(room))
}

// AddToHistory appends a message to the room's history list, keeping the last 100.
func (r *RedisClient) AddToHistory(ctx context.Context, room string, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.RPush(ctx, r.historyKey(room), data)
	pipe.LTrim(ctx, r.historyKey(room), -100, -1)
	_, err = pipe.Exec(ctx)
	return err
}

// GetHistory returns the message history for a room.
func (r *RedisClient) GetHistory(ctx context.Context, room string) ([]Message, error) {
	vals, err := r.client.LRange(ctx, r.historyKey(room), 0, -1).Result()
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(vals))
	for _, v := range vals {
		var msg Message
		if err := json.Unmarshal([]byte(v), &msg); err != nil {
			continue
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

// UpdatePresence sets or refreshes a user's presence in a room.
func (r *RedisClient) UpdatePresence(ctx context.Context, room, username string) error {
	return r.client.ZAdd(ctx, r.presenceKey(room), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: username,
	}).Err()
}

// RemovePresence removes a user from the room's presence set.
func (r *RedisClient) RemovePresence(ctx context.Context, room, username string) error {
	return r.client.ZRem(ctx, r.presenceKey(room), username).Err()
}

// GetPresence returns usernames of users active within the last 30 seconds.
func (r *RedisClient) GetPresence(ctx context.Context, room string) ([]string, error) {
	cutoff := float64(time.Now().Unix() - 30)
	// Remove stale entries
	r.client.ZRemRangeByScore(ctx, r.presenceKey(room), "-inf", fmt.Sprintf("%f", cutoff))
	// Get active users
	return r.client.ZRangeByScore(ctx, r.presenceKey(room), &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", cutoff),
		Max: "+inf",
	}).Result()
}
