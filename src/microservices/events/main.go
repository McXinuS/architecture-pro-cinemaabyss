package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const consumerGroup = "events-service"

var topics = map[string]string{
	"movie":   "movie-events",
	"user":    "user-events",
	"payment": "payment-events",
}

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     *float64  `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

type service struct {
	mu       sync.RWMutex
	producer sarama.SyncProducer
}

func main() {
	port := getEnv("PORT", "8082")
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc := &service{}
	go svc.connectProducer(ctx, brokers)
	go runConsumer(ctx, brokers)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
	})
	mux.HandleFunc("/api/events/health", svc.health)
	mux.HandleFunc("/api/events/movie", svc.handle("movie", decodeMovie))
	mux.HandleFunc("/api/events/user", svc.handle("user", decodeUser))
	mux.HandleFunc("/api/events/payment", svc.handle("payment", decodePayment))

	srv := &http.Server{Addr: ":" + port, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("Starting events service on port %s, brokers %v", port, brokers)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	svc.close()
}

func newConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "cinemaabyss-events"
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	return cfg
}

// Kafka usually starts after this service in docker-compose and Kubernetes,
// so both the producer and the consumer keep retrying instead of exiting.
func (s *service) connectProducer(ctx context.Context, brokers []string) {
	for {
		producer, err := sarama.NewSyncProducer(brokers, newConfig())
		if err == nil {
			s.mu.Lock()
			s.producer = producer
			s.mu.Unlock()
			log.Println("Kafka producer connected")
			return
		}
		log.Printf("Kafka producer not ready: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (s *service) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.producer != nil {
		s.producer.Close()
	}
}

type decoder func(*http.Request) (payload any, id string, err error)

func (s *service) health(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	ready := s.producer != nil
	s.mu.RUnlock()
	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]bool{"status": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (s *service) handle(eventType string, decode decoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		payload, id, err := decode(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		raw, err := json.Marshal(payload)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		id, err = uniqueEventID(id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to create event id")
			return
		}
		event := Event{ID: id, Type: eventType, Timestamp: time.Now().UTC(), Payload: raw}
		value, err := json.Marshal(event)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}

		s.mu.RLock()
		producer := s.producer
		s.mu.RUnlock()
		if producer == nil {
			writeError(w, http.StatusServiceUnavailable, "kafka is not available yet")
			return
		}

		topic := topics[eventType]
		partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(id),
			Value: sarama.ByteEncoder(value),
		})
		if err != nil {
			log.Printf("failed to publish %s event %s: %v", eventType, id, err)
			writeError(w, http.StatusInternalServerError, "failed to publish event")
			return
		}

		log.Printf("Produced %s event %s to %s [partition %d, offset %d]", eventType, id, topic, partition, offset)
		writeJSON(w, http.StatusCreated, EventResponse{
			Status:    "success",
			Partition: partition,
			Offset:    offset,
			Event:     event,
		})
	}
}

func decodeMovie(r *http.Request) (any, string, error) {
	var e MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		return nil, "", fmt.Errorf("invalid movie event: %w", err)
	}
	if e.MovieID == 0 || e.Title == "" || e.Action == "" {
		return nil, "", errors.New("movie_id, title and action are required")
	}
	return e, fmt.Sprintf("movie-%d-%s", e.MovieID, e.Action), nil
}

func decodeUser(r *http.Request) (any, string, error) {
	var e UserEvent
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		return nil, "", fmt.Errorf("invalid user event: %w", err)
	}
	if e.UserID == 0 || e.Action == "" || e.Timestamp.IsZero() {
		return nil, "", errors.New("user_id, action and timestamp are required")
	}
	return e, fmt.Sprintf("user-%d-%s", e.UserID, e.Action), nil
}

func decodePayment(r *http.Request) (any, string, error) {
	var e PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		return nil, "", fmt.Errorf("invalid payment event: %w", err)
	}
	if e.PaymentID == 0 || e.UserID == 0 || e.Amount == nil || e.Status == "" || e.Timestamp.IsZero() {
		return nil, "", errors.New("payment_id, user_id, amount, status and timestamp are required")
	}
	return e, fmt.Sprintf("payment-%d-%s", e.PaymentID, e.Status), nil
}

func uniqueEventID(prefix string) (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(random), nil
}

type consumerHandler struct{}

func (consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Consumed malformed message from %s [partition %d, offset %d]: %v",
				msg.Topic, msg.Partition, msg.Offset, err)
		} else {
			log.Printf("Consumed %s event %s from %s [partition %d, offset %d]: %s",
				event.Type, event.ID, msg.Topic, msg.Partition, msg.Offset, event.Payload)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func runConsumer(ctx context.Context, brokers []string) {
	topicList := []string{topics["movie"], topics["user"], topics["payment"]}
	for ctx.Err() == nil {
		group, err := sarama.NewConsumerGroup(brokers, consumerGroup, newConfig())
		if err != nil {
			log.Printf("Kafka consumer not ready: %v", err)
			sleep(ctx, 3*time.Second)
			continue
		}
		log.Printf("Kafka consumer group %q subscribed to %v", consumerGroup, topicList)
		for ctx.Err() == nil {
			if err := group.Consume(ctx, topicList, consumerHandler{}); err != nil {
				log.Printf("Kafka consumer error: %v", err)
				sleep(ctx, 3*time.Second)
			}
		}
		group.Close()
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
