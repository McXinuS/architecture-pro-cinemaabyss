package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/IBM/sarama/mocks"
)

func TestDecoders(t *testing.T) {
	tests := []struct {
		name    string
		decode  decoder
		body    string
		wantID  string
		wantErr bool
	}{
		{"movie", decodeMovie, `{"movie_id":1,"title":"Inception","action":"viewed","user_id":2}`, "movie-1-viewed", false},
		{"movie missing title", decodeMovie, `{"movie_id":1,"action":"viewed"}`, "", true},
		{"user", decodeUser, `{"user_id":3,"action":"logged_in","timestamp":"2023-01-15T14:30:00Z"}`, "user-3-logged_in", false},
		{"user missing timestamp", decodeUser, `{"user_id":3,"action":"logged_in"}`, "", true},
		{"payment", decodePayment, `{"payment_id":4,"user_id":3,"amount":9.99,"status":"completed","timestamp":"2023-01-15T14:30:00Z"}`, "payment-4-completed", false},
		{"payment missing amount", decodePayment, `{"payment_id":4,"user_id":3,"status":"completed","timestamp":"2023-01-15T14:30:00Z"}`, "", true},
		{"payment missing status", decodePayment, `{"payment_id":4,"user_id":3,"amount":9.99,"timestamp":"2023-01-15T14:30:00Z"}`, "", true},
		{"invalid json", decodeMovie, `{`, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			_, id, err := tt.decode(req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %t", err, tt.wantErr)
			}
			if id != tt.wantID {
				t.Fatalf("id = %q, want %q", id, tt.wantID)
			}
		})
	}
}

func TestUniqueEventID(t *testing.T) {
	first, err := uniqueEventID("movie-1-viewed")
	if err != nil {
		t.Fatal(err)
	}
	second, err := uniqueEventID("movie-1-viewed")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("event IDs must be unique: %q", first)
	}
	if !strings.HasPrefix(first, "movie-1-viewed-") {
		t.Fatalf("event ID = %q, want movie prefix", first)
	}
}

func TestHealthReflectsKafkaReadiness(t *testing.T) {
	svc := &service{}
	rec := httptest.NewRecorder()
	svc.health(rec, httptest.NewRequest(http.MethodGet, "/api/events/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status without Kafka = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	producer := mocks.NewSyncProducer(t, newConfig())
	defer producer.Close()
	svc.producer = producer
	rec = httptest.NewRecorder()
	svc.health(rec, httptest.NewRequest(http.MethodGet, "/api/events/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status with Kafka = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleWithoutKafka(t *testing.T) {
	h := (&service{}).handle("movie", decodeMovie)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodPost, "/api/events/movie",
		strings.NewReader(`{"movie_id":1,"title":"Inception","action":"viewed"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/api/events/movie", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}

	var body map[string]string
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["error"] == "" {
		t.Fatal("expected error message in body")
	}
}
