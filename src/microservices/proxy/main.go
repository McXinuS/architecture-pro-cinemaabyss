package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type router struct {
	monolith         *httputil.ReverseProxy
	movies           *httputil.ReverseProxy
	events           *httputil.ReverseProxy
	gradualMigration bool
	migrationPercent int
	roll             func() int
}

func main() {
	port := getEnv("PORT", "8000")

	r := &router{
		monolith:         newProxy(getEnv("MONOLITH_URL", "http://localhost:8080")),
		movies:           newProxy(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")),
		events:           newProxy(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")),
		gradualMigration: getEnv("GRADUAL_MIGRATION", "false") == "true",
		migrationPercent: parsePercent(getEnv("MOVIES_MIGRATION_PERCENT", "0")),
		roll:             func() int { return rand.Intn(100) },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("Strangler Fig Proxy is healthy"))
	})
	mux.Handle("/", r)

	log.Printf("Starting proxy on port %s (gradual migration: %t, movies migration: %d%%)",
		port, r.gradualMigration, r.migrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func (rt *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	target, proxy := rt.route(req.URL.Path)
	log.Printf("%s %s -> %s", req.Method, req.URL.RequestURI(), target)
	w.Header().Set("X-Routed-To", target)
	proxy.ServeHTTP(w, req)
}

func (rt *router) route(path string) (string, *httputil.ReverseProxy) {
	switch {
	case hasPrefix(path, "/api/events"):
		return "events", rt.events
	case path == "/api/movies/health":
		return "movies", rt.movies
	case hasPrefix(path, "/api/movies"):
		if rt.moviesToMicroservice() {
			return "movies", rt.movies
		}
		return "monolith", rt.monolith
	default:
		return "monolith", rt.monolith
	}
}

// With gradual migration off the movies domain is fully cut over; with it on,
// only MOVIES_MIGRATION_PERCENT of requests reach the new service.
func (rt *router) moviesToMicroservice() bool {
	if !rt.gradualMigration {
		return true
	}
	return rt.roll() < rt.migrationPercent
}

func hasPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func newProxy(rawURL string) *httputil.ReverseProxy {
	target, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("invalid upstream URL %q: %v", rawURL, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		log.Printf("upstream %s unavailable for %s: %v", target.Host, req.URL.Path, err)
		http.Error(w, `{"error":"upstream unavailable"}`, http.StatusBadGateway)
	}
	return proxy
}

func parsePercent(raw string) int {
	p, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid MOVIES_MIGRATION_PERCENT %q, using 0", raw)
		return 0
	}
	if p < 0 {
		return 0
	}
	if p > 100 {
		return 100
	}
	return p
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
