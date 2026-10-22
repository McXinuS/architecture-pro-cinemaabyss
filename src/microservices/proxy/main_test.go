package main

import (
	"net/http/httputil"
	"testing"
)

func newTestRouter(gradual bool, percent, rollValue int) *router {
	return &router{
		monolith:         &httputil.ReverseProxy{},
		movies:           &httputil.ReverseProxy{},
		events:           &httputil.ReverseProxy{},
		gradualMigration: gradual,
		migrationPercent: percent,
		roll:             func() int { return rollValue },
	}
}

func TestRoute(t *testing.T) {
	tests := []struct {
		name    string
		gradual bool
		percent int
		roll    int
		path    string
		want    string
	}{
		{"users stay on monolith", true, 100, 0, "/api/users", "monolith"},
		{"payments stay on monolith", true, 100, 0, "/api/payments", "monolith"},
		{"events go to events service", true, 0, 99, "/api/events/movie", "events"},
		{"movies health always hits movies", true, 0, 99, "/api/movies/health", "movies"},
		{"roll below percent goes to movies", true, 50, 49, "/api/movies", "movies"},
		{"roll at percent stays on monolith", true, 50, 50, "/api/movies", "monolith"},
		{"zero percent keeps monolith", true, 0, 0, "/api/movies", "monolith"},
		{"hundred percent goes to movies", true, 100, 99, "/api/movies", "movies"},
		{"migration off cuts over fully", false, 0, 99, "/api/movies", "movies"},
		{"similar prefix is not movies", true, 100, 0, "/api/moviesx", "monolith"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := newTestRouter(tt.gradual, tt.percent, tt.roll).route(tt.path)
			if got != tt.want {
				t.Fatalf("route(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestParsePercent(t *testing.T) {
	cases := map[string]int{"50": 50, "-5": 0, "150": 100, "abc": 0}
	for in, want := range cases {
		if got := parsePercent(in); got != want {
			t.Errorf("parsePercent(%q) = %d, want %d", in, got, want)
		}
	}
}
