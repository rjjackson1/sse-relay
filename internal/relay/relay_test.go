package relay

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rjjackson1/sse-relay/internal/hub"
)

func TestValidStreamID(t *testing.T) {
	cases := []struct {
		id   string
		want bool
	}{
		{"", false},
		{"a", true},
		{"run-42.job_1", true},
		{strings.Repeat("a", 128), true},
		{strings.Repeat("a", 129), false},
		{"has space", false},
		{"has/slash", false},
		{"has:colon", false},
	}
	for _, c := range cases {
		if got := validStreamID(c.id); got != c.want {
			t.Errorf("validStreamID(%q) = %v, want %v", c.id, got, c.want)
		}
	}
}

func TestLastEventID(t *testing.T) {
	cases := []struct {
		name   string
		header string
		query  string
		want   uint64
	}{
		{"none", "", "", 0},
		{"header", "42", "", 42},
		{"query fallback", "", "7", 7},
		{"header wins over query", "5", "9", 5},
		{"header with whitespace", " 3 ", "", 3},
		{"invalid header falls back to zero", "not-a-number", "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target := "/streams/a/events"
			if c.query != "" {
				target += "?last_event_id=" + c.query
			}
			r := httptest.NewRequest(http.MethodGet, target, nil)
			if c.header != "" {
				r.Header.Set("Last-Event-ID", c.header)
			}
			if got := lastEventID(r); got != c.want {
				t.Errorf("lastEventID() = %d, want %d", got, c.want)
			}
		})
	}
}

func TestHandlePublishRawBody(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	r := httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader("hello"))
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusAccepted, w.Body)
	}
	var resp struct {
		EventID uint64 `json:"event_id"`
		Done    bool   `json:"done"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID != 1 || resp.Done {
		t.Fatalf("resp = %+v, want EventID=1 Done=false", resp)
	}
}

func TestHandlePublishJSONBody(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	r := httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader(`{"data":"chunk","done":true}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body: %s", w.Code, http.StatusAccepted, w.Body)
	}
	stream, ok := srv.hub.Stream("a")
	if !ok {
		t.Fatal("stream was not created")
	}
	if !stream.Done() {
		t.Fatal("expected stream to be finished after done:true")
	}
}

func TestHandlePublishInvalidJSON(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	r := httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader(`not json`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlePublishEmptyChunk(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	r := httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader(""))
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlePublishInvalidStreamID(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	r := httptest.NewRequest(http.MethodPost, "/streams/bad!id", strings.NewReader("x"))
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlePublishAfterDoneConflicts(t *testing.T) {
	h := hub.New(0)
	h.GetOrCreate("a").Finish()
	srv := NewServer(h, Config{})

	r := httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader("late"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusConflict)
	}
}

func TestHandlePublishRequiresToken(t *testing.T) {
	srv := NewServer(hub.New(0), Config{Token: "secret"})

	r := httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader("hi"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing token: status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	r = httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader("hi"))
	r.Header.Set("Authorization", "Bearer wrong")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	r = httptest.NewRequest(http.MethodPost, "/streams/a", strings.NewReader("hi"))
	r.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("correct token: status = %d, want %d, body: %s", w.Code, http.StatusAccepted, w.Body)
	}
}

func TestHandleFinish(t *testing.T) {
	h := hub.New(0)
	srv := NewServer(h, Config{})

	r := httptest.NewRequest(http.MethodPost, "/streams/missing/done", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown stream: status = %d, want %d", w.Code, http.StatusNotFound)
	}

	h.GetOrCreate("a")
	r = httptest.NewRequest(http.MethodPost, "/streams/a/done", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	stream, _ := h.Stream("a")
	if !stream.Done() {
		t.Fatal("expected stream to be finished")
	}
}

func TestHandleDelete(t *testing.T) {
	h := hub.New(0)
	srv := NewServer(h, Config{})

	r := httptest.NewRequest(http.MethodDelete, "/streams/missing", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown stream: status = %d, want %d", w.Code, http.StatusNotFound)
	}

	h.GetOrCreate("a")
	r = httptest.NewRequest(http.MethodDelete, "/streams/a", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if _, ok := h.Stream("a"); ok {
		t.Fatal("stream still present after delete")
	}
}

func TestHandleStatsAndList(t *testing.T) {
	h := hub.New(0)
	srv := NewServer(h, Config{})

	r := httptest.NewRequest(http.MethodGet, "/streams/missing", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown stream: status = %d, want %d", w.Code, http.StatusNotFound)
	}

	stream := h.GetOrCreate("a")
	if _, err := stream.Publish("x"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	r = httptest.NewRequest(http.MethodGet, "/streams/a", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var stats hub.Stats
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.ID != "a" || stats.Events != 1 {
		t.Fatalf("stats = %+v, want ID=a Events=1", stats)
	}

	r = httptest.NewRequest(http.MethodGet, "/streams", nil)
	w = httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	var listed struct {
		Streams []hub.Stats `json:"streams"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed.Streams) != 1 || listed.Streams[0].ID != "a" {
		t.Fatalf("listed = %+v, want one stream named a", listed.Streams)
	}
}

func TestHandleHealth(t *testing.T) {
	h := hub.New(0)
	srv := NewServer(h, Config{})
	h.GetOrCreate("a")

	r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var resp struct {
		Status  string `json:"status"`
		Streams int    `json:"streams"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	if resp.Status != "ok" || resp.Streams != 1 {
		t.Fatalf("resp = %+v, want Status=ok Streams=1", resp)
	}
}

func TestHandleEventsUnknownStream(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	r := httptest.NewRequest(http.MethodGet, "/streams/missing/events", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}
}

// A finished stream never blocks on the live path, so these two tests exercise
// handleEvents end to end without needing a goroutine and a context cancel.

func TestHandleEventsReplaysThenSendsDoneForFinishedStream(t *testing.T) {
	h := hub.New(0)
	srv := NewServer(h, Config{})
	stream := h.GetOrCreate("a")
	if _, err := stream.Publish("first"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := stream.Publish("second"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	stream.Finish()

	r := httptest.NewRequest(http.MethodGet, "/streams/a/events", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	body := w.Body.String()
	for _, want := range []string{"id: 1\n", "data: first\n", "id: 2\n", "data: second\n", "event: done"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q, got:\n%s", want, body)
		}
	}
	if strings.Contains(body, "event: gap") {
		t.Fatalf("unexpected gap frame, got:\n%s", body)
	}
}

func TestHandleEventsReportsGapAfterEviction(t *testing.T) {
	h := hub.New(2)
	srv := NewServer(h, Config{})
	stream := h.GetOrCreate("a")
	for i := 0; i < 3; i++ {
		if _, err := stream.Publish("x"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	stream.Finish()

	r := httptest.NewRequest(http.MethodGet, "/streams/a/events", nil)
	r.Header.Set("Last-Event-ID", "0")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "event: gap\ndata: {\"after\":0}\n\n") {
		t.Fatalf("body missing gap frame, got:\n%s", body)
	}
	if strings.Contains(body, "id: 1\n") {
		t.Fatalf("evicted event 1 should not have been replayed, got:\n%s", body)
	}
}
