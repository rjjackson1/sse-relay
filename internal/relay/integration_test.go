package relay

import (
	"bufio"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rjjackson1/sse-relay/internal/hub"
)

// These tests run the server on a real net/http listener (httptest.NewServer,
// not httptest.NewRecorder) and read the response body as a real client would:
// incrementally, off the wire, while the connection is still open. That is the
// only way to exercise the live path in handleEvents - the Recorder-based
// tests in relay_test.go only ever see a response after the handler already
// returned, which happens to be true for a stream that finished before the
// client subscribed but says nothing about a subscriber that is attached
// while the producer is still publishing.

// sseFrame is one parsed frame off the wire.
type sseFrame struct {
	id    uint64
	event string
	data  string
}

// readSSEFrame reads lines until the blank line that ends a frame, skipping
// heartbeat comments and the leading retry: line, neither of which carry an
// id, event or data field of their own.
func readSSEFrame(r *bufio.Reader) (sseFrame, error) {
	var frame sseFrame
	var data []string
	empty := true
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return sseFrame{}, err
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case line == "":
			if empty {
				continue
			}
			frame.data = strings.Join(data, "\n")
			return frame, nil
		case strings.HasPrefix(line, ":"), strings.HasPrefix(line, "retry:"):
			// heartbeat comment or leading retry hint, no field of its own
		case strings.HasPrefix(line, "event: "):
			frame.event = strings.TrimPrefix(line, "event: ")
			empty = false
		case strings.HasPrefix(line, "id: "):
			id, err := strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			if err != nil {
				return sseFrame{}, fmt.Errorf("parse id line %q: %w", line, err)
			}
			frame.id = id
			empty = false
		case strings.HasPrefix(line, "data: "):
			data = append(data, strings.TrimPrefix(line, "data: "))
			empty = false
		default:
			return sseFrame{}, fmt.Errorf("unexpected SSE line %q", line)
		}
	}
}

// readFrame reads one frame with a deadline, so a bug that stalls delivery
// fails the test instead of hanging the suite. Fatalf only runs on the test
// goroutine via the select, never inside the reader goroutine itself.
func readFrame(t *testing.T, r *bufio.Reader) sseFrame {
	t.Helper()
	type result struct {
		frame sseFrame
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := readSSEFrame(r)
		ch <- result{f, err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("read SSE frame: %v", res.err)
		}
		return res.frame
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for an SSE frame")
		return sseFrame{}
	}
}

func postChunk(t *testing.T, client *http.Client, base, id, chunk string) {
	t.Helper()
	resp, err := client.Post(base+"/streams/"+id, "text/plain", strings.NewReader(chunk))
	if err != nil {
		t.Fatalf("publish %q: %v", chunk, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("publish %q: status = %d, want %d", chunk, resp.StatusCode, http.StatusAccepted)
	}
}

func finishStream(t *testing.T, client *http.Client, base, id string) {
	t.Helper()
	resp, err := client.Post(base+"/streams/"+id+"/done", "text/plain", nil)
	if err != nil {
		t.Fatalf("finish %s: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("finish %s: status = %d, want %d", id, resp.StatusCode, http.StatusOK)
	}
}

func TestIntegrationLiveStreamOverRealListener(t *testing.T) {
	srv := NewServer(hub.New(0), Config{})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	client := ts.Client()

	resp, err := client.Get(ts.URL + "/streams/live/events")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body := bufio.NewReader(resp.Body)

	chunks := []string{"hello", " world", "!"}
	for i, chunk := range chunks {
		// The subscriber is already attached and blocked reading, so this
		// chunk has to travel the live fan-out path, not the replay buffer.
		postChunk(t, client, ts.URL, "live", chunk)

		f := readFrame(t, body)
		wantID := uint64(i + 1)
		if f.id != wantID || f.data != chunk {
			t.Fatalf("frame %d = %+v, want id=%d data=%q", i, f, wantID, chunk)
		}
	}

	finishStream(t, client, ts.URL, "live")
	f := readFrame(t, body)
	if f.event != "done" {
		t.Fatalf("final frame event = %q, want %q", f.event, "done")
	}
}

func TestIntegrationLastEventIDResumeOverRealListener(t *testing.T) {
	h := hub.New(0)
	srv := NewServer(h, Config{})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	client := ts.Client()

	// Published before anyone subscribes, so a fresh connection must recover
	// them from the replay buffer rather than the live channel.
	postChunk(t, client, ts.URL, "resume", "one")
	postChunk(t, client, ts.URL, "resume", "two")

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/streams/resume/events", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Last-Event-ID", "1")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body := bufio.NewReader(resp.Body)

	f := readFrame(t, body)
	if f.id != 2 || f.data != "two" {
		t.Fatalf("replayed frame = %+v, want id=2 data=%q", f, "two")
	}

	// Now that the replay is drained, a new chunk has to arrive over the
	// still-open connection's live path.
	postChunk(t, client, ts.URL, "resume", "three")
	f = readFrame(t, body)
	if f.id != 3 || f.data != "three" {
		t.Fatalf("live frame = %+v, want id=3 data=%q", f, "three")
	}

	finishStream(t, client, ts.URL, "resume")
	f = readFrame(t, body)
	if f.event != "done" {
		t.Fatalf("final frame event = %q, want %q", f.event, "done")
	}
}
