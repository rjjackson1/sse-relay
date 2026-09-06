package hub

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPublishSubscribeDelivery(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")

	sub, gap := s.Subscribe(0, 0)
	if gap {
		t.Fatal("expected no gap on a fresh stream")
	}

	ev, err := s.Publish("hello")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if ev.ID != 1 {
		t.Fatalf("first event id = %d, want 1", ev.ID)
	}

	select {
	case got := <-sub.Events():
		if got.Data != "hello" || got.ID != 1 {
			t.Fatalf("got %+v, want {1 hello}", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestPublishAfterDone(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")
	s.Finish()

	if _, err := s.Publish("late"); !errors.Is(err, ErrStreamDone) {
		t.Fatalf("Publish after Finish: got %v, want ErrStreamDone", err)
	}
}

func TestSubscribeReplaysBufferedEvents(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")

	for i := 0; i < 3; i++ {
		if _, err := s.Publish("x"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	sub, gap := s.Subscribe(1, 0)
	if gap {
		t.Fatal("expected no gap: nothing evicted from the buffer yet")
	}

	var got []Event
	for i := 0; i < 2; i++ {
		select {
		case ev := <-sub.Events():
			got = append(got, ev)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for replay")
		}
	}
	if len(got) != 2 || got[0].ID != 2 || got[1].ID != 3 {
		t.Fatalf("replay = %+v, want ids 2 and 3", got)
	}
}

func TestSubscribeReportsGapAfterEviction(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")
	// capacity is 2, so publishing 3 events evicts the first.
	s.capacity = 2

	for i := 0; i < 3; i++ {
		if _, err := s.Publish("x"); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	sub, gap := s.Subscribe(0, 0)
	defer sub.Close()
	if !gap {
		t.Fatal("expected a gap: event 1 was evicted from the buffer")
	}
}

func TestSubscribeAfterFinishDrainsAndCloses(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")
	if _, err := s.Publish("only"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	s.Finish()

	sub, gap := s.Subscribe(0, 0)
	if gap {
		t.Fatal("expected no gap")
	}

	select {
	case ev, open := <-sub.Events():
		if !open || ev.ID != 1 {
			t.Fatalf("got %+v open=%v, want buffered event then still open", ev, open)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for buffered event")
	}

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected channel to be closed after drain of a finished stream")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}

	if err := sub.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestSlowSubscriberIsDroppedAsLagged(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")

	sub, _ := s.Subscribe(0, 1)
	// Fill the subscriber's buffer, then push one more so the next publish
	// finds it full and drops it.
	if _, err := s.Publish("one"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := s.Publish("two"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case _, open := <-sub.Events():
		if open {
			// Drain the buffered event, then wait for the close.
			select {
			case _, open := <-sub.Events():
				if open {
					t.Fatal("expected channel to close after lag drop")
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for lag close")
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event or close")
	}

	if err := sub.Err(); !errors.Is(err, ErrLagged) {
		t.Fatalf("Err() = %v, want ErrLagged", err)
	}
}

func TestSubscriptionCloseDetaches(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")

	sub, _ := s.Subscribe(0, 0)
	sub.Close()

	if got := s.Stats().Subscribers; got != 0 {
		t.Fatalf("Subscribers = %d, want 0 after Close", got)
	}

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected channel to be closed after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestHubGetOrCreateReturnsSameStream(t *testing.T) {
	h := New(0)
	a := h.GetOrCreate("x")
	b := h.GetOrCreate("x")
	if a != b {
		t.Fatal("GetOrCreate returned different streams for the same id")
	}
}

func TestHubRemove(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("x")
	sub, _ := s.Subscribe(0, 0)

	if ok := h.Remove("missing"); ok {
		t.Fatal("Remove(missing) = true, want false")
	}
	if ok := h.Remove("x"); !ok {
		t.Fatal("Remove(x) = false, want true")
	}
	if _, ok := h.Stream("x"); ok {
		t.Fatal("stream still present after Remove")
	}

	select {
	case _, open := <-sub.Events():
		if open {
			t.Fatal("expected subscriber channel to close after Remove")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestHubIDsSorted(t *testing.T) {
	h := New(0)
	h.GetOrCreate("b")
	h.GetOrCreate("a")
	h.GetOrCreate("c")

	ids := h.IDs()
	want := []string{"a", "b", "c"}
	if len(ids) != len(want) {
		t.Fatalf("IDs() = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("IDs() = %v, want %v", ids, want)
		}
	}
}

func TestHubCloseAllFinishesStreams(t *testing.T) {
	h := New(0)
	s1 := h.GetOrCreate("a")
	s2 := h.GetOrCreate("b")

	h.CloseAll()

	if !s1.Done() || !s2.Done() {
		t.Fatal("expected every stream to be done after CloseAll")
	}
}

func TestConcurrentPublishAssignsUniqueSequentialIDs(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")

	const goroutines = 20
	const perGoroutine = 50
	total := goroutines * perGoroutine

	var wg sync.WaitGroup
	ids := make(chan uint64, total)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				ev, err := s.Publish("x")
				if err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
				ids <- ev.ID
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool, total)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate event id %d from concurrent Publish", id)
		}
		seen[id] = true
	}
	if len(seen) != total {
		t.Fatalf("got %d unique ids, want %d", len(seen), total)
	}
	for id := uint64(1); id <= uint64(total); id++ {
		if !seen[id] {
			t.Fatalf("missing event id %d, ids should be contiguous", id)
		}
	}

	if stats := s.Stats(); stats.Events != uint64(total) {
		t.Fatalf("Stats().Events = %d, want %d", stats.Events, total)
	}
}

// TestConcurrentPublishSubscribeCloseUnderRace exercises Publish racing against
// Subscribe and Close from many goroutines. It relies on -race to catch data
// races in Stream's locking rather than asserting a specific delivery outcome,
// since a subscriber may legitimately be dropped for lagging or miss events
// published before it attached. Finish guarantees every subscriber channel
// closes eventually, so the test cannot hang.
func TestConcurrentPublishSubscribeCloseUnderRace(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")

	const publishers = 8
	const subscribers = 8
	const perPublisher = 100

	var publishWG sync.WaitGroup
	publishWG.Add(publishers)
	for i := 0; i < publishers; i++ {
		go func() {
			defer publishWG.Done()
			for j := 0; j < perPublisher; j++ {
				if _, err := s.Publish("x"); err != nil {
					return
				}
			}
		}()
	}

	var subWG sync.WaitGroup
	subWG.Add(subscribers)
	for i := 0; i < subscribers; i++ {
		go func() {
			defer subWG.Done()
			sub, _ := s.Subscribe(0, 4)
			defer sub.Close()
			for range sub.Events() {
				// drain until the channel closes, whether from lag or Finish
			}
		}()
	}

	publishWG.Wait()
	s.Finish()
	subWG.Wait()
}

func TestStatsReflectsPublishedEvents(t *testing.T) {
	h := New(0)
	s := h.GetOrCreate("a")
	if _, err := s.Publish("one"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := s.Publish("two"); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	stats := s.Stats()
	if stats.Events != 2 || stats.Buffered != 2 || stats.Done {
		t.Fatalf("Stats() = %+v, want Events=2 Buffered=2 Done=false", stats)
	}
}
