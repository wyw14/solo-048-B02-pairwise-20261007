package recovery

import (
	"errors"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/dispatch"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
)

func newHarness(t *testing.T, initial station.Station) (*Reconnector, *dispatch.Buffer, *station.Registry, *station.CommandService) {
	t.Helper()
	registry := station.NewRegistry([]station.Station{initial})
	commands := station.NewCommandService(registry)
	buffer := dispatch.NewBuffer()
	recorder := audit.NewRecorder(store.NewEventLog(t.TempDir() + "/events.jsonl"))
	return NewReconnector(buffer, registry, commands, recorder), buffer, registry, commands
}

func baseStation(epoch uint64, online bool) station.Station {
	return station.Station{
		ID: "s1", Name: "station", Online: online, ControllerEpoch: epoch,
		Exchangers: map[string]station.ExchangerPhase{},
	}
}

func TestReconnectAppliesBufferedCommandsInOrderAndComesOnline(t *testing.T) {
	reconnector, buffer, registry, _ := newHarness(t, baseStation(0, false))
	// Enqueued out of order: replay must still apply seq 1, 2, 3.
	for _, seq := range []uint64{3, 1, 2} {
		if _, err := buffer.Enqueue("s1", 1, seq, float64(60+seq)); err != nil {
			t.Fatalf("enqueue seq %d: %v", seq, err)
		}
	}

	result, err := reconnector.Reconnect("s1", 1)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if !result.Online {
		t.Fatal("result must report online after clean replay")
	}
	if len(result.Applied) != 3 || len(result.Rejected) != 0 || result.Pending != 0 {
		t.Fatalf("unexpected replay result: %+v", result)
	}
	for i, command := range result.Applied {
		if command.Sequence != uint64(i+1) || command.Status != "applied" {
			t.Fatalf("applied commands out of order: %+v", result.Applied)
		}
	}
	state, _ := registry.Get("s1")
	if !state.Online {
		t.Fatal("station must be online after clean replay")
	}
	if pending := buffer.Pending("s1"); len(pending) != 0 {
		t.Fatalf("buffer must be drained, got %d pending", len(pending))
	}
}

func TestReconnectStaleEpochStaysOfflineAndKeepsBuffer(t *testing.T) {
	// Station already knows epoch 5 and is online; a peer reconnects claiming
	// epoch 3. Nothing may be replayed or retired and online must be dropped.
	reconnector, buffer, registry, _ := newHarness(t, baseStation(5, true))
	// Command queued by the stale peer at epoch 3 while the station is at 5.
	if _, err := buffer.Enqueue("s1", 3, 1, 61); err != nil {
		t.Fatal(err)
	}

	result, err := reconnector.Reconnect("s1", 3)
	if !errors.Is(err, ErrReplayIncomplete) {
		t.Fatalf("want ErrReplayIncomplete, got %v", err)
	}
	if result.Online {
		t.Fatal("stale reconnect must not report online")
	}
	if len(result.Applied) != 0 {
		t.Fatalf("stale reconnect must not apply commands: %+v", result.Applied)
	}
	if result.Pending != 1 {
		t.Fatalf("want 1 pending command reported, got %d", result.Pending)
	}
	state, _ := registry.Get("s1")
	if state.Online {
		t.Fatal("station must be forced offline after stale reconnect")
	}
	if pending := buffer.Pending("s1"); len(pending) != 1 {
		t.Fatalf("buffered commands must be preserved for retry, got %d", len(pending))
	}

	// Retry at the current epoch retires the stale command instead of
	// applying it, and still must not fake being online.
	result, err = reconnector.Reconnect("s1", 5)
	if !errors.Is(err, ErrReplayIncomplete) {
		t.Fatalf("retry with retired commands must fail, got %v", err)
	}
	if result.Online || len(result.Rejected) != 1 || result.Rejected[0].Status != "rejected_old_epoch" {
		t.Fatalf("old-epoch command must be rejected: %+v", result)
	}
	state, _ = registry.Get("s1")
	if state.Online {
		t.Fatal("station with retired commands must stay offline")
	}
}

func TestReconnectOldEpochRejectsButAppliesCurrentCommands(t *testing.T) {
	reconnector, buffer, registry, _ := newHarness(t, baseStation(0, false))
	// A reconnect previously announced epoch 2.
	buffer.AcceptEpoch("s1", 2)
	if _, err := buffer.Enqueue("s1", 1, 1, 60); err != nil { // retired
		t.Fatal(err)
	}
	if _, err := buffer.Enqueue("s1", 2, 1, 62); err != nil { // ready
		t.Fatal(err)
	}

	result, err := reconnector.Reconnect("s1", 2)
	if !errors.Is(err, ErrReplayIncomplete) {
		t.Fatalf("want ErrReplayIncomplete, got %v", err)
	}
	if result.Online {
		t.Fatal("replay with retired commands must not report online")
	}
	if len(result.Applied) != 1 || result.Applied[0].Epoch != 2 {
		t.Fatalf("current-epoch command should still be applied: %+v", result.Applied)
	}
	if len(result.Rejected) != 1 || result.Rejected[0].Epoch != 1 {
		t.Fatalf("old-epoch command should be rejected: %+v", result.Rejected)
	}
	state, _ := registry.Get("s1")
	if state.Online {
		t.Fatal("station must remain offline")
	}
	if state.TargetCelsius != 62 {
		t.Fatalf("current command target not applied: %f", state.TargetCelsius)
	}
}

func TestReconnectAfterRetiredCommandsSucceedsWithEmptyBuffer(t *testing.T) {
	reconnector, buffer, _, _ := newHarness(t, baseStation(0, false))
	buffer.AcceptEpoch("s1", 2)
	if _, err := buffer.Enqueue("s1", 1, 1, 60); err != nil {
		t.Fatal(err)
	}
	if _, err := reconnector.Reconnect("s1", 2); !errors.Is(err, ErrReplayIncomplete) {
		t.Fatalf("first reconnect should fail, got %v", err)
	}

	// Controller reconnects again with no buffered commands left: clean.
	result, err := reconnector.Reconnect("s1", 2)
	if err != nil {
		t.Fatalf("reconnect with empty buffer should succeed: %v", err)
	}
	if !result.Online {
		t.Fatal("station should be online once nothing is unresolved")
	}
}

func TestBufferRequeuePreservesOrderForRetry(t *testing.T) {
	buffer := dispatch.NewBuffer()
	if _, err := buffer.Enqueue("s1", 1, 3, 63); err != nil {
		t.Fatal(err)
	}
	ready, _ := buffer.Drain("s1", 1)
	if len(ready) != 1 {
		t.Fatalf("drain: %d", len(ready))
	}
	// Simulate a rejected replay: untouched suffix goes back in original order.
	buffer.Requeue("s1", ready)
	if pending := buffer.Pending("s1"); len(pending) != 1 || pending[0].Sequence != 3 {
		t.Fatalf("requeued command must retain its position: %+v", pending)
	}
}
