package recovery_test

import (
	"path/filepath"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/dispatch"
	"heatpulse/internal/recovery"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
)

func newFixture(t *testing.T, stations ...station.Station) (*recovery.Reconnector, *dispatch.Buffer, *station.Registry) {
	t.Helper()
	registry := station.NewRegistry(stations)
	buffer := dispatch.NewBuffer()
	commands := station.NewCommandService(registry)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(t.TempDir(), "events.jsonl")))
	return recovery.NewReconnector(buffer, registry, commands, recorder), buffer, registry
}

func online(t *testing.T, registry *station.Registry, stationID string) bool {
	t.Helper()
	state, ok := registry.Get(stationID)
	if !ok {
		t.Fatalf("station %s not found", stationID)
	}
	return state.Online
}

func TestReconnectCleanReplaySetsOnline(t *testing.T) {
	reconnector, buffer, registry := newFixture(t, station.Station{ID: "st-1", Online: false})

	if _, err := buffer.Enqueue("st-1", 5, 2, 42.0); err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Enqueue("st-1", 5, 1, 41.5); err != nil {
		t.Fatal(err)
	}

	result, err := reconnector.Reconnect("st-1", 5)
	if err != nil {
		t.Fatalf("clean replay returned error: %v", err)
	}
	if len(result.Applied) != 2 || len(result.Rejected) != 0 {
		t.Fatalf("applied=%d rejected=%d, want 2/0", len(result.Applied), len(result.Rejected))
	}
	// Drain replays in (epoch, sequence) order even though enqueue was reversed.
	if result.Applied[0].Sequence != 1 || result.Applied[1].Sequence != 2 {
		t.Fatalf("applied out of order: seq %d then %d", result.Applied[0].Sequence, result.Applied[1].Sequence)
	}
	if !online(t, registry, "st-1") {
		t.Fatal("station must be online after a clean replay")
	}
	if pending := buffer.Pending("st-1"); len(pending) != 0 {
		t.Fatalf("buffer must be empty after clean replay, got %d pending", len(pending))
	}
}

func TestReconnectRejectedCommandsStayOfflineAndRetryInOrder(t *testing.T) {
	reconnector, buffer, registry := newFixture(t, station.Station{ID: "st-1", Online: false, ControllerEpoch: 10})

	first, err := buffer.Enqueue("st-1", 8, 1, 40.0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buffer.Enqueue("st-1", 8, 2, 40.5)
	if err != nil {
		t.Fatal(err)
	}

	// Controller is ahead of the buffered epoch: both commands are refused.
	result, err := reconnector.Reconnect("st-1", 8)
	if err == nil {
		t.Fatal("rejected replay must return an error")
	}
	if len(result.Applied) != 0 || len(result.Rejected) != 2 {
		t.Fatalf("applied=%d rejected=%d, want 0/2", len(result.Applied), len(result.Rejected))
	}
	if online(t, registry, "st-1") {
		t.Fatal("station must not report online while commands are unhandled")
	}

	// Unhandled commands are requeued with their identity and order intact.
	pending := buffer.Pending("st-1")
	if len(pending) != 2 {
		t.Fatalf("expected 2 requeued commands, got %d", len(pending))
	}
	if pending[0].CommandID != first.CommandID || pending[1].CommandID != second.CommandID {
		t.Fatalf("retry order not preserved: got %s, %s", pending[0].CommandID, pending[1].CommandID)
	}

	// Once the controller accepts the epoch, the retry applies in order.
	if _, err := registry.Update("st-1", func(item *station.Station) error {
		item.ControllerEpoch = 7
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	retry, err := reconnector.Reconnect("st-1", 8)
	if err != nil {
		t.Fatalf("retry after controller caught up returned error: %v", err)
	}
	if len(retry.Applied) != 2 || retry.Applied[0].Sequence != 1 || retry.Applied[1].Sequence != 2 {
		t.Fatalf("retry applied out of order: %+v", retry.Applied)
	}
	if !online(t, registry, "st-1") {
		t.Fatal("station must be online after the requeued commands apply")
	}
	if pending := buffer.Pending("st-1"); len(pending) != 0 {
		t.Fatalf("buffer must be drained after successful retry, got %d pending", len(pending))
	}
}

func TestReconnectOldEpochCommandsBlockOnline(t *testing.T) {
	reconnector, buffer, registry := newFixture(t, station.Station{ID: "st-1", Online: false})

	if _, err := buffer.Enqueue("st-1", 3, 1, 39.0); err != nil {
		t.Fatal(err)
	}

	// The fence moves past the buffered epoch: the command is retired as stale.
	result, err := reconnector.Reconnect("st-1", 5)
	if err == nil {
		t.Fatal("replay with old-epoch commands must return an error")
	}
	if len(result.Rejected) != 1 || result.Rejected[0].Status != "rejected_old_epoch" {
		t.Fatalf("expected one rejected_old_epoch command, got %+v", result.Rejected)
	}
	if online(t, registry, "st-1") {
		t.Fatal("station must not report online while old-epoch commands exist")
	}
	// Retired commands are stale for good: dropped, not requeued.
	if pending := buffer.Pending("st-1"); len(pending) != 0 {
		t.Fatalf("retired commands must not be requeued, got %d pending", len(pending))
	}

	// A subsequent clean reconnect reconciles and reports online.
	if _, err := reconnector.Reconnect("st-1", 5); err != nil {
		t.Fatalf("clean reconnect after retirement returned error: %v", err)
	}
	if !online(t, registry, "st-1") {
		t.Fatal("station must be online once the backlog is reconciled")
	}
}
