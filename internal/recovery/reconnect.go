package recovery

import (
	"errors"
	"fmt"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/dispatch"
	"heatpulse/internal/station"
)

// ErrReplayIncomplete signals that a reconnect could not fully reconcile the
// station: buffered commands remain unapplied (rejected by the controller or
// carrying an epoch older than the station's current one). The station must not
// be reported online in that case, and the untouched commands stay buffered in
// their original order for the next replay.
var ErrReplayIncomplete = errors.New("recovery replay incomplete")

type ReplayResult struct {
	StationID string            `json:"station_id"`
	Epoch     uint64            `json:"epoch"`
	Online    bool              `json:"online"`
	Applied   []station.Command `json:"applied"`
	Rejected  []station.Command `json:"rejected"`
	Pending   int               `json:"pending"`
	At        time.Time         `json:"at"`
}

type Reconnector struct {
	buffer   *dispatch.Buffer
	stations *station.Registry
	commands *station.CommandService
	audit    *audit.Recorder
}

func NewReconnector(buffer *dispatch.Buffer, stations *station.Registry, commands *station.CommandService, recorder *audit.Recorder) *Reconnector {
	return &Reconnector{buffer: buffer, stations: stations, commands: commands, audit: recorder}
}

func (r *Reconnector) Reconnect(stationID string, currentEpoch uint64) (ReplayResult, error) {
	stationState, ok := r.stations.Get(stationID)
	if !ok {
		return ReplayResult{}, fmt.Errorf("station %s not found", stationID)
	}
	result := ReplayResult{StationID: stationID, Epoch: currentEpoch, At: time.Now().UTC()}

	// The station already knows a newer controller epoch: this reconnect is
	// stale and must not retire or replay anything against it.
	if currentEpoch < stationState.ControllerEpoch {
		result.Pending = len(r.buffer.Pending(stationID))
		return r.fail(result, fmt.Errorf("%w: reconnect epoch %d is older than station epoch %d",
			ErrReplayIncomplete, currentEpoch, stationState.ControllerEpoch))
	}

	r.buffer.AcceptEpoch(stationID, currentEpoch)
	ready, retired := r.buffer.Drain(stationID, currentEpoch)
	for _, buffered := range retired {
		result.Rejected = append(result.Rejected, station.Command{
			ID: buffered.CommandID, StationID: buffered.StationID, Epoch: buffered.Epoch,
			Sequence: buffered.Sequence, Celsius: buffered.Celsius, Status: "rejected_old_epoch", At: buffered.QueuedAt,
		})
	}
	// Replay current-epoch commands in epoch/sequence order. A controller
	// rejection blocks every later command: the rejected command and its
	// untouched suffix go back to the buffer in their original order for the
	// next replay. Old-epoch rejections do not block newer commands from
	// being applied, but still keep the station offline (checked below).
	for index, buffered := range ready {
		command, err := r.commands.Apply(station.Command{
			ID: buffered.CommandID, StationID: stationID, Epoch: buffered.Epoch,
			Sequence: buffered.Sequence, Celsius: buffered.Celsius, At: buffered.QueuedAt,
		})
		if err != nil {
			if command.ID != "" {
				result.Rejected = append(result.Rejected, command)
			}
			r.buffer.Requeue(stationID, ready[index:])
			result.Pending = len(ready) - index
			return r.fail(result, fmt.Errorf("%w: command %s (epoch %d, sequence %d) rejected: %v",
				ErrReplayIncomplete, buffered.CommandID, buffered.Epoch, buffered.Sequence, err))
		}
		result.Applied = append(result.Applied, command)
	}

	// Old-epoch commands were retired rather than reconciled: the replay is
	// not clean, so the station must not be reported online.
	if len(retired) > 0 {
		return r.fail(result, fmt.Errorf("%w: %d command(s) carried an older epoch", ErrReplayIncomplete, len(retired)))
	}

	if _, err := r.stations.SetOnline(stationID, true); err != nil {
		return result, err
	}
	result.Online = true
	if err := r.audit.Accepted("recovery.reconnect", stationID, "recovery", "station replay reconciled", map[string]any{"epoch": currentEpoch, "applied": len(result.Applied), "rejected": len(result.Rejected)}); err != nil {
		return result, err
	}
	return result, nil
}

// fail records the rejected replay and forces the station offline so it can
// never be falsely reported as online while commands remain unresolved.
func (r *Reconnector) fail(result ReplayResult, cause error) (ReplayResult, error) {
	if _, err := r.stations.SetOnline(result.StationID, false); err != nil {
		return result, err
	}
	details := map[string]any{
		"epoch":    result.Epoch,
		"applied":  len(result.Applied),
		"rejected": len(result.Rejected),
		"pending":  result.Pending,
		"reason":   cause.Error(),
	}
	if err := r.audit.Rejected("recovery.reconnect", result.StationID, "recovery", "station replay incomplete", details); err != nil {
		return result, err
	}
	return result, cause
}
