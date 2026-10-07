package dispatch

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Buffer struct {
	mu       sync.Mutex
	commands map[string][]BufferedCommand
	accepted map[string]uint64
}

func NewBuffer() *Buffer {
	return &Buffer{commands: make(map[string][]BufferedCommand), accepted: make(map[string]uint64)}
}

func (b *Buffer) Enqueue(stationID string, epoch, sequence uint64, celsius float64) (BufferedCommand, error) {
	if stationID == "" || epoch == 0 || sequence == 0 {
		return BufferedCommand{}, fmt.Errorf("buffered command identity is incomplete")
	}
	command := BufferedCommand{
		StationID: stationID, CommandID: uuid.NewString(), Epoch: epoch,
		Sequence: sequence, Celsius: celsius, QueuedAt: time.Now().UTC(),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands[stationID] = append(b.commands[stationID], command)
	return command, nil
}

func (b *Buffer) AcceptEpoch(stationID string, epoch uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if epoch > b.accepted[stationID] {
		b.accepted[stationID] = epoch
	}
}

func (b *Buffer) Drain(stationID string, currentEpoch uint64) (ready, retired []BufferedCommand) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if currentEpoch > b.accepted[stationID] {
		b.accepted[stationID] = currentEpoch
	}
	fence := b.accepted[stationID]
	for _, command := range b.commands[stationID] {
		if command.Epoch < fence {
			retired = append(retired, command)
			continue
		}
		ready = append(ready, command)
	}
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].Epoch == ready[j].Epoch {
			return ready[i].Sequence < ready[j].Sequence
		}
		return ready[i].Epoch < ready[j].Epoch
	})
	delete(b.commands, stationID)
	return ready, retired
}

func (b *Buffer) Requeue(stationID string, commands []BufferedCommand) {
	if len(commands) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands[stationID] = append(b.commands[stationID], commands...)
}

func (b *Buffer) Pending(stationID string) []BufferedCommand {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]BufferedCommand(nil), b.commands[stationID]...)
}
