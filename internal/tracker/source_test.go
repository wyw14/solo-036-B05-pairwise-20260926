package tracker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Concurrent Add and Evidence/Observations calls must not race, and the
// resulting observation order must stay deterministic.
func TestSourceLineageConcurrentAccess(t *testing.T) {
	lineage := NewSourceLineage()
	base := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)

	const writers = 8
	const perWriter = 50

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				err := lineage.Add(SourceObservation{
					RunID:            "run-1",
					StationID:        "station-1",
					ObservationID:    fmt.Sprintf("obs-%d-%d", w, i),
					PhysicalSourceID: fmt.Sprintf("source-%d", w),
					Kind:             SourcePressure,
					Position:         float64(i),
					Generation:       1,
					ObservedAt:       base.Add(time.Duration(i) * time.Second),
					Valid:            true,
				})
				if err != nil {
					t.Errorf("add observation: %v", err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter*writers; i++ {
				_ = lineage.Observations("run-1", "station-1", 1)
				_ = lineage.Evidence("run-1", "station-1", 1)
			}
		}()
	}
	wg.Wait()

	observations := lineage.Observations("run-1", "station-1", 1)
	if len(observations) != writers*perWriter {
		t.Fatalf("expected %d observations, got %d", writers*perWriter, len(observations))
	}
	for i := 1; i < len(observations); i++ {
		prev, cur := observations[i-1], observations[i]
		if cur.ObservedAt.Before(prev.ObservedAt) {
			t.Fatalf("observations out of order at %d: %v before %v", i, cur.ObservedAt, prev.ObservedAt)
		}
		if cur.ObservedAt.Equal(prev.ObservedAt) && cur.ObservationID < prev.ObservationID {
			t.Fatalf("observation id tie-break out of order at %d", i)
		}
	}

	evidence := lineage.Evidence("run-1", "station-1", 1)
	if !IndependentPhysicalArrival(evidence, writers) {
		t.Fatalf("expected %d independent physical sources to satisfy quorum", writers)
	}
}
