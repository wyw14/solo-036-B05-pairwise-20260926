package tracker

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

// Concurrent writers record probe observations while concurrent readers query
// evidence and observation order. Run with -race: before SourceLineage was
// synchronized this reliably triggered "concurrent map read and map write".
func TestSourceLineageConcurrentReadWrite(t *testing.T) {
	lineage := NewSourceLineage()
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	const writers = 8
	const observationsPerWriter = 50

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			runID := fmt.Sprintf("run-%d", w%4)
			stationID := fmt.Sprintf("station-%d", w%2)
			for i := 0; i < observationsPerWriter; i++ {
				err := lineage.Add(SourceObservation{
					RunID:            runID,
					StationID:        stationID,
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

	readErrors := make(chan error, writers*observationsPerWriter)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			runID := fmt.Sprintf("run-%d", r%4)
			stationID := fmt.Sprintf("station-%d", r%2)
			for i := 0; i < observationsPerWriter; i++ {
				observations := lineage.Observations(runID, stationID, 1)
				if !sort.SliceIsSorted(observations, func(i, j int) bool {
					if observations[i].ObservedAt.Equal(observations[j].ObservedAt) {
						return observations[i].ObservationID < observations[j].ObservationID
					}
					return observations[i].ObservedAt.Before(observations[j].ObservedAt)
				}) {
					readErrors <- fmt.Errorf("observations for %s/%s are not in observation order", runID, stationID)
					return
				}
				evidence := lineage.Evidence(runID, stationID, 1)
				// Evidence is queried after Observations, and Add only ever
				// appends, so the later snapshot must see at least as many.
				if len(evidence) < len(observations) {
					readErrors <- fmt.Errorf("evidence count %d is behind earlier observation count %d", len(evidence), len(observations))
					return
				}
				_ = IndependentPhysicalArrival(evidence, 2)
			}
		}(r)
	}
	wg.Wait()
	close(readErrors)
	for err := range readErrors {
		t.Error(err)
	}

	// Every accepted observation must be queryable once writes quiesce.
	total := 0
	for w := 0; w < 4; w++ {
		for s := 0; s < 2; s++ {
			total += len(lineage.Observations(fmt.Sprintf("run-%d", w), fmt.Sprintf("station-%d", s), 1))
		}
	}
	if want := writers * observationsPerWriter; total != want {
		t.Errorf("queryable observations = %d, want %d", total, want)
	}
}

// Derived observations attach to a shared parent while readers query the
// lineage; the parent/child maps must stay consistent under concurrency.
func TestSourceLineageConcurrentDerivedObservations(t *testing.T) {
	lineage := NewSourceLineage()
	base := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

	root := SourceObservation{
		RunID:            "run-1",
		StationID:        "station-1",
		ObservationID:    "root",
		PhysicalSourceID: "source-1",
		Kind:             SourceMagnetic,
		Generation:       1,
		ObservedAt:       base,
		Valid:            true,
	}
	if err := lineage.Add(root); err != nil {
		t.Fatalf("add root observation: %v", err)
	}

	const children = 64
	var wg sync.WaitGroup
	for i := 0; i < children; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			derived := root
			derived.ObservationID = fmt.Sprintf("derived-%d", i)
			derived.ParentObservationID = "root"
			derived.Kind = SourceDerivedPosition
			derived.ObservedAt = base.Add(time.Duration(i+1) * time.Second)
			if err := lineage.Add(derived); err != nil {
				t.Errorf("add derived observation: %v", err)
			}
		}(i)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < children; i++ {
				_ = lineage.Evidence("run-1", "station-1", 1)
			}
		}()
	}
	wg.Wait()

	observations := lineage.Observations("run-1", "station-1", 1)
	if want := children + 1; len(observations) != want {
		t.Errorf("observations = %d, want %d", len(observations), want)
	}
}

func TestSourceLineageRejectsInvalidObservations(t *testing.T) {
	lineage := NewSourceLineage()
	valid := SourceObservation{
		RunID:            "run-1",
		StationID:        "station-1",
		ObservationID:    "obs-1",
		PhysicalSourceID: "source-1",
		Kind:             SourceAcoustic,
		Generation:       1,
		ObservedAt:       time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		Valid:            true,
	}
	if err := lineage.Add(valid); err != nil {
		t.Fatalf("add valid observation: %v", err)
	}
	if err := lineage.Add(valid); err == nil {
		t.Error("duplicate observation id was accepted")
	}
	missingParent := valid
	missingParent.ObservationID = "obs-2"
	missingParent.ParentObservationID = "does-not-exist"
	if err := lineage.Add(missingParent); err == nil {
		t.Error("observation with missing parent was accepted")
	}
	crossRun := valid
	crossRun.ObservationID = "obs-3"
	crossRun.ParentObservationID = "obs-1"
	crossRun.RunID = "run-2"
	if err := lineage.Add(crossRun); err == nil {
		t.Error("derived observation crossing runs was accepted")
	}
	if got := lineage.Evidence("run-1", "station-1", 1); len(got) != 1 {
		t.Errorf("evidence count = %d, want 1 (rejected observations must not be recorded)", len(got))
	}
	if !IndependentPhysicalArrival([]model.PassageEvidence{
		{RunID: "run-1", Station: "station-1", SourceID: "a/1", Generation: 1, At: valid.ObservedAt, Valid: true},
		{RunID: "run-1", Station: "station-1", SourceID: "b/1", Generation: 1, At: valid.ObservedAt, Valid: true},
	}, 2) {
		t.Error("independent physical sources were not recognized")
	}
}
