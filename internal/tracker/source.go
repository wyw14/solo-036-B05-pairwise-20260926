package tracker

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wyw14/solo-036/internal/model"
)

type SourceKind string

const (
	SourcePressure        SourceKind = "pressure"
	SourceAcoustic        SourceKind = "acoustic"
	SourceMagnetic        SourceKind = "magnetic"
	SourceDerivedPosition SourceKind = "derived_position"
)

type SourceObservation struct {
	RunID               string
	StationID           string
	ObservationID       string
	PhysicalSourceID    string
	ParentObservationID string
	Kind                SourceKind
	Position            float64
	Generation          int64
	ObservedAt          time.Time
	Valid               bool
}

type SourceLineage struct {
	mu           sync.RWMutex
	observations map[string]SourceObservation
	children     map[string][]string
}

func NewSourceLineage() *SourceLineage {
	return &SourceLineage{
		observations: make(map[string]SourceObservation),
		children:     make(map[string][]string),
	}
}

func (l *SourceLineage) Add(observation SourceObservation) error {
	if strings.TrimSpace(observation.ObservationID) == "" {
		return errors.New("observation id is required")
	}
	if strings.TrimSpace(observation.PhysicalSourceID) == "" {
		return errors.New("physical source id is required")
	}
	if observation.RunID == "" || observation.StationID == "" {
		return errors.New("observation run and station are required")
	}
	if observation.Generation <= 0 {
		return errors.New("observation generation must be positive")
	}
	if observation.ObservedAt.IsZero() {
		return errors.New("observation time is required")
	}
	switch observation.Kind {
	case SourcePressure, SourceAcoustic, SourceMagnetic, SourceDerivedPosition:
	default:
		return errors.New("observation source kind is invalid")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.observations[observation.ObservationID]; exists {
		return errors.New("observation id already exists")
	}
	if observation.ParentObservationID != "" {
		parent, ok := l.observations[observation.ParentObservationID]
		if !ok {
			return errors.New("parent observation does not exist")
		}
		if parent.RunID != observation.RunID || parent.StationID != observation.StationID {
			return errors.New("derived observation crosses run or station")
		}
		if parent.PhysicalSourceID != observation.PhysicalSourceID {
			return errors.New("derived observation changes physical source")
		}
		if parent.Generation != observation.Generation {
			return errors.New("derived observation changes generation")
		}
		l.children[parent.ObservationID] = append(l.children[parent.ObservationID], observation.ObservationID)
	}
	l.observations[observation.ObservationID] = observation
	return nil
}

func (l *SourceLineage) Observations(runID, stationID string, generation int64) []SourceObservation {
	l.mu.RLock()
	out := make([]SourceObservation, 0)
	for _, observation := range l.observations {
		if observation.RunID == runID && observation.StationID == stationID && observation.Generation == generation {
			out = append(out, observation)
		}
	}
	l.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].ObservedAt.Equal(out[j].ObservedAt) {
			return out[i].ObservationID < out[j].ObservationID
		}
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	return out
}

func (l *SourceLineage) Evidence(runID, stationID string, generation int64) []model.PassageEvidence {
	observations := l.Observations(runID, stationID, generation)
	evidence := make([]model.PassageEvidence, 0, len(observations))
	for _, observation := range observations {
		evidence = append(evidence, model.PassageEvidence{
			RunID:      observation.RunID,
			Station:    observation.StationID,
			SourceID:   observation.PhysicalSourceID,
			Generation: observation.Generation,
			Position:   observation.Position,
			At:         observation.ObservedAt,
			Valid:      observation.Valid,
		})
	}
	return evidence
}

func IndependentPhysicalArrival(events []model.PassageEvidence, required int) bool {
	if required <= 0 {
		return false
	}
	sources := make(map[string]bool)
	for _, event := range events {
		if !model.ValidEvidence(event) {
			continue
		}
		physicalSource := strings.TrimSpace(strings.SplitN(event.SourceID, "/", 2)[0])
		if physicalSource != "" {
			sources[physicalSource] = true
		}
	}
	return len(sources) >= required
}
