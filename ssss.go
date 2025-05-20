package ssss

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"sort"
)

// SamplingSpaceSavingSets implements the HeavyDistinctHitterSketch interface
type SamplingSpaceSavingSets[L comparable, T comparable] struct {
	config    *Config
	counters  map[L]*CachedSketch[T]
	threshold uint64
}

// NewSamplingSpaceSavingSets creates a new SamplingSpaceSavingSets sketch
func NewSamplingSpaceSavingSets[L comparable, T comparable](
	config *Config,
) *SamplingSpaceSavingSets[L, T] {
	return &SamplingSpaceSavingSets[L, T]{
		config:    config,
		counters:  make(map[L]*CachedSketch[T], config.MaxNumCounters),
		threshold: 0,
	}
}

// NewHLLSamplingSpaceSavingSets creates a new SamplingSpaceSavingSets sketch with HyperLogLog as the cardinality sketch
func NewHLLSamplingSpaceSavingSets[L comparable, T comparable](
	config *Config,
) *SamplingSpaceSavingSets[L, T] {
	return NewSamplingSpaceSavingSets[L, T](config)
}

// Insert adds an item to the set associated with the given label
func (s *SamplingSpaceSavingSets[L, T]) Insert(label L, item T) {
	// If the counter for the label exists, use it
	if counter, exists := s.counters[label]; exists {
		counter.Insert(item)
		return
	}

	// If we have space, create a new counter
	if len(s.counters) < s.config.MaxNumCounters {
		hll := NewHyperLogLog[T](s.config.CardinalitySketchConfig)
		counter := NewCachedSketch[T](hll)
		s.counters[label] = counter
		counter.Insert(item)
		return
	}

	// Otherwise, use the sampling strategy
	cardinalityEstimate := s.cardinalityEstimate(label, item)

	// Only consider labels with estimated cardinality above the threshold
	if cardinalityEstimate > s.threshold {
		// Find the counter with the minimum cardinality
		var minLabel L
		var minCardinality uint64 = math.MaxUint64

		for l, c := range s.counters {
			cardinality := c.Cardinality()
			if cardinality < minCardinality {
				minLabel = l
				minCardinality = cardinality
			}
		}

		// Set threshold to min cardinality
		s.threshold = minCardinality

		// If the estimated cardinality is greater than the minimum cardinality,
		// replace the minimum counter with a new one for the label
		if cardinalityEstimate > minCardinality {
			// Remove the counter with the minimum cardinality
			minCounter := s.counters[minLabel]
			delete(s.counters, minLabel)

			// Reset the counter
			minCounter.Clear()

			// Map the counter to the new label
			s.counters[label] = minCounter

			// Insert the item
			minCounter.Insert(item)
		}
	}
}

// Merge combines this sketch with another sketch of the same type
func (s *SamplingSpaceSavingSets[L, T]) Merge(other HeavyDistinctHitterSketch[L, T]) error {
	otherSSS, ok := other.(*SamplingSpaceSavingSets[L, T])
	if !ok {
		return errors.New("can only merge with another SamplingSpaceSavingSets")
	}

	// Check if configs match
	if s.config.MaxNumCounters != otherSSS.config.MaxNumCounters ||
		len(s.config.Seeds) != len(otherSSS.config.Seeds) {
		return errors.New("config mismatch")
	}

	for i := range s.config.Seeds {
		if s.config.Seeds[i] != otherSSS.config.Seeds[i] {
			return errors.New("config mismatch: different seeds")
		}
	}

	// Check if HLL configs match
	if s.config.CardinalitySketchConfig.NumRegisters != otherSSS.config.CardinalitySketchConfig.NumRegisters {
		return errors.New("config mismatch: different HLL register count")
	}

	// Merge the two sets of counters
	for label, counter := range otherSSS.counters {
		if existingCounter, exists := s.counters[label]; exists {
			// If the counter already exists, merge it
			err := existingCounter.Merge(counter)
			if err != nil {
				return err
			}
		} else {
			// Otherwise, create a new counter
			hll := NewHyperLogLog[T](s.config.CardinalitySketchConfig)
			newCounter := NewCachedSketch[T](hll)
			err := newCounter.Merge(counter)
			if err != nil {
				return err
			}
			s.counters[label] = newCounter
		}
	}

	// Only keep the top MaxNumCounters counters
	if len(s.counters) > s.config.MaxNumCounters {
		var entries []LabelCount[L]
		for label, counter := range s.counters {
			entries = append(entries, LabelCount[L]{
				Label: label,
				Count: counter.Cardinality(),
			})
		}

		// Sort by cardinality in descending order
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Count > entries[j].Count
		})

		// Keep only the top MaxNumCounters entries
		for _, entry := range entries[s.config.MaxNumCounters:] {
			delete(s.counters, entry.Label)
		}
	}

	// Update the threshold to the minimum cardinality
	s.threshold = math.MaxUint64
	for _, counter := range s.counters {
		cardinality := counter.Cardinality()
		if cardinality < s.threshold {
			s.threshold = cardinality
		}
	}

	// If there are no counters, reset the threshold
	if len(s.counters) == 0 {
		s.threshold = 0
	}

	return nil
}

// Clear resets the sketch to its initial state
func (s *SamplingSpaceSavingSets[L, T]) Clear() {
	s.counters = make(map[L]*CachedSketch[T], s.config.MaxNumCounters)
	s.threshold = 0
}

// Cardinality returns the estimated cardinality of the set associated with the given label
func (s *SamplingSpaceSavingSets[L, T]) Cardinality(label L) uint64 {
	if counter, exists := s.counters[label]; exists {
		return counter.Cardinality()
	}

	// If the label doesn't exist, return the minimum cardinality or 0
	minCardinality := uint64(0)
	if len(s.counters) > 0 {
		minCardinality = math.MaxUint64
		for _, counter := range s.counters {
			cardinality := counter.Cardinality()
			if cardinality < minCardinality {
				minCardinality = cardinality
			}
		}
	}

	return minCardinality
}

// Top returns the k labels with the highest cardinality, along with their estimated cardinalities
func (s *SamplingSpaceSavingSets[L, T]) Top(k int) []LabelCount[L] {
	var entries []LabelCount[L]
	for label, counter := range s.counters {
		entries = append(entries, LabelCount[L]{
			Label: label,
			Count: counter.Cardinality(),
		})
	}

	// Sort by cardinality in descending order
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Count > entries[j].Count
	})

	// Return the top k entries
	if k < len(entries) {
		return entries[:k]
	}

	return entries
}

// cardinalityEstimate estimates the cardinality of a set based on the hash of an item
func (s *SamplingSpaceSavingSets[L, T]) cardinalityEstimate(_ L, item T) uint64 {
	// Create a hash of the item
	hasher := fnv.New64a()
	fmt.Fprintf(hasher, "%v", item)
	itemHash := hasher.Sum64()

	// Use all available seeds and average the estimates
	var totalEstimate uint64
	seedCount := len(s.config.Seeds)

	if seedCount == 0 {
		// Fallback if no seeds are provided
		trailingZeros := uint64(bits.TrailingZeros64(itemHash))
		if trailingZeros >= 64 {
			return math.MaxUint64 // Avoid overflow
		}
		return uint64(1) << trailingZeros
	}

	for _, seed := range s.config.Seeds {
		// Mix with the seed
		seedHash := itemHash ^ seed

		// Count the number of trailing zeros in the hash
		trailingZeros := uint64(bits.TrailingZeros64(seedHash))

		// Estimate cardinality as 2^(trailing zeros)
		// This is based on the HyperLogLog algorithm's insight that the
		// probability of seeing a hash with n trailing zeros is 2^(-n)
		// So if we see a hash with n trailing zeros, we estimate the cardinality as 2^n
		if trailingZeros >= 64 {
			totalEstimate += math.MaxUint64 // Avoid overflow
		} else {
			totalEstimate += uint64(1) << trailingZeros
		}
	}

	// Return the average estimate
	return totalEstimate / uint64(seedCount)
}
