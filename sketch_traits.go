package ssss

// CardinalitySketch represents a sketch that estimates the cardinality of a set
type CardinalitySketch[T comparable] interface {
	// Insert adds an item to the sketch
	Insert(item T)

	// Merge combines this sketch with another sketch of the same type
	Merge(other CardinalitySketch[T]) error

	// Clear resets the sketch to its initial state
	Clear()

	// Cardinality returns the estimated cardinality of the set
	Cardinality() uint64
}

// HeavyDistinctHitterSketch represents a sketch that tracks labels with high cardinality sets
type HeavyDistinctHitterSketch[L comparable, T comparable] interface {
	// Insert adds an item to the set associated with the given label
	Insert(label L, item T)

	// Merge combines this sketch with another sketch of the same type
	Merge(other HeavyDistinctHitterSketch[L, T]) error

	// Clear resets the sketch to its initial state
	Clear()

	// Cardinality returns the estimated cardinality of the set associated with the given label
	Cardinality(label L) uint64

	// Top returns the k labels with the highest cardinality, along with their estimated cardinalities
	Top(k int) []LabelCount[L]
}

// LabelCount represents a label and its associated count
type LabelCount[L comparable] struct {
	Label L
	Count uint64
}
