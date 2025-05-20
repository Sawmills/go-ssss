package ssss

// CachedSketch wraps a CardinalitySketch and caches the cardinality value
type CachedSketch[T comparable] struct {
	sketch      CardinalitySketch[T]
	cardinality uint64
}

// NewCachedSketch creates a new cached sketch
func NewCachedSketch[T comparable](sketch CardinalitySketch[T]) *CachedSketch[T] {
	return &CachedSketch[T]{
		sketch:      sketch,
		cardinality: 0,
	}
}

// Insert adds an item to the sketch and updates the cached cardinality
func (c *CachedSketch[T]) Insert(item T) {
	c.sketch.Insert(item)
	c.cardinality = c.sketch.Cardinality()
}

// Merge combines this sketch with another sketch of the same type
func (c *CachedSketch[T]) Merge(other CardinalitySketch[T]) error {
	otherCached, ok := other.(*CachedSketch[T])
	if !ok {
		return c.sketch.Merge(other)
	}

	err := c.sketch.Merge(otherCached.sketch)
	if err != nil {
		return err
	}

	c.cardinality = c.sketch.Cardinality()
	return nil
}

// Clear resets the sketch to its initial state
func (c *CachedSketch[T]) Clear() {
	c.sketch.Clear()
	c.cardinality = 0
}

// Cardinality returns the cached cardinality value
func (c *CachedSketch[T]) Cardinality() uint64 {
	return c.cardinality
}
