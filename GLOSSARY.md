# Sampling Space-Saving Sets (SSSS)

Sampling Space-Saving Sets tracks labels with large distinct-item sets in a
stream of label and item pairs. The sketch keeps a bounded number of labels
and estimates the size of each retained set.

## Language

### Distinct counts

**Label**:
The group to which an observed item belongs. Each label has its own set of
distinct items.

**Item**:
A value observed for a label. Repeated observations of the same value
contribute one distinct item to that label's set.

**Cardinality**:
The number of distinct items in a set. The sketch reports estimated
cardinalities for tracked labels.

**Distinct Heavy Hitter**:
A label whose set of distinct items is among the largest in the stream.
_Avoid_: most frequent label

**Cardinality Sketch**:
A compact summary of observed items used to estimate a set's cardinality.
Sampling Space-Saving Sets keeps one summary for each tracked label.

**HyperLogLog**:
The cardinality sketch used for each tracked label. It estimates the number
of distinct items from their hashes.

### Label selection

**Label Capacity**:
The maximum number of labels the sketch retains. Once the sketch is full,
admitting a new label can replace a tracked label.

**Sampling Estimate**:
A rough estimate derived from the incoming item's hash for a label that is
outside the sketch. A full sketch uses it to evaluate the label for entry.

**Sampling Threshold**:
The stored cutoff a Sampling Estimate must exceed before the sketch checks
its current smallest tracked cardinality. That check refreshes the cutoff.

**Eviction**:
Removal of a tracked label to make room for another. During insertion, the
Sampling Estimate must exceed the stored threshold and the current smallest
tracked cardinality, and the replacement starts with the incoming item.

**Untracked Label**:
A label outside the sketch's current retained set. Its cardinality lookup
returns the smallest tracked estimate, or zero when the sketch is empty.

### Combining and reporting

**Sketch Merge**:
The combination of two sketches' retained summaries by label, keeping the
largest resulting estimates up to capacity. Merging checks for matching
label capacity, sampling seeds, and HyperLogLog register counts.

**Top-K Labels**:
Up to the requested number of tracked labels with the largest estimated
cardinalities, ordered from largest to smallest.
