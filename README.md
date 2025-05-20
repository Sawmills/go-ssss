# Sampling Space-Saving Sets (SSSS) in Go

This is a Go implementation of the Sampling Space-Saving Sets algorithm, a high-performance fixed-size mergeable "distinct heavy-hitter sketch" that takes streaming (label, item) pairs.

## Overview

The sketch holds a fixed number of labels and corresponding cardinality sketches (HyperLogLog). It efficiently tracks labels with high cardinality sets in a streaming context.

Key features:

* Fixed memory usage
* Mergeable sketches
* Efficient sampling strategy
* Generic implementation using Go generics

## How It Works

If a label exists in the sketch, the item is added to the cardinality sketch associated with the label. If the label does not exist in the sketch, it samples in two stages:

1. The cardinality of the label's set is crudely estimated by taking the hash value of the item.
2. This estimate is compared to the threshold value kept by the sketch.
3. If the estimate is greater, it is then compared to the minimum cardinality in the sketch.
4. Only if the input is greater than the minimum cardinality do we drop the minimum label and replace it with the new label.

This has nice effects:

1. It prevents small cardinality labels from always replacing the bottom of the sketch and lessens the churn.
2. Because we're not even considering any input that doesn't pass the initial threshold, we don't have to calculate the minimum cardinality on every input, which improves the speed considerably.

## Usage

```go
package main

import (
    "fmt"
    "github.com/user/go-ssss"
)

func main() {
    // Create a new HyperLogLog configuration
    hllConfig, err := ssss.NewHLLConfig(256, nil)
    if err != nil {
        panic(err)
    }

    // Create a new SamplingSpaceSavingSets configuration
    config, err := ssss.NewConfig(10, hllConfig, nil)
    if err != nil {
        panic(err)
    }

    // Create a new SamplingSpaceSavingSets sketch
    sketch := ssss.NewHLLSamplingSpaceSavingSets[int, int](config)

    // Insert items into the sketch
    for label := 10; label <= 100; label += 10 {
        for item := 0; item < label; item++ {
            sketch.Insert(label, item)
        }
    }

    // Get the top 2 labels
    top := sketch.Top(2)
    for _, entry := range top {
        fmt.Printf("Label: %d, Cardinality: %d\n", entry.Label, entry.Count)
    }
}
```

## Requirements

* Go 1.18+ (for generics support)

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.

## Acknowledgments

This implementation is based on the Rust implementation from the paper "Sampling Space-Saving Set Sketches".
