package main

import (
	"fmt"

	"github.com/sawmills/go-ssss"
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

	// Get the top 5 labels
	top := sketch.Top(5)
	fmt.Println("Top 5 labels:")
	for _, entry := range top {
		fmt.Printf("Label: %d, Cardinality: %d\n", entry.Label, entry.Count)
	}

	// Create another sketch with different data
	sketch2 := ssss.NewHLLSamplingSpaceSavingSets[int, int](config)
	for label := 50; label <= 150; label += 10 {
		for item := 100; item < label+100; item++ {
			sketch2.Insert(label, item)
		}
	}

	// Merge the sketches
	err = sketch.Merge(sketch2)
	if err != nil {
		panic(err)
	}

	// Get the top 5 labels after merging
	top = sketch.Top(5)
	fmt.Println("\nTop 5 labels after merging:")
	for _, entry := range top {
		fmt.Printf("Label: %d, Cardinality: %d\n", entry.Label, entry.Count)
	}

	// Check the cardinality of a specific label
	label := 100
	cardinality := sketch.Cardinality(label)
	fmt.Printf("\nCardinality of label %d: %d\n", label, cardinality)
}
