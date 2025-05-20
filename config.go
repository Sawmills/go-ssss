package ssss

import (
	"errors"
)

// Config represents the configuration for a SamplingSpaceSavingSets sketch
type Config struct {
	// MaxNumCounters is the maximum number of counters to keep
	MaxNumCounters int
	// Seeds are used for hashing
	Seeds []uint64
	// CardinalitySketchConfig is the configuration for the cardinality sketch
	CardinalitySketchConfig *HLLConfig
}

// NewConfig creates a new configuration for a SamplingSpaceSavingSets sketch
func NewConfig(
	maxNumCounters int,
	cardinalitySketchConfig *HLLConfig,
	seeds []uint64,
) (*Config, error) {
	if maxNumCounters == 0 {
		return nil, errors.New("max number of counters must be greater than zero")
	}

	// If no seeds are provided, generate random ones
	if seeds == nil {
		seeds = make([]uint64, 4)
		for i := range seeds {
			seeds[i] = secureRandomInt()
		}
	}

	return &Config{
		MaxNumCounters:          maxNumCounters,
		Seeds:                   seeds,
		CardinalitySketchConfig: cardinalitySketchConfig,
	}, nil
}
