package ssss

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

// HLLConfig represents the configuration for a HyperLogLog sketch
type HLLConfig struct {
	// NumRegisters is the number of registers in the sketch
	NumRegisters int
	// Alpha is the bias correction factor
	Alpha float64
	// Seeds are used for hashing
	Seeds []uint64
}

func secureRandomInt() uint64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err) // or handle error appropriately
	}
	return binary.LittleEndian.Uint64(b[:])
}

// NewHLLConfig creates a new HyperLogLog configuration
func NewHLLConfig(numRegisters int, seeds []uint64) (*HLLConfig, error) {
	if numRegisters == 0 {
		return nil, errors.New("number of registers must be greater than zero")
	}

	if numRegisters&(numRegisters-1) != 0 {
		return nil, errors.New("number of registers must be a power of 2")
	}

	// If no seeds are provided, generate random ones
	if seeds == nil {
		seeds = make([]uint64, 8)
		for i := range seeds {
			seeds[i] = secureRandomInt()
		}
	}

	// Calculate alpha based on the number of registers
	var alpha float64
	switch {
	case numRegisters == 16:
		alpha = 0.673
	case numRegisters == 32:
		alpha = 0.697
	case numRegisters == 64:
		alpha = 0.709
	default:
		alpha = 0.7213 / (1.0 + 1.079/float64(numRegisters))
	}

	return &HLLConfig{
		NumRegisters: numRegisters,
		Alpha:        alpha,
		Seeds:        seeds,
	}, nil
}

// HyperLogLog implements the CardinalitySketch interface
type HyperLogLog[T comparable] struct {
	config           *HLLConfig
	registers        []byte
	numZeroRegisters int
	zInv             float64
}

// NewHyperLogLog creates a new HyperLogLog sketch
func NewHyperLogLog[T comparable](config *HLLConfig) *HyperLogLog[T] {
	registers := make([]byte, config.NumRegisters)
	return &HyperLogLog[T]{
		config:           config,
		registers:        registers,
		numZeroRegisters: config.NumRegisters,
		zInv:             float64(config.NumRegisters),
	}
}

// Insert adds an item to the sketch
func (h *HyperLogLog[T]) Insert(item T) {
	hash := h.hashItem(item)
	h.insertHash(hash)
}

// Merge combines this sketch with another sketch of the same type
func (h *HyperLogLog[T]) Merge(other CardinalitySketch[T]) error {
	otherHLL, ok := other.(*HyperLogLog[T])
	if !ok {
		return errors.New("can only merge with another HyperLogLog")
	}

	if h.config.NumRegisters != otherHLL.config.NumRegisters {
		return errors.New("config mismatch: different number of registers")
	}

	h.numZeroRegisters = 0
	h.zInv = 0

	for i := 0; i < h.config.NumRegisters; i++ {
		if otherHLL.registers[i] > h.registers[i] {
			h.registers[i] = otherHLL.registers[i]
		}

		if h.registers[i] == 0 {
			h.numZeroRegisters++
		}

		h.zInv += math.Pow(2.0, -float64(h.registers[i]))
	}

	return nil
}

// Clear resets the sketch to its initial state
func (h *HyperLogLog[T]) Clear() {
	for i := range h.registers {
		h.registers[i] = 0
	}
	h.numZeroRegisters = h.config.NumRegisters
	h.zInv = float64(h.config.NumRegisters)
}

// Cardinality returns the estimated cardinality of the set
func (h *HyperLogLog[T]) Cardinality() uint64 {
	estimate := uint64(
		float64(h.config.NumRegisters*h.config.NumRegisters) * h.config.Alpha / h.zInv,
	)

	// Small range correction
	if estimate <= 5*uint64(h.config.NumRegisters>>1) {
		if h.numZeroRegisters > 0 {
			estimate = uint64(h.linearCounting())
		}
	}

	// Large range correction not implemented

	return estimate
}

// linearCounting implements the linear counting algorithm for small cardinalities
func (h *HyperLogLog[T]) linearCounting() float64 {
	return float64(
		h.config.NumRegisters,
	) * math.Log(
		float64(h.config.NumRegisters)/float64(h.numZeroRegisters),
	)
}

// hashItem hashes an item and returns the hash value
func (h *HyperLogLog[T]) hashItem(item T) uint64 {
	// Create a hash of the item
	hasher := fnv.New64a()
	fmt.Fprintf(hasher, "%v", item)
	hash := hasher.Sum64()

	// Mix with one of the seeds
	hash ^= h.config.Seeds[1]

	return hash
}

// insertHash processes a hash value and updates the registers
func (h *HyperLogLog[T]) insertHash(hash uint64) {
	// Use the first few bits to determine the register index
	registerBits := uint(bits.Len(uint(h.config.NumRegisters - 1)))
	registerIdx := hash & ((1 << registerBits) - 1)

	// Count the number of leading zeros in the rest of the hash
	remainingHash := hash >> registerBits
	leadingZeros := uint8(bits.LeadingZeros64(remainingHash)) + 1

	if h.registers[registerIdx] < leadingZeros {
		if h.registers[registerIdx] == 0 {
			h.numZeroRegisters--
		}

		// Update zInv by removing the old value and adding the new one
		h.zInv -= math.Pow(2.0, -float64(h.registers[registerIdx]))
		h.zInv += math.Pow(2.0, -float64(leadingZeros))

		h.registers[registerIdx] = leadingZeros
	}
}
