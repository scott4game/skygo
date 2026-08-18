//go:build stress

package actor

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

const defaultStressSeed uint64 = 0x5a17c0de

func stressSeed(t *testing.T) uint64 {
	t.Helper()
	value := os.Getenv("SKYGO_STRESS_SEED")
	if value == "" {
		t.Logf("SKYGO_STRESS_SEED=%d", defaultStressSeed)
		return defaultStressSeed
	}
	seed, err := strconv.ParseUint(value, 0, 64)
	if err != nil {
		t.Fatalf("invalid SKYGO_STRESS_SEED %q: %v", value, err)
	}
	t.Logf("SKYGO_STRESS_SEED=%d", seed)
	return seed
}

func stressError(op string, worker, iteration int, err error) error {
	return fmt.Errorf("%s worker=%d iteration=%d: %w", op, worker, iteration, err)
}
