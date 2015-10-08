package dsync

import (
	"math"
	"time"
)

type BackoffFunc func(int64) time.Duration

// ConstantBackoff generates a simple back-off strategy of a static backoff duration
func ConstantBackoff(d time.Duration) BackoffFunc {
	return func(int64) time.Duration {
		return d
	}
}

// ExponentialBackoff generates a simple back-off strategy of doubling the backoff duration
// per each iteration. (duration * 2^n)
func ExponentialBackoff(d time.Duration) BackoffFunc {
	return func(n int64) time.Duration {
		multiplier := int64(math.Pow(float64(2), float64(n)))
		return d * time.Duration(multiplier)
	}
}
