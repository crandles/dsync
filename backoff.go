package dsync

import (
	"math"
	"time"
)

// A BackoffFunc determines the backoff strategy for the associated remote call,
// given a step 'n', the function should return the duration to sleep before
// performing the next remote call.
type BackoffFunc func(n int64) time.Duration

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
