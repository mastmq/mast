// Package quota holds the arithmetic of tenant limits: a token bucket for a
// rate, and the census that adds up a tenant's connections across nodes.
//
// Neither reads a clock or touches a lock. The caller passes the time in and
// serialises access, which is what keeps both testable to the nanosecond and
// leaves the decision of how to share them to the code that knows how they
// are used.
package quota

import "time"

// Bucket is a token bucket: it holds up to burst tokens, refills at rate per
// second, and each admitted event takes one.
//
// The zero value admits nothing; build one with [NewBucket].
type Bucket struct {
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket. It starts full because a tenant that has
// sent nothing has not used its burst: starting empty would refuse the first
// publish of every connection on a node that had not seen the tenant yet.
func NewBucket(rate, burst float64, now time.Time) Bucket {
	return Bucket{rate: rate, burst: burst, tokens: burst, last: now}
}

// Allow takes one token if there is one, and reports whether it did.
func (b *Bucket) Allow(now time.Time) bool {
	b.refill(now)

	if b.tokens < 1 {
		return false
	}

	b.tokens--

	return true
}

// Reset changes the rate and burst, keeping what is left in the bucket up to
// the new burst. A limit raised by a later authentication takes effect at
// once; one lowered does not hand back a burst the tenant already spent.
func (b *Bucket) Reset(rate, burst float64, now time.Time) {
	b.refill(now)
	b.rate, b.burst = rate, burst
	b.tokens = min(b.tokens, burst)
}

// refill adds what the time since the last call earned. A clock that went
// backwards earns nothing rather than draining the bucket.
func (b *Bucket) refill(now time.Time) {
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}

	b.last = now
	b.tokens = min(b.burst, b.tokens+elapsed.Seconds()*b.rate)
}
