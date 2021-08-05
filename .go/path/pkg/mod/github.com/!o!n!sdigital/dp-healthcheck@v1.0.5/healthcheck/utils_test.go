package healthcheck

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestJitter(t *testing.T) {

	interval := 120 * time.Second
	timeRef := time.Now().UTC().Round(time.Hour)
	timeRefWithInterval := timeRef.Add(interval)

	Convey("check calcIntervalWithJitter is returning values in the expected range", t, func() {
		jitterMax := time.Duration(getMaxJitter(interval))
		So(jitterMax, ShouldBeGreaterThan, 0)

		for i := 1; i < 20; i++ {
			timeWithJitteredInterval := timeRef.Add(calcIntervalWithJitter(interval))
			So(timeWithJitteredInterval, ShouldHappenWithin, jitterMax, timeRefWithInterval)
		}
	})
}
