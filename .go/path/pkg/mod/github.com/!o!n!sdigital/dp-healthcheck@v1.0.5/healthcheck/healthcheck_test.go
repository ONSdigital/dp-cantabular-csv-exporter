package healthcheck

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	criticalTimeout = 15 * time.Second
	interval        = 100 * time.Millisecond
)

var version = VersionInfo{
	BuildTime:       time.Unix(0, 0),
	GitCommit:       "d6cd1e2bd19e03a81132a23b2025920577f84e37",
	Language:        language,
	LanguageVersion: "1.12",
	Version:         "1.0.0",
}

func generateTestState(msg string) CheckState {
	previousTime := time.Unix(0, 0).UTC()
	currentTime := previousTime.Add(time.Duration(30) * time.Minute)
	return CheckState{
		name:        "some check",
		status:      StatusOK,
		statusCode:  200,
		message:     msg,
		lastChecked: &previousTime,
		lastSuccess: &previousTime,
		lastFailure: &currentTime,
	}
}

func TestNew(t *testing.T) {
	checkFunc := func(ctx context.Context, state *CheckState) error {
		now := time.Now().UTC()
		state.mutex.Lock()
		defer state.mutex.Unlock()

		state.lastChecked = &now
		state.lastSuccess = &now
		return nil
	}

	cfFail := func(ctx context.Context, state *CheckState) error {
		err := errors.New("checker failed to run for cfFail")
		return err
	}

	Convey("Create a new Health Check", t, func() {
		ctx := context.Background()
		timeBeforeCreation := time.Now().UTC()
		hc := New(version, criticalTimeout, interval)
		hc.Start(ctx)
		defer hc.Stop()

		So(hc.Version.BuildTime, ShouldEqual, time.Unix(0, 0))
		So(hc.Version.GitCommit, ShouldEqual, "d6cd1e2bd19e03a81132a23b2025920577f84e37")
		So(hc.Version.Language, ShouldEqual, language)
		So(hc.Version.LanguageVersion, ShouldEqual, "1.12")
		So(hc.Version.Version, ShouldEqual, "1.0.0")
		So(hc.StartTime, ShouldHappenOnOrBetween, timeBeforeCreation, time.Now().UTC())
		So(hc.criticalErrorTimeout, ShouldEqual, criticalTimeout)
		So(len(hc.Checks), ShouldEqual, 0)
		So(len(hc.tickers), ShouldEqual, 0)
	})

	Convey("Create a new Health Check and add one good working check function", t, func() {
		ctx := context.Background()
		timeBeforeCreation := time.Now().UTC()
		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("check 1", checkFunc)
		hc.Start(ctx)
		defer hc.Stop()

		So(err, ShouldBeNil)
		So(hc.Checks[0].checker, ShouldEqual, checkFunc)
		So(hc.Version.BuildTime, ShouldEqual, time.Unix(0, 0))
		So(hc.Version.GitCommit, ShouldEqual, "d6cd1e2bd19e03a81132a23b2025920577f84e37")
		So(hc.Version.Language, ShouldEqual, language)
		So(hc.Version.LanguageVersion, ShouldEqual, "1.12")
		So(hc.Version.Version, ShouldEqual, "1.0.0")
		So(hc.StartTime, ShouldHappenOnOrBetween, timeBeforeCreation, time.Now().UTC())
		So(hc.criticalErrorTimeout, ShouldEqual, criticalTimeout)
		So(len(hc.tickers), ShouldEqual, 1)
		Convey("After check function should have run, ensure the check state has updated", func() {
			time.Sleep(2 * interval)

			hc.tickers[0].check.state.mutex.RLock()
			So(*hc.tickers[0].check.state.LastChecked(), ShouldHappenOnOrBetween, timeBeforeCreation, time.Now().UTC())
			hc.tickers[0].check.state.mutex.RUnlock()
		})
	})

	Convey("Create a new Health Check and add two good working check functions", t, func() {
		ctx := context.Background()
		timeBeforeCreation := time.Now().UTC()
		hc := New(version, criticalTimeout, interval)
		err1 := hc.AddCheck("check 1", checkFunc)
		err2 := hc.AddCheck("check 2", checkFunc)
		hc.Start(ctx)
		defer hc.Stop()

		So(err1, ShouldBeNil)
		So(err2, ShouldBeNil)
		So(hc.Checks[0].checker, ShouldEqual, checkFunc)
		So(hc.Checks[1].checker, ShouldEqual, checkFunc)
		So(hc.Version.BuildTime, ShouldEqual, time.Unix(0, 0))
		So(hc.Version.GitCommit, ShouldEqual, "d6cd1e2bd19e03a81132a23b2025920577f84e37")
		So(hc.Version.Language, ShouldEqual, language)
		So(hc.Version.LanguageVersion, ShouldEqual, "1.12")
		So(hc.Version.Version, ShouldEqual, "1.0.0")
		So(hc.StartTime, ShouldHappenOnOrBetween, timeBeforeCreation, time.Now().UTC())
		So(hc.criticalErrorTimeout, ShouldEqual, criticalTimeout)
		So(len(hc.tickers), ShouldEqual, 2)
		Convey("After the check functions should have run, ensure both check states have updated", func() {
			time.Sleep(2 * interval)

			hc.tickers[0].check.state.mutex.RLock()
			So(*hc.tickers[0].check.state.LastChecked(), ShouldHappenOnOrBetween, timeBeforeCreation, time.Now().UTC())
			hc.tickers[0].check.state.mutex.RUnlock()

			hc.tickers[1].check.state.mutex.RLock()
			So(*hc.tickers[1].check.state.LastChecked(), ShouldHappenOnOrBetween, timeBeforeCreation, time.Now().UTC())
			hc.tickers[1].check.state.mutex.RUnlock()
		})
	})

	Convey("Create a new Health Check and add a broken check function", t, func() {
		ctx := context.Background()
		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("failing check", cfFail)
		hc.Start(ctx)
		defer hc.Stop()

		So(err, ShouldBeNil)
		Convey("After check function has run, ensure it has correctly stored the results", func() {
			time.Sleep(2 * interval)

			hc.tickers[0].check.state.mutex.RLock()
			s := *hc.tickers[0].check.state
			hc.tickers[0].check.state.mutex.RUnlock()

			s.mutex = nil
			So(s, ShouldResemble, CheckState{name: "failing check"})
		})
	})

	Convey("Given a Health Check with a cancellable context", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("cancellable", cfFail)
		hc.Start(ctx)
		// no `defer hc.Stop()` because of `cancel()`

		So(err, ShouldBeNil)
		So(hc.context, ShouldNotBeNil)

		Convey("When the check function has time to run, and the context is cancelled", func() {
			time.Sleep(2 * interval)

			So(len(hc.tickers), ShouldEqual, 1)
			So(hc.tickers[0].check.state, ShouldPointTo, hc.Checks[0].state)
			So(hc.tickers[0].isStopping(), ShouldBeFalse)

			cancel()

			Convey("Then the tickers are stopped/stopping", func() {
				time.Sleep(2 * interval)
				So(hc.tickers[0].isStopping(), ShouldBeTrue)
			})
		})
	})

	Convey("Create a new Health Check given 1 successful check followed by a broken run check", t, func() {
		now := time.Now().UTC()
		name := "some name"
		status := "OK"
		message := "success"
		statusCode := 200

		ctx := context.Background()
		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck(name, cfFail)
		hc.Checks[0].state.status = status
		hc.Checks[0].state.message = message
		hc.Checks[0].state.statusCode = statusCode
		hc.Checks[0].state.lastChecked = &now
		hc.Checks[0].state.lastSuccess = &now
		hc.Start(ctx)
		defer hc.Stop()

		So(err, ShouldBeNil)

		Convey("After check function has run, the original check should not be overwritten by the failed check", func() {
			time.Sleep(2 * interval)

			hc.Checks[0].state.mutex.RLock()
			So(hc.Checks[0].state.name, ShouldEqual, name)
			So(hc.Checks[0].state.status, ShouldEqual, status)
			So(hc.Checks[0].state.message, ShouldEqual, message)
			So(hc.Checks[0].state.statusCode, ShouldEqual, statusCode)
			So(hc.Checks[0].state.lastChecked, ShouldEqual, &now)
			So(hc.Checks[0].state.lastSuccess, ShouldEqual, &now)
			hc.tickers[0].check.state.mutex.RUnlock()
		})
	})
}

func TestAddCheck(t *testing.T) {
	cf := func(ctx context.Context, state *CheckState) error {
		return nil
	}

	Convey("Given a Health Check without any registered checks", t, func() {
		ctx := context.Background()
		hc := New(version, criticalTimeout, interval)

		Convey("After adding a check there should be one timer on start", func() {
			err := hc.AddCheck("check 1", cf)
			So(err, ShouldBeNil)

			hc.Start(ctx)
			defer hc.Stop()

			time.Sleep(2 * interval)
			So(len(hc.tickers), ShouldEqual, 1)
		})
	})

	Convey("Given a Health Check with 1 registered check", t, func() {
		ctx := context.Background()
		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("check 1", cf)

		So(err, ShouldBeNil)

		Convey("After adding the second check there should be two timers on start", func() {
			err := hc.AddCheck("check 2", cf)
			So(err, ShouldBeNil)

			hc.Start(ctx)
			defer hc.Stop()

			time.Sleep(2 * interval)
			So(len(hc.tickers), ShouldEqual, 2)
		})
	})

	Convey("Given a Health Check with 1 check that is started", t, func() {
		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("check 1", cf)
		hc.Start(context.Background())
		defer hc.Stop()

		So(err, ShouldBeNil)
		origNumberOftickers := len(hc.tickers)
		Convey("When you add another check", func() {
			err := hc.AddCheck("check 2", cf)
			time.Sleep(2 * interval)
			Convey("Then the number of tickers should increase by one", func() {
				So(err, ShouldBeNil)
				So(len(hc.tickers), ShouldEqual, origNumberOftickers+1)
			})
		})
	})

	Convey("Given a Health Check without any registered checks", t, func() {
		ctx := context.Background()
		hc := New(version, criticalTimeout, interval)

		Convey("Then adding a check with a nil checker function should fail", func() {
			err := hc.AddCheck("nil check", nil)
			So(err, ShouldNotBeNil)

			hc.Start(ctx)
			defer hc.Stop()

			time.Sleep(2 * interval)
			So(len(hc.tickers), ShouldEqual, 0)
		})
	})
}

func TestNewVersionInfo(t *testing.T) {
	Convey("Create a new versionInfo object", t, func() {
		buildTime := "0"
		gitCommit := "d6cd1e2bd19e03a81132a23b2025920577f84e37"
		version := "1.0.0"

		expectedVersion := VersionInfo{
			BuildTime:       time.Unix(0, 0),
			GitCommit:       gitCommit,
			Language:        language,
			LanguageVersion: runtime.Version(),
			Version:         version,
		}

		outputVersion, err := NewVersionInfo(buildTime, gitCommit, version)

		So(err, ShouldBeNil)
		So(outputVersion, ShouldResemble, expectedVersion)
	})

	Convey("Create a new versionInfo object passing an invalid build time", t, func() {
		buildTime := "some invalid date"
		gitCommit := "d6cd1e2bd19e03a81132a23b2025920577f84e37"
		version := "1.0.0"

		expectedVersion := VersionInfo{
			BuildTime:       time.Unix(0, 0),
			GitCommit:       gitCommit,
			Language:        language,
			LanguageVersion: runtime.Version(),
			Version:         version,
		}

		outputVersion, err := NewVersionInfo(buildTime, gitCommit, version)

		So(err, ShouldNotBeNil)
		So(outputVersion, ShouldResemble, expectedVersion)
	})
}

func TestStop(t *testing.T) {

	tickerFinished := false
	checkerDuration := interval / 2

	emptyChecker := func(ctx context.Context, state *CheckState) error {
		return nil
	}

	longRunningChecker := func(ctx context.Context, state *CheckState) error {
		time.Sleep(checkerDuration)
		tickerFinished = true
		return nil
	}

	Convey("Given a Health Check with a long running checker", t, func() {

		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("check 1", longRunningChecker)
		So(err, ShouldBeNil)
		hc.Start(context.Background())

		Convey("When stop is called while the ticker is running", func() {

			time.Sleep(interval + (checkerDuration / 2)) // wait for the initial tick and then give it time to start running
			hc.Stop()

			Convey("Then the stop function does not return until the checker is complete", func() {
				So(tickerFinished, ShouldBeTrue)
			})
		})
	})

	Convey("Given a Health Check with multiple long running checkers", t, func() {

		hc := New(version, criticalTimeout, interval)
		err := hc.AddCheck("check 1", emptyChecker)
		So(err, ShouldBeNil)
		err = hc.AddCheck("check 2", emptyChecker)
		So(err, ShouldBeNil)
		err = hc.AddCheck("check 3", longRunningChecker)
		So(err, ShouldBeNil)
		hc.Start(context.Background())

		Convey("When stop is called while the ticker is running", func() {

			time.Sleep(interval + (checkerDuration / 2)) // wait for the initial tick and then give it time to start running
			hc.Stop()

			Convey("Then the stop function does not return until all checkers have completed", func() {
				So(tickerFinished, ShouldBeTrue)
			})
		})
	})
}
