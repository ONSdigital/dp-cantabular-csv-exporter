package healthcheck

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ONSdigital/log.go/log"
	. "github.com/smartystreets/goconvey/convey"
)

var testVersion = VersionInfo{
	BuildTime:       time.Unix(0, 0),
	GitCommit:       "d6cd1e2bd19e03a81132a23b2025920577f84e37",
	Language:        "go",
	LanguageVersion: "1.12",
	Version:         "1.0.0",
}

// Test getStatus() function that inherits logic from isAppStartingUp() and isAppHealthy()
func TestGetStatus(t *testing.T) {
	t0 := time.Now().UTC()
	criticalErrTimeout := 10 * time.Minute
	ctx := context.Background()

	Convey("Given application is still starting up", t, func() {
		Convey("Then the app has a health state of WARNING", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding checks
			statuses := []CheckState{CheckState{}}
			hc.Checks = createChecksSlice(statuses, true)

			state := hc.getStatus(ctx)
			So(state, ShouldEqual, StatusWarning)
		})
	})

	Convey("Given application has started up successfully", t, func() {
		Convey("Then the app has a health state of OK", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding checks
			statuses := []CheckState{CheckState{status: StatusOK, lastChecked: &t0, mutex: &sync.RWMutex{}}}
			hc.Checks = createChecksSlice(statuses, true)

			state := hc.getStatus(ctx)
			So(state, ShouldEqual, StatusOK)
		})
	})
}

// Test isAppStartingUp() function
func TestIsAppStartingUP(t *testing.T) {
	t0 := time.Now().UTC()
	criticalErrTimeout := 10 * time.Minute

	Convey("Given no checks against healthcheck", t, func() {
		Convey("Then the app has not started up", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			isStarting := hc.isAppStartingUp()
			So(isStarting, ShouldEqual, false)
		})
	})

	Convey("Given a healthcheck with a single check without a lastChecked timestamp", t, func() {
		Convey("Then the app has not started up", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding checks
			statuses := []CheckState{CheckState{}}
			hc.Checks = createChecksSlice(statuses, true)

			isStarting := hc.isAppStartingUp()
			So(isStarting, ShouldEqual, true)
		})
	})

	Convey("Given a healthcheck with a single check with a lastChecked timestamp", t, func() {
		Convey("Then the app has not started up", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding checks
			statuses := []CheckState{CheckState{lastChecked: &t0, mutex: &sync.RWMutex{}}}
			hc.Checks = createChecksSlice(statuses, true)

			isStarting := hc.isAppStartingUp()
			So(isStarting, ShouldEqual, false)
		})
	})

	Convey("Given a healthcheck with two checks both with a lastChecked timestamp", t, func() {
		Convey("Then the app has not started up", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding checks
			statuses := []CheckState{CheckState{lastChecked: &t0, mutex: &sync.RWMutex{}}, CheckState{lastChecked: &t0, mutex: &sync.RWMutex{}}}
			hc.Checks = createChecksSlice(statuses, true)

			isStarting := hc.isAppStartingUp()
			So(isStarting, ShouldEqual, false)
		})
	})

	Convey("Given a healthcheck with two checks with at least one without a lastChecked timestamp", t, func() {
		Convey("Then the app has started up", func() {
			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding checks
			statuses := []CheckState{CheckState{lastChecked: &t0, mutex: &sync.RWMutex{}}, CheckState{mutex: &sync.RWMutex{}}}
			hc.Checks = createChecksSlice(statuses, true)

			isStarting := hc.isAppStartingUp()
			So(isStarting, ShouldEqual, true)
		})
	})
}

// Test getCheckStatus() function
func TestGetCheckStatus(t *testing.T) {
	t0 := time.Now().UTC()
	t9 := t0.Add(-9 * time.Minute)   // 9 min ago
	t10 := t0.Add(-10 * time.Minute) // 10 min ago
	t20 := t0.Add(-20 * time.Minute) // 20 min ago

	criticalErrTimeout := 10 * time.Minute

	// Create empty health check object
	hc := HealthCheck{
		Version:              testVersion,
		StartTime:            t0,
		criticalErrorTimeout: criticalErrTimeout,
		tickers:              nil,
	}

	Convey("Given check status is okay return OK", t, func() {
		check := &Check{
			state: &CheckState{
				status: StatusOK,
				mutex:  &sync.RWMutex{},
			},
		}

		status := hc.getCheckStatus(check)
		So(status, ShouldEqual, StatusOK)
	})

	Convey("Given check status is warning return warning", t, func() {
		check := &Check{
			state: &CheckState{
				status: StatusWarning,
				mutex:  &sync.RWMutex{},
			},
		}

		status := hc.getCheckStatus(check)
		So(status, ShouldEqual, StatusWarning)
	})

	Convey("Given check status is failure", t, func() {
		Convey("When check is without last successful state and time of first critical state"+
			"is greater than the critical timeout", func() {
			Convey("Then the returning status is critical", func() {
				check := &Check{
					state: &CheckState{
						status: StatusCritical,
						mutex:  &sync.RWMutex{},
					},
				}

				hc.timeOfFirstCriticalError = t20

				status := hc.getCheckStatus(check)
				So(status, ShouldEqual, StatusCritical)
				So(hc.timeOfFirstCriticalError, ShouldEqual, t20)
			})
		})

		Convey("When check is without last successful state and time of first critical state"+
			"is equal to the critical timeout", func() {

			Convey("Then the returning status is critical", func() {
				check := &Check{
					state: &CheckState{
						status: StatusCritical,
						mutex:  &sync.RWMutex{},
					},
				}

				hc.timeOfFirstCriticalError = t10

				status := hc.getCheckStatus(check)
				So(status, ShouldEqual, StatusCritical)
				So(hc.timeOfFirstCriticalError, ShouldEqual, t10)
			})
		})

		Convey("When check is without last successful state and time of first critical state"+
			"is less than the critical timeout", func() {
			Convey("Then the returning status is warning", func() {
				check := &Check{
					state: &CheckState{
						status: StatusCritical,
						mutex:  &sync.RWMutex{},
					},
				}

				hc.timeOfFirstCriticalError = t9

				status := hc.getCheckStatus(check)
				So(status, ShouldEqual, StatusWarning)
				So(hc.timeOfFirstCriticalError, ShouldEqual, t9)
				So(check.state.LastSuccess(), ShouldBeNil)
			})
		})

		Convey("When time of first critical state is less than the critical timeout"+
			"but the check has a last successful state that occurred within the critical timeout", func() {
			Convey("Then the returning status is warning", func() {
				check := &Check{
					state: &CheckState{
						status:      StatusCritical,
						lastSuccess: &t9,
						mutex:       &sync.RWMutex{},
					},
				}

				hc.timeOfFirstCriticalError = t10

				status := hc.getCheckStatus(check)
				So(status, ShouldEqual, StatusWarning)
				So(hc.timeOfFirstCriticalError, ShouldHappenOnOrBetween, t0, time.Now().UTC())
				So(check.state.LastSuccess(), ShouldResemble, &t9)
			})
		})
	})
}

// Testing isAppHealthy() function that inherits logic from getCheckStatus()
func TestIsAppHealthy(t *testing.T) {

	criticalErrTimeout := 10 * time.Minute

	t0 := time.Now().UTC()
	t1 := t0.Add(-1 * time.Minute)   // 1 min ago
	t10 := t0.Add(-10 * time.Minute) // 10 min ago
	t20 := t0.Add(-20 * time.Minute) // 20 min ago

	healthyCheck := CheckState{
		name:        "service-1",
		status:      StatusOK,
		statusCode:  http.StatusOK,
		message:     "Service is healthy",
		lastChecked: &t1,
		lastSuccess: &t1,
	}

	warningCheck := CheckState{
		name:        "service-2",
		status:      StatusWarning,
		statusCode:  http.StatusTooManyRequests,
		message:     "Part of service is unavailable",
		lastChecked: &t1,
		lastSuccess: &t10,
	}

	criticalCheck := CheckState{
		name:        "service-3",
		status:      StatusCritical,
		statusCode:  http.StatusInternalServerError,
		message:     "Service is unavailable",
		lastChecked: &t1,
		lastSuccess: &t20,
		lastFailure: &t1,
	}

	Convey("Given healthcheck contains two checks, both with statuses of OK", t, func() {
		Convey("Then the returning status is OK", func() {

			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding two healthy checks
			statuses := []CheckState{healthyCheck, healthyCheck}
			hc.Checks = createChecksSlice(statuses, true)

			status := hc.isAppHealthy()
			So(status, ShouldEqual, StatusOK)
		})
	})

	Convey("Given healthcheck contains two checks, one with status of OK"+
		"and the other with status WARNING", t, func() {
		Convey("Then the returning status is WARNING", func() {

			// Create health check object
			hc := HealthCheck{
				Version:              testVersion,
				StartTime:            t0,
				criticalErrorTimeout: criticalErrTimeout,
				tickers:              nil,
			}

			// Adding two healthy checks
			statuses := []CheckState{healthyCheck, warningCheck}
			hc.Checks = createChecksSlice(statuses, true)

			status := hc.isAppHealthy()
			So(status, ShouldEqual, StatusWarning)
		})
	})

	Convey("Given healthcheck contains two checks, one with status of OK"+
		"and the other with status CRITICAL but has not succeeded the critical timeout", t, func() {
		Convey("Then the returning status is WARNING", func() {

			// Create health check object
			hc := HealthCheck{
				Version:                  testVersion,
				StartTime:                t20,
				criticalErrorTimeout:     criticalErrTimeout,
				tickers:                  nil,
				timeOfFirstCriticalError: t1,
			}

			// Adding two healthy checks
			statuses := []CheckState{healthyCheck, criticalCheck}
			hc.Checks = createChecksSlice(statuses, true)

			status := hc.isAppHealthy()
			So(status, ShouldEqual, StatusWarning)
		})
	})

	Convey("Given healthcheck contains two checks, one with status of OK"+
		"and the other with status CRITICAL that has succeeded the critical timeout", t, func() {
		Convey("Then the returning status is CRITICAL", func() {

			// Create health check object
			hc := HealthCheck{
				Version:                  testVersion,
				StartTime:                t20,
				criticalErrorTimeout:     criticalErrTimeout,
				tickers:                  nil,
				timeOfFirstCriticalError: t10,
			}

			// Adding two healthy checks
			statuses := []CheckState{healthyCheck, criticalCheck}
			hc.Checks = createChecksSlice(statuses, true)

			status := hc.isAppHealthy()
			So(status, ShouldEqual, StatusCritical)
		})
	})

	Convey("Given healthcheck contains two checks, one with status of WARNING"+
		"and the other with status CRITICAL but has not succeeded the critical timeout", t, func() {
		Convey("Then the returning status is WARNING", func() {

			// Create health check object
			hc := HealthCheck{
				Version:                  testVersion,
				StartTime:                t20,
				criticalErrorTimeout:     criticalErrTimeout,
				tickers:                  nil,
				timeOfFirstCriticalError: t1,
			}

			// Adding two healthy checks
			statuses := []CheckState{warningCheck, criticalCheck}
			hc.Checks = createChecksSlice(statuses, true)

			status := hc.isAppHealthy()
			So(status, ShouldEqual, StatusWarning)
		})
	})

	Convey("Given healthcheck contains two checks, one with status of WARNING"+
		"and the other with status CRITICAL and has succeeded the critical timeout", t, func() {
		Convey("Then the returning status is CRITICAL", func() {

			// Create health check object
			hc := HealthCheck{
				Version:                  testVersion,
				StartTime:                t20,
				criticalErrorTimeout:     criticalErrTimeout,
				tickers:                  nil,
				timeOfFirstCriticalError: t10,
			}

			// Adding two healthy checks
			statuses := []CheckState{warningCheck, criticalCheck}
			hc.Checks = createChecksSlice(statuses, true)

			status := hc.isAppHealthy()
			So(status, ShouldEqual, StatusCritical)
		})
	})
}

func TestHandlerSingleCheck(t *testing.T) {
	t0 := time.Now().UTC()
	t10 := t0.Add(-10 * time.Minute) // 10 min ago
	t20 := t0.Add(-20 * time.Minute) // 20 min ago
	t30 := t0.Add(-30 * time.Minute) // 30 min ago
	criticalErrTimeout := 11 * time.Minute

	healthyStatus1 := CheckState{
		name:        "Some App 1",
		status:      StatusOK,
		statusCode:  http.StatusOK,
		message:     "App 1 is healthy",
		lastChecked: &t0,
		lastSuccess: &t0,
		lastFailure: &t10,
	}
	unhealthyStatus := CheckState{
		name:        "Some App 2",
		status:      StatusWarning,
		statusCode:  http.StatusTooManyRequests,
		message:     "Something has been unhealthy for past 10 minutes",
		lastChecked: &t0,
		lastSuccess: &t10,
		lastFailure: &t0,
	}
	freshCriticalStatus := CheckState{
		name:        "Some App 3",
		status:      StatusCritical,
		statusCode:  http.StatusInternalServerError,
		message:     "Something has been critical for the past 10 minutes",
		lastChecked: &t0,
		lastSuccess: &t10,
		lastFailure: &t0,
	}
	oldCriticalStatus := CheckState{
		name:        "Some App 4",
		status:      StatusCritical,
		statusCode:  http.StatusInternalServerError,
		message:     "Something has been critical for the past 30 minutes",
		lastChecked: &t0,
		lastSuccess: &t30,
		lastFailure: &t0,
	}

	nilStatus := CheckState{
		name: "Some App 5",
	}
	healthyNeverUnhealthyStatus := CheckState{
		name:        "Some App 6",
		status:      StatusOK,
		statusCode:  http.StatusOK,
		message:     "App 6 is healthy",
		lastChecked: &t0,
		lastSuccess: &t0,
	}
	unhealthyNeverHealthyStatus := CheckState{
		name:        "Some App 7",
		status:      StatusWarning,
		statusCode:  http.StatusTooManyRequests,
		message:     "Something is unhealthy",
		lastChecked: &t0,
		lastFailure: &t0,
	}
	criticalNeverHealthyStatus := CheckState{
		name:        "Some App 8",
		status:      StatusCritical,
		statusCode:  http.StatusTooManyRequests,
		message:     "Something is critical",
		lastChecked: &t0,
		lastFailure: &t0,
	}

	Convey("Given a healthcheck with no past failures or successes", t, func() {

		hc := HealthCheck{
			Version:              testVersion,
			StartTime:            t0,
			criticalErrorTimeout: criticalErrTimeout,
			tickers:              nil,
		}

		Convey("Then an empty check should result in the app reporting back as warning", func() {
			statuses := []CheckState{nilStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError not set
			So(hc.timeOfFirstCriticalError, ShouldResemble, time.Time{})
		})

		Convey("Then a healthy check that has never been unhealthy should result in the app reporting back as healthy", func() {
			statuses := []CheckState{healthyNeverUnhealthyStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusOK, testVersion, t0, statuses, http.StatusOK)
			// timeOfFirstCriticalError not set
			So(hc.timeOfFirstCriticalError, ShouldResemble, time.Time{})
		})
		Convey("Then a healthy check that has been unhealthy in the past should result in the app reporting back as healthy", func() {
			statuses := []CheckState{healthyStatus1}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusOK, testVersion, t0, statuses, http.StatusOK)
			// timeOfFirstCriticalError not set
			So(hc.timeOfFirstCriticalError, ShouldResemble, time.Time{})
		})
		Convey("Then an unhealthy check that has never been healthy should result in the app reporting back as warning", func() {
			statuses := []CheckState{unhealthyNeverHealthyStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError not set
			So(hc.timeOfFirstCriticalError, ShouldResemble, time.Time{})
		})
		Convey("Then an unhealthy check that has been healthy in the past should result in the app reporting back as warning", func() {
			statuses := []CheckState{unhealthyStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError not set
			So(hc.timeOfFirstCriticalError, ShouldResemble, time.Time{})
		})
		Convey("Then a critical check that has never been healthy should result in the app reporting back as warning and updating timestamp for first critical error", func() {
			statuses := []CheckState{criticalNeverHealthyStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError set to this check's failure time
			So(hc.timeOfFirstCriticalError, ShouldHappenWithin, time.Second, t0)
		})
		Convey("Then a critical check that has been healthy in the past should result in the app reporting back as warning and updating timestamp for first critical error", func() {
			statuses := []CheckState{oldCriticalStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError set to this check's failure time
			So(hc.timeOfFirstCriticalError, ShouldHappenWithin, time.Second, t0)
		})
	})

	Convey("Given a healthcheck with a recent past critical check (timeout not expired), and no success received since", t, func() {

		hc := HealthCheck{
			Version:                  testVersion,
			StartTime:                t0,
			criticalErrorTimeout:     criticalErrTimeout,
			timeOfFirstCriticalError: t10,
			tickers:                  nil,
		}

		Convey("Then a healthy check should result in the app reporting back as healthy", func() {
			statuses := []CheckState{healthyNeverUnhealthyStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusOK, testVersion, t0, statuses, http.StatusOK)
			// timeOfFirstCriticalError not updated
			So(hc.timeOfFirstCriticalError, ShouldResemble, t10)
		})
		Convey("Then a recent critical check happening before the timeout expires should result in the app reporting back as warning", func() {
			statuses := []CheckState{freshCriticalStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError not updated
			So(hc.timeOfFirstCriticalError, ShouldResemble, t10)
		})
		Convey("Then a critical check that has been critical for longer than the timeout and the value of first critical error, "+
			"should result in the app reporting back as warning and not updating timestamp for first critical error", func() {
			statuses := []CheckState{oldCriticalStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError not updated
			So(hc.timeOfFirstCriticalError, ShouldResemble, t10)
		})
	})

	Convey("Given a healthcheck with an old past critical check (timeout expired), and no success received since", t, func() {

		hc := HealthCheck{
			Version:                  testVersion,
			StartTime:                t0,
			criticalErrorTimeout:     criticalErrTimeout,
			timeOfFirstCriticalError: t20,
			tickers:                  nil,
		}

		Convey("Then a healthy check should result in the app reporting back as healthy", func() {
			statuses := []CheckState{healthyNeverUnhealthyStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusOK, testVersion, t0, statuses, http.StatusOK)
			// timeOfFirstCriticalError not set
			So(hc.timeOfFirstCriticalError, ShouldResemble, t20)
		})
		Convey("Then a recent critical check (last success more recent than first critical) should result in the app reporting back as warning "+
			"and refresh timestamp for first critical error", func() {
			statuses := []CheckState{freshCriticalStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, t0, statuses, http.StatusTooManyRequests)
			// timeOfFirstCriticalError set to this check's failure time
			So(hc.timeOfFirstCriticalError, ShouldHappenWithin, time.Second, t0)
		})
		Convey("Then a critical check (last success older than first critical) should result in the app reporting back as critical "+
			"and not refreshing timestamp for first critical error", func() {
			statuses := []CheckState{oldCriticalStatus}
			hc.Checks = createChecksSlice(statuses, true)
			runHealthHandlerAndTest(t, &hc, StatusCritical, testVersion, t0, statuses, http.StatusInternalServerError)
			// timeOfFirstCriticalError set to this check's failure time
			So(hc.timeOfFirstCriticalError, ShouldResemble, t20)
		})
	})

}

func TestHandlerMultipleChecks(t *testing.T) {
	testStartTime := time.Now().UTC().Add(-20 * time.Minute)
	priorTestTime := testStartTime.Add(-30 * time.Minute)
	healthyStatus1 := CheckState{
		name:        "Some App 1",
		status:      StatusOK,
		statusCode:  http.StatusOK,
		message:     "Some message about app 1 here",
		lastChecked: &testStartTime,
		lastSuccess: &testStartTime,
		lastFailure: &priorTestTime,
	}
	healthyStatus2 := CheckState{
		name:        "Some App 2",
		status:      StatusOK,
		statusCode:  http.StatusOK,
		message:     "Some message about app 2 here",
		lastChecked: &testStartTime,
		lastSuccess: &testStartTime,
		lastFailure: &priorTestTime,
	}
	healthyStatus3 := CheckState{
		name:        "Some App 3",
		status:      StatusOK,
		message:     "Some message about app 2 here",
		lastChecked: &testStartTime,
		lastSuccess: &testStartTime,
		lastFailure: &priorTestTime,
	}
	unhealthyStatus := CheckState{
		name:        "Some App 4",
		status:      StatusWarning,
		statusCode:  http.StatusTooManyRequests,
		message:     "Something has been unhealthy for past 30 minutes",
		lastChecked: &testStartTime,
		lastSuccess: &priorTestTime,
		lastFailure: &testStartTime,
	}
	criticalStatus := CheckState{
		name:        "Some App 5",
		status:      StatusCritical,
		statusCode:  http.StatusInternalServerError,
		message:     "Something has been critical for the past 30 minutes",
		lastChecked: &testStartTime,
		lastSuccess: &priorTestTime,
		lastFailure: &testStartTime,
	}
	freshCriticalStatus := CheckState{
		name:        "Some App 6",
		status:      StatusCritical,
		statusCode:  http.StatusInternalServerError,
		message:     "Something has been critical for the past 30 minutes",
		lastChecked: &testStartTime,
		lastSuccess: &testStartTime,
		lastFailure: &priorTestTime,
	}

	Convey("Given a complete Healthy set of checks the app should report back as healthy", t, func() {
		statuses := []CheckState{healthyStatus1, healthyStatus2, healthyStatus3}
		hc := createHealthCheck(statuses, testStartTime, 10*time.Minute, true)
		hc.timeOfFirstCriticalError = testStartTime.Add(-30 * time.Minute)
		runHealthHandlerAndTest(t, &hc, StatusOK, testVersion, testStartTime, statuses, http.StatusOK)
	})
	Convey("Given a healthy app and an unhealthy app", t, func() {
		statuses := []CheckState{healthyStatus1, unhealthyStatus}
		hc := createHealthCheck(statuses, testStartTime, 15*time.Second, true)
		hc.timeOfFirstCriticalError = testStartTime.Add(-30 * time.Minute)
		runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, testStartTime, statuses, http.StatusTooManyRequests)
	})
	Convey("Given a healthy app and a critical app that is beyond the threshold", t, func() {
		checks := []CheckState{healthyStatus1, criticalStatus}
		hc := createHealthCheck(checks, testStartTime, 10*time.Minute, true)
		hc.timeOfFirstCriticalError = testStartTime.Add(-22 * time.Minute)
		runHealthHandlerAndTest(t, &hc, StatusCritical, testVersion, testStartTime, checks, http.StatusInternalServerError)
	})
	Convey("Given an unhealthy app and an app that has just turned critical and is under the critical threshold", t, func() {
		statuses := []CheckState{unhealthyStatus, freshCriticalStatus}
		hc := createHealthCheck(statuses, testStartTime, 10*time.Minute, true)
		hc.timeOfFirstCriticalError = testStartTime.Add(-1 * time.Minute)
		runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, testStartTime, statuses, http.StatusTooManyRequests)
	})
	Convey("Given an unhealthy app and an app that has been critical for longer than the critical threshold", t, func() {
		statuses := []CheckState{unhealthyStatus, criticalStatus}
		hc := createHealthCheck(statuses, testStartTime, 10*time.Minute, true)
		hc.timeOfFirstCriticalError = testStartTime.Add(-22 * time.Minute)
		runHealthHandlerAndTest(t, &hc, StatusCritical, testVersion, testStartTime, statuses, http.StatusInternalServerError)
	})
	Convey("Given an app just started up", t, func() {
		statuses := []CheckState{freshCriticalStatus}
		justStartedTime := time.Now().UTC()
		hc := createHealthCheck(statuses, justStartedTime, 10*time.Minute, false)
		hc.timeOfFirstCriticalError = justStartedTime
		runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, justStartedTime, nil, http.StatusTooManyRequests)
	})
	Convey("Given an app has begun to start but not finished starting up completely", t, func() {
		statuses := []CheckState{freshCriticalStatus}
		justStartedTime := time.Now().UTC()
		hc := createHealthCheck(statuses, justStartedTime, 10*time.Minute, true)
		hc.timeOfFirstCriticalError = justStartedTime
		runHealthHandlerAndTest(t, &hc, StatusWarning, testVersion, justStartedTime, statuses, http.StatusTooManyRequests)
	})
	Convey("Given no apps", t, func() {
		var checks []*Check
		var statuses []CheckState
		hc := HealthCheck{
			Checks:                   checks,
			Version:                  testVersion,
			StartTime:                testStartTime,
			criticalErrorTimeout:     10 * time.Minute,
			timeOfFirstCriticalError: testStartTime.Add(-30 * time.Minute),
			tickers:                  nil,
		}
		runHealthHandlerAndTest(t, &hc, StatusOK, testVersion, testStartTime, statuses, http.StatusOK)
	})
}

func createATestCheck(stateToReturn CheckState, hasPreviousCheck bool) *Check {
	checkerFunc := func(ctx context.Context, state *CheckState) error {
		state.status = stateToReturn.status
		state.message = stateToReturn.message
		state.statusCode = stateToReturn.statusCode
		state.lastChecked = stateToReturn.lastChecked
		state.lastSuccess = stateToReturn.lastSuccess
		state.lastFailure = stateToReturn.lastFailure
		return nil
	}
	check, _ := NewCheck(stateToReturn.name, checkerFunc)
	if hasPreviousCheck {
		check.state.status = stateToReturn.status
		check.state.message = stateToReturn.message
		check.state.statusCode = stateToReturn.statusCode
		check.state.lastChecked = stateToReturn.lastChecked
		check.state.lastSuccess = stateToReturn.lastSuccess
		check.state.lastFailure = stateToReturn.lastFailure
	}
	return check
}

func createHealthCheck(statuses []CheckState, startTime time.Time, critErrTimeout time.Duration, hasPreviousCheck bool) HealthCheck {
	hc := HealthCheck{
		Checks:               createChecksSlice(statuses, hasPreviousCheck),
		Version:              testVersion,
		StartTime:            startTime,
		criticalErrorTimeout: critErrTimeout,
		tickers:              nil,
	}
	return hc
}

func createChecksSlice(statuses []CheckState, hasPreviousCheck bool) []*Check {
	var checks []*Check
	for _, status := range statuses {
		checks = append(checks, createATestCheck(status, hasPreviousCheck))
	}
	return checks
}

func runHealthHandlerAndTest(t *testing.T, hc *HealthCheck, desiredStatus string, testVersion VersionInfo, testStartTime time.Time, statuses []CheckState, expectedHTTPCode int) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fail()
	}
	w := httptest.NewRecorder()
	handler := http.HandlerFunc(hc.Handler)
	handler.ServeHTTP(w, req)
	b, err := ioutil.ReadAll(w.Body)
	if err != nil {
		log.Event(nil, "unable to read request body", log.Error(err))
	}
	var healthCheck HealthCheck
	err = json.Unmarshal(b, &healthCheck)
	if err != nil {
		log.Event(nil, "unable to unmarshal bytes into healthcheck", log.Error(err))

		So(err, ShouldBeNil)
		return
	}
	So(w.Code, ShouldEqual, expectedHTTPCode)
	So(healthCheck.Status, ShouldEqual, desiredStatus)
	So(healthCheck.Version, ShouldResemble, testVersion)
	So(healthCheck.StartTime, ShouldEqual, testStartTime)
	So(healthCheck.Uptime, ShouldNotBeNil)
	So(time.Now().UTC().After(healthCheck.StartTime.Add(healthCheck.Uptime)), ShouldBeTrue)

	for i, check := range healthCheck.Checks {
		if i < len(statuses) {
			check.state.mutex.RLock()
			s := *check.state
			check.state.mutex.RUnlock()
			s.mutex = nil
			So(s, ShouldResemble, statuses[i])
		}
	}
}
