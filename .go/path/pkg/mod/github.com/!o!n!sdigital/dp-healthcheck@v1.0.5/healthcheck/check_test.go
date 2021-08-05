package healthcheck

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateNew(t *testing.T) {
	var (
		checkerFunc = func(ctx context.Context, check *CheckState) error {
			return nil
		}
		check *Check
		err   error
	)
	Convey("Create a new check", t, func() {
		check, _ = NewCheck("check", checkerFunc)
		So(err, ShouldBeNil)
		So(check.checker, ShouldEqual, checkerFunc)
		So(check.state.mutex, ShouldNotBeNil)
		So(check.state.name, ShouldEqual, "check")
		So(check.state.status, ShouldEqual, "")
		So(check.state.statusCode, ShouldEqual, 0)
		So(check.state.message, ShouldEqual, "")
		So(check.state.lastChecked, ShouldBeNil)
		So(check.state.lastSuccess, ShouldBeNil)
		So(check.state.lastFailure, ShouldBeNil)
	})
	Convey("A second new check shares the right values", t, func() {
		check2, err := NewCheck("check 2", checkerFunc)
		So(err, ShouldBeNil)
		So(check2.checker, ShouldEqual, check.checker)
		So(check2.state.mutex, ShouldNotPointTo, check.state.mutex)
	})
	Convey("Fail to create a new check as checker given is nil", t, func() {
		check3, err := NewCheck("nil check", nil)
		So(check3, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
}

func TestUpdate(t *testing.T) {
	var (
		checkName   = "check name"
		okMessage   = "I'm OK"
		failMessage = "failed to ..."
	)

	Convey("Given a new check state", t, func() {
		before := time.Now().UTC()

		state := NewCheckState(checkName)

		So(state.name, ShouldEqual, checkName)

		Convey("When the state is updated with OK status", func() {
			err := state.Update(StatusOK, okMessage, 200)
			So(err, ShouldBeNil)

			Convey("Then the check state should be set accordingly", func() {
				after := time.Now().UTC()

				So(state.name, ShouldEqual, checkName)
				So(state.status, ShouldEqual, StatusOK)
				So(state.message, ShouldEqual, okMessage)
				So(state.statusCode, ShouldEqual, 200)
				So(*state.lastChecked, ShouldHappenOnOrBetween, before, after)
				So(*state.lastSuccess, ShouldHappenOnOrBetween, before, after)
			})
		})
	})

	Convey("Given a new check state", t, func() {
		before := time.Now().UTC()

		state := NewCheckState(checkName)

		So(state.name, ShouldEqual, checkName)

		Convey("When the state is updated with warning status", func() {
			err := state.Update(StatusWarning, failMessage, 200)
			So(err, ShouldBeNil)

			Convey("Then the check state should be set accordingly", func() {
				after := time.Now().UTC()

				So(state.name, ShouldEqual, checkName)
				So(state.status, ShouldEqual, StatusWarning)
				So(state.message, ShouldEqual, failMessage)
				So(state.statusCode, ShouldEqual, 200)
				So(*state.lastChecked, ShouldHappenOnOrBetween, before, after)
				So(*state.lastFailure, ShouldHappenOnOrBetween, before, after)
			})
		})
	})

	Convey("Given a new check state with valid existing state", t, func() {
		before := time.Now().UTC()

		state := NewCheckState(checkName)

		So(state.name, ShouldEqual, checkName)

		Convey("When the state is updated with critical status", func() {
			err := state.Update(StatusCritical, failMessage, 502)
			So(err, ShouldBeNil)

			Convey("Then the check state should be set accordingly", func() {
				after := time.Now().UTC()

				So(state.name, ShouldEqual, checkName)
				So(state.status, ShouldEqual, StatusCritical)
				So(state.message, ShouldEqual, failMessage)
				So(state.statusCode, ShouldEqual, 502)
				So(*state.lastChecked, ShouldHappenOnOrBetween, before, after)
				So(*state.lastFailure, ShouldHappenOnOrBetween, before, after)
			})
		})
	})

	Convey("Given a new check state", t, func() {
		before := time.Now().UTC()

		state := NewCheckState(checkName)

		err := state.Update(StatusOK, okMessage, 200)
		So(err, ShouldBeNil)

		after := time.Now().UTC()

		So(state.name, ShouldEqual, checkName)
		So(state.status, ShouldEqual, StatusOK)
		So(state.message, ShouldEqual, okMessage)
		So(state.statusCode, ShouldEqual, 200)
		So(*state.lastChecked, ShouldHappenOnOrBetween, before, after)
		So(*state.lastSuccess, ShouldHappenOnOrBetween, before, after)

		Convey("When the state is updated with another state", func() {
			before2 := time.Now().UTC()
			err := state.Update(StatusCritical, failMessage, 0)
			So(err, ShouldBeNil)

			Convey("Then then only the changed fields should overwitten", func() {
				after2 := time.Now().UTC()

				So(state.name, ShouldEqual, checkName)
				So(state.status, ShouldEqual, StatusCritical)
				So(state.message, ShouldEqual, failMessage)
				So(state.statusCode, ShouldEqual, 0)
				So(*state.lastChecked, ShouldHappenOnOrBetween, before2, after2)
				So(*state.lastFailure, ShouldHappenOnOrBetween, before2, after2)
				So(*state.lastSuccess, ShouldHappenOnOrBetween, before, after)
			})
		})
	})

	Convey("Given a new check state with valid existing state", t, func() {
		state := NewCheckState(checkName)

		Convey("When the state is updated with an invalid status", func() {
			err := state.Update("some invalid status", failMessage, 502)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestGets(t *testing.T) {
	Convey("Given a populated check state", t, func() {
		t0 := time.Unix(0, 0).UTC()
		t1 := t0.Add(1 * time.Minute)
		t2 := t0.Add(2 * time.Minute)
		state := CheckState{
			name:        "something",
			status:      "OK",
			message:     "this is a message",
			statusCode:  200,
			lastChecked: &t0,
			lastSuccess: &t1,
			lastFailure: &t2,
			mutex:       &sync.RWMutex{},
		}

		Convey("When getting the name", func() {
			name := state.Name()

			Convey("Then the correct name should be returned", func() {
				So(name, ShouldEqual, state.name)
			})
		})

		Convey("When getting the status", func() {
			status := state.Status()

			Convey("Then the correct status should be returned", func() {
				So(status, ShouldEqual, state.status)
			})
		})

		Convey("When getting the message", func() {
			message := state.Message()

			Convey("Then the correct message should be returned", func() {
				So(message, ShouldEqual, state.message)
			})
		})

		Convey("When getting the status code", func() {
			statusCode := state.StatusCode()

			Convey("Then the correct status code should be returned", func() {
				So(statusCode, ShouldEqual, state.statusCode)
			})
		})

		Convey("When getting the last checked time", func() {
			lastChecked := state.LastChecked()

			Convey("Then the correct time should be returned", func() {
				So(lastChecked, ShouldResemble, state.lastChecked)
			})
		})

		Convey("When getting the last success time", func() {
			lastSuccess := state.LastSuccess()

			Convey("Then the correct time should be returned", func() {
				So(lastSuccess, ShouldResemble, state.lastSuccess)
			})
		})

		Convey("When getting the last failure time", func() {
			lastFailure := state.LastFailure()

			Convey("Then the correct time should be returned", func() {
				So(lastFailure, ShouldResemble, state.lastFailure)
			})
		})
	})

	Convey("Given an unpopulated check state", t, func() {
		state := CheckState{
			mutex: &sync.RWMutex{},
		}

		Convey("When getting the last checked time", func() {
			lastChecked := state.LastChecked()

			Convey("Then nil should be returned", func() {
				So(lastChecked, ShouldBeNil)
			})
		})

		Convey("When getting the last success time", func() {
			lastSuccess := state.LastSuccess()

			Convey("Then nil should be returned", func() {
				So(lastSuccess, ShouldBeNil)
			})
		})

		Convey("When getting the last failure time", func() {
			lastFailure := state.LastFailure()

			Convey("Then nil should be returned", func() {
				So(lastFailure, ShouldBeNil)
			})
		})
	})
}

func TestJSONMarshalling(t *testing.T) {
	Convey("Given a new check with a populated state", t, func() {
		t0 := time.Unix(0, 0).UTC()
		t1 := t0.Add(1 * time.Minute)
		t2 := t0.Add(2 * time.Minute)
		t3 := t0.Add(3 * time.Minute)
		checkerFunc := func(ctx context.Context, state *CheckState) error {
			return nil
		}
		check, err := NewCheck("some check", checkerFunc)
		check.state.status = "OK"
		check.state.message = "this is a message"
		check.state.statusCode = 200
		check.state.lastChecked = &t1
		check.state.lastSuccess = &t2
		check.state.lastFailure = &t3

		So(err, ShouldBeNil)
		Convey("When marshalling to json", func() {
			j, err := json.Marshal(check)

			Convey("Then the string form of the state should successfully marshal", func() {
				So(err, ShouldBeNil)
				So(string(j), ShouldEqual, "{\"name\":\"some check\",\"status\":\"OK\",\"status_code\":200,\"message\":\"this is a message\",\"last_checked\":\"1970-01-01T00:01:00Z\",\"last_success\":\"1970-01-01T00:02:00Z\",\"last_failure\":\"1970-01-01T00:03:00Z\"}")
			})

			Convey("When unmarshalling from json to an empty Check", func() {
				check2 := &Check{}

				So(err, ShouldBeNil)

				err = json.Unmarshal(j, check2)

				So(err, ShouldBeNil)
				So(check2.state.name, ShouldEqual, check.state.name)
				So(check2.state.status, ShouldEqual, check.state.status)
				So(check2.state.statusCode, ShouldEqual, check.state.statusCode)
				So(check2.state.message, ShouldEqual, check.state.message)
				So(*check2.state.lastChecked, ShouldEqual, *check.state.lastChecked)
				So(*check2.state.lastFailure, ShouldEqual, *check.state.lastFailure)
				So(*check2.state.lastSuccess, ShouldEqual, *check.state.lastSuccess)
			})
		})
	})

	Convey("Given a populated check state", t, func() {
		t0 := time.Unix(0, 0).UTC()
		t1 := t0.Add(1 * time.Minute)
		t2 := t0.Add(2 * time.Minute)
		t3 := t0.Add(3 * time.Minute)
		state := NewCheckState("some check")
		state.status = "OK"
		state.message = "this is a message"
		state.statusCode = 200
		state.lastChecked = &t1
		state.lastSuccess = &t2
		state.lastFailure = &t3

		Convey("When marshalling to json", func() {
			j, err := json.Marshal(state)

			Convey("Then the string form of the state should successfully marshal", func() {
				So(err, ShouldBeNil)
				So(string(j), ShouldEqual, "{\"name\":\"some check\",\"status\":\"OK\",\"status_code\":200,\"message\":\"this is a message\",\"last_checked\":\"1970-01-01T00:01:00Z\",\"last_success\":\"1970-01-01T00:02:00Z\",\"last_failure\":\"1970-01-01T00:03:00Z\"}")
			})

			Convey("When unmarshalling from json to an empty CheckState", func() {
				state2 := &CheckState{}

				err := json.Unmarshal(j, state2)

				So(err, ShouldBeNil)
				So(state2.name, ShouldEqual, state.name)
				So(state2.status, ShouldEqual, state.status)
				So(state2.statusCode, ShouldEqual, state.statusCode)
				So(state2.message, ShouldEqual, state.message)
				So(*state2.lastChecked, ShouldEqual, *state.lastChecked)
				So(*state2.lastFailure, ShouldEqual, *state.lastFailure)
				So(*state2.lastSuccess, ShouldEqual, *state.lastSuccess)
			})
		})
	})
}
