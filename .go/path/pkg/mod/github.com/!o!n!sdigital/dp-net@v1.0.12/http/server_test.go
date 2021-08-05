package http

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"golang.org/x/net/context"
)

// listenAndServeTLSCalls keeps track of a listenAndServeTLS call
type listenAndServeTLSCalls struct {
	httpServer *http.Server
	certFile   string
	keyFile    string
}

// listenAndServeCalls keeps track of a listenAndServe call
type listenAndServeCalls struct {
	httpServer *http.Server
}

func TestNew(t *testing.T) {

	Convey("Given mocked network calls", t, func() {

		doListenAndServe = func(httpServer *http.Server) error {
			return errors.New("unexpected ListenAndServe call")
		}

		doListenAndServeTLS = func(httpServer *http.Server, certFile, keyFile string) error {
			return errors.New("unexpected ListenAndServeTLS call")

		}

		doShutdown = func(ctx context.Context, httpServer *http.Server) error {
			return errors.New("unexpected Shutdown call")

		}

		Convey("New should return a new server with sensible defaults", func() {
			h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
			s := NewServer(":0", h)

			So(s, ShouldNotBeNil)
			So(s.Handler, ShouldEqual, h)
			So(s.Alice, ShouldBeNil)
			So(s.Addr, ShouldEqual, ":0")
			So(s.MaxHeaderBytes, ShouldEqual, 0)

			Convey("TLS should not be configured by default", func() {
				So(s.CertFile, ShouldBeEmpty)
				So(s.KeyFile, ShouldBeEmpty)
			})

			Convey("Default middleware should include RequestID and Log", func() {
				So(s.middleware, ShouldContainKey, RequestIDHandlerKey)
				So(s.middleware, ShouldContainKey, LogHandlerKey)
				So(s.middlewareOrder, ShouldResemble, []string{RequestIDHandlerKey, LogHandlerKey})
			})

			Convey("Default timeouts should be sensible", func() {
				So(s.ReadTimeout, ShouldEqual, time.Second*5)
				So(s.WriteTimeout, ShouldEqual, time.Second*10)
				So(s.ReadHeaderTimeout, ShouldEqual, 0)
				So(s.IdleTimeout, ShouldEqual, 0)
			})

			Convey("Handle OS signals by default", func() {
				So(s.HandleOSSignals, ShouldEqual, true)
			})

			Convey("A default shutdown context is initialised", func() {
				So(s.DefaultShutdownTimeout, ShouldEqual, 10*time.Second)
			})
		})

		Convey("prep should prepare the server correctly", func() {
			Convey("prep should create a valid Server instance", func() {
				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				s.prep()
				So(s.Server.Addr, ShouldEqual, ":0")
			})

			Convey("invalid middleware should panic", func() {
				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				s.middlewareOrder = []string{"foo"}

				So(func() {
					s.prep()
				}, ShouldPanicWith, "middleware not found: foo")
			})

			Convey("ListenAndServe with invalid middleware should panic", func() {
				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				s.middlewareOrder = []string{"foo"}

				So(func() {
					s.ListenAndServe()
				}, ShouldPanicWith, "middleware not found: foo")
			})

			Convey("ListenAndServeTLS with invalid middleware should panic", func() {
				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				s.middlewareOrder = []string{"foo"}

				So(func() {
					s.ListenAndServeTLS("testdata/certFile", "testdata/keyFile")
				}, ShouldPanicWith, "middleware not found: foo")
			})
		})

		Convey("ListenAndServeTLS", func() {
			Convey("ListenAndServeTLS should set CertFile/KeyFile", func() {
				wg := &sync.WaitGroup{}
				calls := []listenAndServeTLSCalls{}
				doListenAndServeTLS = func(httpServer *http.Server, certFile, keyFile string) error {
					defer wg.Done()
					calls = append(calls, listenAndServeTLSCalls{
						httpServer: httpServer,
						certFile:   certFile,
						keyFile:    keyFile})
					return nil
				}

				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				// execute ListenAndServer and wait for it to finish
				wg.Add(1)
				go func() {
					s.ListenAndServeTLS("testdata/certFile", "testdata/keyFile")
				}()
				wg.Wait()

				So(s.CertFile, ShouldEqual, "testdata/certFile")
				So(s.KeyFile, ShouldEqual, "testdata/keyFile")
				So(calls, ShouldHaveLength, 1)
				So(calls[0].httpServer, ShouldNotBeNil)
				So(calls[0].certFile, ShouldEqual, "testdata/certFile")
				So(calls[0].keyFile, ShouldEqual, "testdata/keyFile")
			})

			Convey("ListenAndServeTLS with only CertFile should panic", func() {
				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				So(func() {
					s.ListenAndServeTLS("certFile", "")
				}, ShouldPanicWith, "either CertFile/KeyFile must be blank, or both provided")
			})

			Convey("ListenAndServeTLS with only KeyFile should panic", func() {
				h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
				s := NewServer(":0", h)

				So(func() {
					s.ListenAndServeTLS("", "keyFile")
				}, ShouldPanicWith, "either CertFile/KeyFile must be blank, or both provided")
			})
		})

		Convey("Given a mocked ListenAndServe", func() {
			wg := &sync.WaitGroup{}
			calls := []listenAndServeCalls{}
			doListenAndServe = func(httpServer *http.Server) error {
				defer wg.Done()
				calls = append(calls, listenAndServeCalls{httpServer: httpServer})
				return nil
			}

			h := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {})
			s := NewServer(":0", h)

			Convey("then ListenAndServe starts a working HTTP server", func() {
				So(s.HandleOSSignals, ShouldBeTrue)

				wg.Add(1)
				go func() {
					s.ListenAndServe()
				}()
				wg.Wait()

				So(calls, ShouldHaveLength, 1)
				So(calls[0].httpServer, ShouldNotBeNil)
			})

			Convey("then if HandleOSSignals is disabled, ListenAndServe starts a working HTTP server", func() {
				s.HandleOSSignals = false

				wg.Add(1)
				go func() {
					s.ListenAndServe()
				}()
				wg.Wait()

				So(calls, ShouldHaveLength, 1)
				So(calls[0].httpServer, ShouldNotBeNil)
			})
		})
	})
}
