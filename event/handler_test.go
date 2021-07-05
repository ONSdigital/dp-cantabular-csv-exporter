package event_test

import (
	"os"
	"testing"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/event"
	. "github.com/smartystreets/goconvey/convey"
)

// TODO: remove hello called example test
func TestInstanceCompleteHandler_Handle(t *testing.T) {

	Convey("Given a successful event handler, when Handle is triggered", t, func() {
		eventHandler := &event.InstanceCompleteHandler{}
		filePath := "/tmp/helloworld.txt"
		os.Remove(filePath)
		err := eventHandler.Handle(testCtx, &config.Config{OutputFilePath: filePath}, &testEvent)
		So(err, ShouldBeNil)
	})

	Convey("handler returns an error when cannot write to file", t, func() {
		eventHandler := &event.InstanceCompleteHandler{}
		filePath := ""
		err := eventHandler.Handle(testCtx, &config.Config{OutputFilePath: filePath}, &testEvent)
		So(err, ShouldNotBeNil)
	})
}
