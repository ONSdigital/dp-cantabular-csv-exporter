package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/ONSdigital/dp-cantabular-csv-exporter/config"
	"github.com/ONSdigital/dp-cantabular-csv-exporter/features/steps"
	componenttest "github.com/ONSdigital/dp-component-test"
	dplogs "github.com/ONSdigital/log.go/v2/log"
	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"
)

const componentLogFile = "component-output.txt"

var componentFlag = flag.Bool("component", false, "perform component tests")

type ComponentTest struct {
	MongoFeature *componenttest.MongoFeature
	t            *testing.T
}

func (f *ComponentTest) InitializeScenario(godogCtx *godog.ScenarioContext) {
	component := steps.NewComponent(f.t)

	godogCtx.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		if err := component.Reset(); err != nil {
			return ctx, fmt.Errorf("unable to initialise scenario: %s", err)
		}
		return ctx, nil
	})

	godogCtx.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		component.Close()
		return ctx, nil
	})

	component.RegisterSteps(godogCtx)
}

func (f *ComponentTest) InitializeTestSuite(_ *godog.TestSuiteContext) {
	dplogs.Namespace = "dp-cantabular-csv-exporter"
}

func TestComponent(t *testing.T) {
	if *componentFlag {
		status := 0

		cfg, err := config.Get()
		if err != nil {
			t.Fatalf("failed to get service config: %s", err)
		}

		var output io.Writer = os.Stdout

		if cfg.ComponentTestUseLogFile {
			logfile, err := os.Create(componentLogFile)
			if err != nil {
				t.Fatalf("could not create logs file: %s", err)
			}

			defer func() {
				if err := logfile.Close(); err != nil {
					t.Fatalf("failed to close logs file: %s", err)
				}
			}()
			output = logfile

			dplogs.SetDestination(logfile, nil)
		}

		var opts = godog.Options{
			Output: colors.Colored(output),
			Format: "pretty",
			Paths:  flag.Args(),
		}

		f := &ComponentTest{t: t}

		status = godog.TestSuite{
			Name:                 "feature_tests",
			ScenarioInitializer:  f.InitializeScenario,
			TestSuiteInitializer: f.InitializeTestSuite,
			Options:              &opts,
		}.Run()

		if status > 0 {
			t.Fail()
		}
	} else {
		t.Skip("component flag required to run component tests")
	}
}
