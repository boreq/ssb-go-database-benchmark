package main

import (
	"fmt"
	"github.com/boreq/db_benchmark/chart"
	"github.com/boreq/errors"
	gochart "github.com/wcharczuk/go-chart"
	"os"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	results, err := chart.GetBenchResults(os.Stdin)
	if err != nil {
		return errors.Wrap(err, "error getting bench results")
	}

	for _, result := range results {
		resultsChart, err := chart.MakeResultChart(result)
		if err != nil {
			return errors.Wrap(err, "error creating chart")
		}

		f, err := os.Create(fmt.Sprintf("results/%s.png", result.BenchmarkName))
		if err != nil {
			return errors.Wrap(err, "error creating chart file")
		}

		if err := resultsChart.Render(gochart.PNG, f); err != nil {
			return errors.Wrap(err, "error rendering the chart")
		}
	}

	return nil
}
