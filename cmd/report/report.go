package main

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/boreq/db_benchmark/report"
	"github.com/boreq/errors"
	gochart "github.com/wcharczuk/go-chart/v2"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	results, err := report.GetBenchResults(os.Stdin)
	if err != nil {
		return errors.Wrap(err, "error getting bench results")
	}

	directory := path.Join(
		"results",
		fmt.Sprintf("%s-%s-%s", results.Cpu, results.Goarch, results.Goos),
	)

	if err := os.RemoveAll(directory); err != nil {
		return errors.Wrap(err, "error removing directory")
	}

	if err := os.MkdirAll(directory, 0700); err != nil {
		return errors.Wrap(err, "error recreating directory")
	}

	readmeBuffer := bytes.NewBuffer(nil)
	readmeBuffer.WriteString("# Results\n")
	readmeBuffer.WriteString("```\n")
	readmeBuffer.WriteString(fmt.Sprintf("goarch=%s\n", results.Goarch))
	readmeBuffer.WriteString(fmt.Sprintf("goos=%s\n", results.Goos))
	readmeBuffer.WriteString(fmt.Sprintf("cpu=%s\n", results.Cpu))
	readmeBuffer.WriteString("```\n")

	readmeBuffer.WriteString("## Performance\n")

	for _, result := range results.PerformanceResults {
		resultsChart, err := report.MakePerformanceResultChart(result)
		if err != nil {
			return errors.Wrap(err, "error creating chart")
		}

		filename := fmt.Sprintf(
			"%s.png",
			strings.Replace(result.BenchmarkName, string(os.PathSeparator), "-", -1),
		)

		f, err := os.Create(path.Join(directory, filename))
		if err != nil {
			return errors.Wrap(err, "error creating chart file")
		}

		if err := resultsChart.Render(gochart.PNG, f); err != nil {
			return errors.Wrap(err, "error rendering the chart")
		}

		readmeBuffer.WriteString(fmt.Sprintf("### %s\n", result.BenchmarkName))
		readmeBuffer.WriteString(fmt.Sprintf("![](./%s)\n", filename))
		readmeBuffer.WriteString("```\n")
		sort.Slice(result.Systems, func(i, j int) bool {
			return result.Systems[i].NsOp < result.Systems[j].NsOp
		})
		for _, system := range result.Systems {
			readmeBuffer.WriteString(fmt.Sprintf("%20s = %.0f ns per op\n", system.SystemName, system.NsOp))
		}
		readmeBuffer.WriteString("```\n")

	}

	readmeBuffer.WriteString("## Size\n")
	readmeBuffer.WriteString("\n")
	readmeBuffer.WriteString("Warning: bbolt metrics are not reliable as bbolt grows its file in large increments. Initially the size of the underlying file is multiplied by two and then once it is at above 1 GiB in size 1 GiB is added to it every time the database runs out of space.")
	readmeBuffer.WriteString("\n")

	for _, result := range results.SizeResults {
		resultsChart, err := report.MakeSizeResultChart(result)
		if err != nil {
			return errors.Wrap(err, "error creating chart")
		}

		filename := fmt.Sprintf(
			"%s.png",
			strings.Replace(result.BenchmarkName, string(os.PathSeparator), "-", -1),
		)

		f, err := os.Create(path.Join(directory, filename))
		if err != nil {
			return errors.Wrap(err, "error creating chart file")
		}

		if err := resultsChart.Render(gochart.PNG, f); err != nil {
			return errors.Wrap(err, "error rendering the chart")
		}

		readmeBuffer.WriteString(fmt.Sprintf("### %s\n", result.BenchmarkName))
		readmeBuffer.WriteString(fmt.Sprintf("![](./%s)\n", filename))
		readmeBuffer.WriteString("```\n")
		sort.Slice(result.Systems, func(i, j int) bool {
			return result.Systems[i].BytesOp < result.Systems[j].BytesOp
		})
		for _, system := range result.Systems {
			readmeBuffer.WriteString(fmt.Sprintf("%20s = %.0f bytes per op (n=%d)\n", system.SystemName, system.BytesOp, system.N))
		}
		readmeBuffer.WriteString("```\n")

	}

	readmeFile, err := os.Create(path.Join(directory, "README.md"))
	if err != nil {
		return errors.Wrap(err, "error creating readme")
	}

	if _, err := readmeBuffer.WriteTo(readmeFile); err != nil {
		return errors.Wrap(err, "error writing to readme file")
	}

	return nil
}
