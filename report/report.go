package report

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/boreq/errors"
	"github.com/wcharczuk/go-chart/v2"
	"golang.org/x/tools/benchmark/parse"
)

type BenchResults struct {
	Goos               string
	Goarch             string
	Cpu                string
	PerformanceResults []PerformanceBenchResult
	SizeResults        []SizeBenchResult
}

type PerformanceBenchResult struct {
	BenchmarkName string
	Systems       []SystemPerformanceBenchResult
}

type SizeBenchResult struct {
	BenchmarkName string
	Systems       []SystemSizeBenchResult
}

type SystemPerformanceBenchResult struct {
	SystemName string
	NsOp       float64
}

type SystemSizeBenchResult struct {
	SystemName string
	N          int64
	BytesOp    float64
}

func GetBenchResults(r io.Reader) (BenchResults, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return BenchResults{}, errors.Wrap(err, "error reading all")
	}

	var result BenchResults

	scan := bufio.NewScanner(bytes.NewReader(b))
	for scan.Scan() {
		parseLine(scan.Text(), &result)
	}

	if err := scan.Err(); err != nil {
		return BenchResults{}, errors.Wrap(err, "scan error")
	}

	if result.Cpu == "" || result.Goarch == "" || result.Goos == "" {
		return BenchResults{}, fmt.Errorf("missing execution environment info in output: '%+v'", result)
	}

	performanceResults, err := getPerformanceBenchResults(bytes.NewReader(b))
	if err != nil {
		return BenchResults{}, errors.Wrap(err, "error getting performance results")
	}

	sizeResults, err := getSizeBenchResults(bytes.NewReader(b))
	if err != nil {
		return BenchResults{}, errors.Wrap(err, "error getting size results")
	}

	result.PerformanceResults = performanceResults
	result.SizeResults = sizeResults

	return result, err
}

const lineSep = ":"

func parseLine(line string, result *BenchResults) error {
	splitLine := strings.SplitN(line, lineSep, 2)
	if len(splitLine) != 2 {
		return errors.New("invalid number of strings")
	}

	key := splitLine[0]
	value := strings.TrimSpace(splitLine[1])

	switch key {
	case "goos":
		result.Goos = value
	case "goarch":
		result.Goarch = value
	case "cpu":
		result.Cpu = value
	default:
		return errors.New("unknown line")
	}

	return nil
}

func getPerformanceBenchResults(r io.Reader) ([]PerformanceBenchResult, error) {
	var results []PerformanceBenchResult

	set, err := parse.ParseSet(r)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing set")
	}

	for _, benchmarks := range set {
		for _, benchmark := range benchmarks {
			if !strings.HasPrefix(benchmark.Name, "BenchmarkPerformance") {
				continue
			}

			systemName, benchmarkName, err := ParsePerformanceBenchmarkName(benchmark.Name)
			if err != nil {
				return nil, errors.Wrap(err, "error parsing benchmark name")
			}

			bench, ok := findPerformanceBenchmark(results, benchmarkName)
			if !ok {
				results = append(results, PerformanceBenchResult{
					BenchmarkName: benchmarkName,
					Systems:       nil,
				})
				bench = &results[len(results)-1]
			}

			bench.Systems = append(bench.Systems, SystemPerformanceBenchResult{
				SystemName: systemName,
				NsOp:       benchmark.NsPerOp,
			})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].BenchmarkName < results[j].BenchmarkName
	})

	for _, result := range results {
		sort.Slice(result.Systems, func(i, j int) bool {
			return result.Systems[i].SystemName < result.Systems[j].SystemName
		})
	}

	return results, nil
}

func getSizeBenchResults(r io.Reader) ([]SizeBenchResult, error) {
	var results []SizeBenchResult

	scan := bufio.NewScanner(r)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) != 4 {
			continue
		}

		benchName := fields[0]
		benchN := fields[1]
		benchValue := fields[2]
		benchUnit := fields[3]

		if !strings.HasPrefix(benchName, "BenchmarkSize") {
			continue
		}

		if benchUnit != "bytes/op" {
			return nil, errors.New("invalid unit")
		}

		systemName, benchmarkName, err := ParseSizeBenchmarkName(benchName)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing benchmark name")
		}

		bench, ok := findSizeBenchmark(results, benchmarkName)
		if !ok {
			results = append(results, SizeBenchResult{
				BenchmarkName: benchmarkName,
				Systems:       nil,
			})
			bench = &results[len(results)-1]
		}

		f, err := strconv.ParseFloat(benchValue, 64)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing value")
		}

		n, err := strconv.ParseInt(benchN, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing n")
		}

		bench.Systems = append(bench.Systems, SystemSizeBenchResult{
			SystemName: systemName,
			N:          n,
			BytesOp:    f,
		})
	}

	if err := scan.Err(); err != nil {
		return nil, errors.Wrap(err, "scan error")
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].BenchmarkName < results[j].BenchmarkName
	})

	for _, result := range results {
		sort.Slice(result.Systems, func(i, j int) bool {
			return result.Systems[i].SystemName < result.Systems[j].SystemName
		})
	}

	return results, nil
}

const (
	chartWidth    = 2000
	chartBarWidth = 300
)

func MakePerformanceResultChart(result PerformanceBenchResult) (chart.BarChart, error) {
	graph := chart.BarChart{
		Title: result.BenchmarkName,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   512,
		BarWidth: chartBarWidth,
		Width:    chartWidth,
		YAxis: chart.YAxis{
			Name: "ns per op",
			Range: &chart.ContinuousRange{
				Min: 0,
				Max: 0,
			},
		},
	}

	for _, system := range result.Systems {
		graph.Bars = append(graph.Bars, chart.Value{
			Label: system.SystemName,
			Value: system.NsOp,
		})

		if v := system.NsOp * 1.1; v > graph.YAxis.Range.GetMax() {
			graph.YAxis.Range.SetMax(v)
		}
	}

	return graph, nil
}

func MakeSizeResultChart(result SizeBenchResult) (chart.BarChart, error) {
	graph := chart.BarChart{
		Title: result.BenchmarkName,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   512,
		BarWidth: chartBarWidth,
		Width:    chartWidth,
		YAxis: chart.YAxis{
			Name: "bytes per op",
			Range: &chart.ContinuousRange{
				Min: 0,
				Max: 0,
			},
		},
	}

	for _, system := range result.Systems {
		graph.Bars = append(graph.Bars, chart.Value{
			Label: system.SystemName,
			Value: system.BytesOp,
		})

		if v := system.BytesOp * 1.1; v > graph.YAxis.Range.GetMax() {
			graph.YAxis.Range.SetMax(v)
		}
	}

	return graph, nil
}

func ParsePerformanceBenchmarkName(name string) (string, string, error) {
	split := strings.SplitN(name, "/", 3)
	if len(split) != 3 {
		return "", "", errors.New("invalid name")
	}

	return split[1], split[2], nil
}

func ParseSizeBenchmarkName(name string) (string, string, error) {
	split := strings.SplitN(name, "/", 3)
	if len(split) != 3 {
		return "", "", errors.New("invalid name")
	}

	return split[1], split[2], nil
}

func findPerformanceBenchmark(results []PerformanceBenchResult, benchmarkName string) (*PerformanceBenchResult, bool) {
	for i := range results {
		if results[i].BenchmarkName == benchmarkName {
			return &results[i], true
		}
	}
	return nil, false
}

func findSizeBenchmark(results []SizeBenchResult, benchmarkName string) (*SizeBenchResult, bool) {
	for i := range results {
		if results[i].BenchmarkName == benchmarkName {
			return &results[i], true
		}
	}
	return nil, false
}
