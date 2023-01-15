package chart

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/boreq/errors"
	"github.com/wcharczuk/go-chart/v2"
	"golang.org/x/tools/benchmark/parse"
	"io"
	"sort"
	"strings"
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
	SystemName    string
	DirectorySize int64
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

	results, err := getPerformanceBenchResults(bytes.NewReader(b))
	if err != nil {
		return BenchResults{}, errors.Wrap(err, "error getting results")
	}

	result.PerformanceResults = results

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

			bench, ok := findBenchmark(results, benchmarkName)
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

func MakeResultChart(result PerformanceBenchResult) (chart.BarChart, error) {
	graph := chart.BarChart{
		Title: result.BenchmarkName,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   512,
		BarWidth: 100,
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

func ParsePerformanceBenchmarkName(name string) (string, string, error) {
	split := strings.SplitN(name, "/", 3)
	if len(split) != 3 {
		return "", "", errors.New("invalid name")
	}

	return split[1], split[2], nil
}

func findBenchmark(results []PerformanceBenchResult, benchmarkName string) (*PerformanceBenchResult, bool) {
	for i := range results {
		if results[i].BenchmarkName == benchmarkName {
			return &results[i], true
		}
	}
	return nil, false
}
