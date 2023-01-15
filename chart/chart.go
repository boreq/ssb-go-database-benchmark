package chart

import (
	"fmt"
	"github.com/boreq/errors"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"github.com/wcharczuk/go-chart"
	"golang.org/x/tools/benchmark/parse"
	"io"
	"math/rand"
	"strings"
)

type BenchResult struct {
	BenchmarkName string
	Systems       []SystemBenchResult
}

type SystemBenchResult struct {
	SystemName string
	NsOp       float64
}

func GetBenchResults(r io.Reader) ([]BenchResult, error) {
	var results []BenchResult

	set, err := parse.ParseSet(r)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing set")
	}

	for _, benchmarks := range set {
		for _, benchmark := range benchmarks {
			systemName, benchmarkName, err := ParseBenchmarkName(benchmark.Name)
			if err != nil {
				return nil, errors.Wrap(err, "error parsing benchmark name")
			}

			bench, ok := findBenchmark(results, benchmarkName)
			if !ok {
				results = append(results, BenchResult{
					BenchmarkName: benchmarkName,
					Systems:       nil,
				})
				bench = &results[len(results)-1]
			}

			bench.Systems = append(bench.Systems, SystemBenchResult{
				SystemName: systemName,
				NsOp:       benchmark.NsPerOp,
			})
		}
	}

	return results, nil
}

func MakeResultChart(result BenchResult) (chart.BarChart, error) {
	//graph := chart.Chart{
	//	Series: []chart.Series{
	//		chart.ContinuousSeries{
	//			XValues: []float64{1.0, 2.0, 3.0, 4.0},
	//			YValues: []float64{1.0, 2.0, 3.0, 4.0},
	//		},
	//	},
	//}

	graph := chart.BarChart{
		Title: "Test Bar Chart",
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Height:   512,
		BarWidth: 60,
		Bars: []chart.Value{
			{Value: 5.25, Label: "Blue"},
			{Value: 4.88, Label: "Green"},
			{Value: 4.74, Label: "Gray"},
			{Value: 3.22, Label: "Orange"},
			{Value: 3, Label: "Test"},
			{Value: 2.27, Label: "??"},
			{Value: 1, Label: "!!"},
		},
	}

	return graph, nil

	//bar := charts.NewBar()
	//
	//bar.SetGlobalOptions(
	//	charts.WithInitializationOpts(opts.Initialization{
	//		Width:      "2000px",
	//		Height:     "1000px",
	//	}),
	//	charts.WithTitleOpts(opts.Title{
	//		Title: result.BenchmarkName,
	//	}),
	//	charts.WithXAxisOpts(opts.XAxis{
	//		Name:        "Benchmark name",
	//		Type:        "",
	//		Show:        false,
	//		Data:        nil,
	//		SplitNumber: 0,
	//		Scale:       false,
	//		Min:         nil,
	//		Max:         nil,
	//		GridIndex:   0,
	//		SplitArea:   nil,
	//		SplitLine:   nil,
	//		AxisLabel: &opts.AxisLabel{
	//			Show:          true,
	//			Interval:      "",
	//			Inside:        false,
	//			Rotate:        90,
	//			Margin:        0,
	//			Formatter:     "",
	//			ShowMinLabel:  true,
	//			ShowMaxLabel:  true,
	//			Color:         "",
	//			FontStyle:     "",
	//			FontWeight:    "",
	//			FontFamily:    "",
	//			FontSize:      "",
	//			Align:         "",
	//			VerticalAlign: "",
	//			LineHeight:    "",
	//		},
	//	}),
	//	charts.WithYAxisOpts(opts.YAxis{
	//		Name:        "Ns per op",
	//		Type:        "",
	//		Show:        false,
	//		Data:        nil,
	//		SplitNumber: 0,
	//		Scale:       false,
	//		Min:         nil,
	//		Max:         nil,
	//		GridIndex:   0,
	//		SplitArea:   nil,
	//		SplitLine:   nil,
	//		AxisLabel:   nil,
	//	}),
	//	charts.WithLegendOpts(opts.Legend{
	//		Show:          true,
	//		Left:          "",
	//		Top:           "",
	//		Right:         "",
	//		Bottom:        "",
	//		Data:          nil,
	//		Orient:        "vertical",
	//		InactiveColor: "",
	//		Selected:      nil,
	//		SelectedMode:  "",
	//		Padding:       nil,
	//		ItemWidth:     0,
	//		ItemHeight:    0,
	//		X:             "",
	//		Y:             "",
	//		Width:         "",
	//		Height:        "",
	//		Align:         "right",
	//		TextStyle:     nil,
	//	}),
	//)
	//
	////var xaxisLabels []string
	////for _, system := range result.Systems {
	////	xaxisLabels = append(xaxisLabels, system..BenchmarkName)
	////	fmt.Println("benchmark", result.BenchmarkName)
	////
	////	for _, system := range result.Systems {
	////		fmt.Println("system", system.SystemName, "nsperop", system.NsOp)
	////
	////	}
	////}
	//
	//categories := make(map[string][]opts.BarData)
	//
	////for _, result := range results {
	//	for _, system := range result.Systems {
	//		categories[system.SystemName] = append(categories[system.SystemName], opts.BarData{
	//			Name:  "NsOp",
	//			Value: system.NsOp,
	//			Tooltip: &opts.Tooltip{
	//				Show: true,
	//			},
	//		})
	//	}
	////
	////}
	//
	//xaxis := bar.SetXAxis([]string{result.BenchmarkName})
	//for categoryName, barData := range categories {
	//	xaxis.AddSeries(categoryName, barData)
	//}
	//
	////xaxis.SetGlobalOptions(
	////	charts.WithXAxisOpts(),
	////	)
	//
	//return bar, nil
}


func MakeChart(results []BenchResult) (*charts.Bar, error) {
	bar := charts.NewBar()

	bar.SetGlobalOptions(
		charts.WithInitializationOpts(opts.Initialization{
			Width:      "2000px",
			Height:     "1000px",
		}),
		charts.WithTitleOpts(opts.Title{
			Title: "My first bar chart generated by go-echarts",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name:        "Benchmark name",
			Type:        "",
			Show:        true,
			Data:        nil,
			SplitNumber: 0,
			Scale:       false,
			Min:         nil,
			Max:         nil,
			GridIndex:   0,
			SplitArea:   nil,
			SplitLine:   nil,
			AxisLabel: &opts.AxisLabel{
				Show:          true,
				Interval:      "",
				Inside:        false,
				Rotate:        90,
				Margin:        0,
				Formatter:     "",
				ShowMinLabel:  true,
				ShowMaxLabel:  true,
				Color:         "",
				FontStyle:     "",
				FontWeight:    "",
				FontFamily:    "",
				FontSize:      "",
				Align:         "",
				VerticalAlign: "",
				LineHeight:    "",
			},
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Name:        "Ns per op",
			Type:        "",
			Show:        false,
			Data:        nil,
			SplitNumber: 0,
			Scale:       false,
			Min:         nil,
			Max:         nil,
			GridIndex:   0,
			SplitArea:   nil,
			SplitLine:   nil,
			AxisLabel:   nil,
		}),
		charts.WithLegendOpts(opts.Legend{
			Show:          true,
			Left:          "",
			Top:           "",
			Right:         "",
			Bottom:        "",
			Data:          nil,
			Orient:        "vertical",
			InactiveColor: "",
			Selected:      nil,
			SelectedMode:  "",
			Padding:       nil,
			ItemWidth:     0,
			ItemHeight:    0,
			X:             "",
			Y:             "",
			Width:         "",
			Height:        "",
			Align:         "right",
			TextStyle:     nil,
		}),
	)

	var xaxisLabels []string
	for _, result := range results {
		xaxisLabels = append(xaxisLabels, result.BenchmarkName)
		fmt.Println("benchmark", result.BenchmarkName)

		for _, system := range result.Systems {
			fmt.Println("system", system.SystemName, "nsperop", system.NsOp)

		}
	}

	categories := make(map[string][]opts.BarData)

	for _, result := range results {
		for _, system := range result.Systems {
			categories[system.SystemName] = append(categories[system.SystemName], opts.BarData{
				Name:  "NsOp",
				Value: system.NsOp,
				Tooltip: &opts.Tooltip{
					Show: true,
				},
			})
		}

	}

	xaxis := bar.SetXAxis(xaxisLabels)
	for categoryName, barData := range categories {
		xaxis.AddSeries(categoryName, barData)
	}

	//xaxis.SetGlobalOptions(
	//	charts.WithXAxisOpts(),
	//	)

	return bar, nil
}

func generateBarItems() []opts.BarData {
	items := make([]opts.BarData, 0)
	for i := 0; i < 7; i++ {
		items = append(items, opts.BarData{Value: rand.Intn(300)})
	}
	return items
}

func ParseBenchmarkName(name string) (string, string, error) {
	split := strings.Split(name, "/")
	if len(split) != 3 {
		return "", "", errors.New("invalid name")
	}

	return split[1], split[2], nil
}

func findBenchmark(results []BenchResult, benchmarkName string) (*BenchResult, bool) {
	for i := range results {
		if results[i].BenchmarkName == benchmarkName {
			return &results[i], true
		}
	}
	return nil, false
}
