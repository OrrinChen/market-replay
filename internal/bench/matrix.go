package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/orynwilder/market-replay-service/internal/parser"
	"github.com/orynwilder/market-replay-service/internal/workload"
)

const (
	MatrixModeFixture   = "fixture"
	MatrixModeGenerated = "generated"

	DefaultMatrixOutputDir     = "tmp/bench-matrix"
	DefaultMatrixRuns          = 3
	DefaultMatrixWorkloadBytes = int64(64 * 1024)
)

const (
	MatrixFormatJSONL = parser.FormatJSONL
	MatrixFormatCSV   = parser.FormatCSV
)

type MatrixOptions struct {
	RootDir       string
	OutputDir     string
	Runs          int
	WorkloadBytes int64
	Symbol        string
}

type MatrixResult struct {
	Name             string               `json:"name"`
	Mode             string               `json:"mode"`
	Format           string               `json:"format"`
	SourcePath       string               `json:"source_path"`
	WorkloadFilePath string               `json:"workload_file_path"`
	WorkloadBytes    int64                `json:"workload_bytes"`
	Runs             []event.ReplayMetric `json:"runs"`
	Median           event.ReplayMetric   `json:"median"`
}

type matrixCase struct {
	name       string
	mode       string
	format     parser.Format
	sourcePath string
	outputName string
}

func RunMatrix(opts MatrixOptions) ([]MatrixResult, error) {
	opts = normalizeMatrixOptions(opts)
	cases := []matrixCase{
		{
			name:       "fixture-jsonl-btcusdt-depth",
			mode:       MatrixModeFixture,
			format:     parser.FormatJSONL,
			sourcePath: filepath.Join("testdata", "btcusdt_depth.jsonl"),
		},
		{
			name:       "fixture-csv-solusdt-aggtrade",
			mode:       MatrixModeFixture,
			format:     parser.FormatCSV,
			sourcePath: filepath.Join("testdata", "solusdt_aggtrade.csv"),
		},
		{
			name:       "generated-jsonl-btcusdt-depth",
			mode:       MatrixModeGenerated,
			format:     parser.FormatJSONL,
			sourcePath: filepath.Join("testdata", "btcusdt_depth.jsonl"),
			outputName: "btcusdt_depth.jsonl",
		},
		{
			name:       "generated-csv-solusdt-aggtrade",
			mode:       MatrixModeGenerated,
			format:     parser.FormatCSV,
			sourcePath: filepath.Join("testdata", "solusdt_aggtrade.csv"),
			outputName: "solusdt_aggtrade.csv",
		},
	}

	results := make([]MatrixResult, 0, len(cases))
	for _, c := range cases {
		sourcePath := matrixPath(opts.RootDir, c.sourcePath)
		path := sourcePath
		if c.mode == MatrixModeGenerated {
			if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
				return nil, err
			}
			path = filepath.Join(opts.OutputDir, c.outputName)
			if _, err := workload.GenerateFile(workload.GenerateOptions{
				SourcePath:  sourcePath,
				OutputPath:  path,
				TargetBytes: opts.WorkloadBytes,
			}); err != nil {
				return nil, fmt.Errorf("generate %s: %w", c.name, err)
			}
		}

		runs := make([]event.ReplayMetric, 0, opts.Runs)
		for i := 0; i < opts.Runs; i++ {
			metric, err := File(path, c.format, opts.Symbol)
			if err != nil {
				return nil, fmt.Errorf("bench %s run %d: %w", c.name, i+1, err)
			}
			runs = append(runs, metric)
		}

		size, err := fileSize(path)
		if err != nil {
			return nil, err
		}
		results = append(results, MatrixResult{
			Name:             c.name,
			Mode:             c.mode,
			Format:           string(c.format),
			SourcePath:       c.sourcePath,
			WorkloadFilePath: path,
			WorkloadBytes:    size,
			Runs:             runs,
			Median:           medianRun(runs),
		})
	}
	return results, nil
}

func normalizeMatrixOptions(opts MatrixOptions) MatrixOptions {
	if opts.RootDir == "" {
		opts.RootDir = "."
	}
	if opts.OutputDir == "" {
		opts.OutputDir = DefaultMatrixOutputDir
	}
	if opts.Runs <= 0 {
		opts.Runs = DefaultMatrixRuns
	}
	if opts.WorkloadBytes <= 0 {
		opts.WorkloadBytes = DefaultMatrixWorkloadBytes
	}
	return opts
}

func matrixPath(root, path string) string {
	if filepath.IsAbs(path) || root == "" || root == "." {
		return path
	}
	return filepath.Join(root, path)
}

func medianRun(runs []event.ReplayMetric) event.ReplayMetric {
	if len(runs) == 0 {
		return event.ReplayMetric{}
	}
	cp := append([]event.ReplayMetric(nil), runs...)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].RowsPerSecond < cp[j].RowsPerSecond
	})
	return cp[len(cp)/2]
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
