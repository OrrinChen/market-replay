package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/orynwilder/market-replay-service/internal/bench"
	"github.com/orynwilder/market-replay-service/internal/event"
	"github.com/orynwilder/market-replay-service/internal/parser"
	"github.com/orynwilder/market-replay-service/internal/replay"
	"github.com/orynwilder/market-replay-service/internal/servicebench"
	"github.com/orynwilder/market-replay-service/internal/validate"
	"github.com/orynwilder/market-replay-service/internal/workload"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return flag.ErrHelp
	}

	switch args[0] {
	case "validate":
		return runValidate(args[1:])
	case "bench":
		return runBench(args[1:])
	case "bench-matrix":
		return runBenchMatrix(args[1:])
	case "db-bench":
		return runDBBench(args[1:])
	case "queue-bench":
		return runQueueBench(args[1:])
	case "replay":
		return runReplay(args[1:])
	case "generate-workload":
		return runGenerateWorkload(args[1:])
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runQueueBench(args []string) error {
	fs := flag.NewFlagSet("queue-bench", flag.ContinueOnError)
	redisAddr := fs.String("redis-addr", os.Getenv("REDIS_ADDR"), "Redis address")
	jobs := fs.Int("jobs", servicebench.DefaultQueueJobs, "jobs to enqueue into an isolated benchmark queue")
	queueName := fs.String("queue", "", "optional isolated benchmark queue name")
	jsonOut := fs.Bool("json", false, "emit raw JSON metric instead of Markdown table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *redisAddr == "" {
		return fmt.Errorf("queue-bench requires --redis-addr or REDIS_ADDR")
	}
	if *jobs <= 0 {
		return fmt.Errorf("--jobs must be positive")
	}

	result, err := servicebench.RunQueue(context.Background(), servicebench.QueueOptions{
		RedisAddr: *redisAddr,
		Jobs:      *jobs,
		QueueName: *queueName,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(result)
	}
	printQueueBenchTable(result)
	return nil
}

func runDBBench(args []string) error {
	fs := flag.NewFlagSet("db-bench", flag.ContinueOnError)
	databaseURL := fs.String("database-url", os.Getenv("DATABASE_URL"), "Postgres database URL")
	rows := fs.Int("rows", servicebench.DefaultDBRows, "rows for lookup benchmark")
	lookups := fs.Int("lookups", servicebench.DefaultDBLookups, "lookup count for indexed/no-index comparison")
	insertRows := fs.Int("insert-rows", servicebench.DefaultDBInsertRows, "rows for row insert vs COPY insert comparison")
	jsonOut := fs.Bool("json", false, "emit raw JSON metric instead of Markdown table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *databaseURL == "" {
		return fmt.Errorf("db-bench requires --database-url or DATABASE_URL")
	}
	if *rows <= 0 || *lookups <= 0 || *insertRows <= 0 {
		return fmt.Errorf("--rows, --lookups, and --insert-rows must be positive")
	}

	result, err := servicebench.RunDB(context.Background(), servicebench.DBOptions{
		DatabaseURL: *databaseURL,
		Rows:        *rows,
		Lookups:     *lookups,
		InsertRows:  *insertRows,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(result)
	}
	printDBBenchTable(result)
	return nil
}

func runValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	file := fs.String("file", "", "market event file path")
	format := fs.String("format", "auto", "input format: auto, jsonl, csv")
	symbol := fs.String("symbol", "", "optional symbol filter")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("validate requires --file")
	}

	resolved, err := parser.ResolveFormat(*file, parser.Format(*format))
	if err != nil {
		return err
	}
	result, err := validate.File(*file, resolved, validate.Options{Symbol: *symbol})
	if err != nil {
		return err
	}
	return writeJSON(result)
}

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	file := fs.String("file", "", "market event file path")
	format := fs.String("format", "auto", "input format: auto, jsonl, csv")
	symbol := fs.String("symbol", "", "optional symbol filter")
	jsonOut := fs.Bool("json", false, "emit raw JSON metric instead of Markdown table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("bench requires --file")
	}

	resolved, err := parser.ResolveFormat(*file, parser.Format(*format))
	if err != nil {
		return err
	}
	metric, err := bench.File(*file, resolved, *symbol)
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(metric)
	}
	printBenchTable(metric)
	return nil
}

func runBenchMatrix(args []string) error {
	fs := flag.NewFlagSet("bench-matrix", flag.ContinueOnError)
	runs := fs.Int("runs", bench.DefaultMatrixRuns, "number of repeated runs per matrix row")
	bytes := fs.Int64("bytes", bench.DefaultMatrixWorkloadBytes, "target bytes for each generated workload")
	outputDir := fs.String("output-dir", bench.DefaultMatrixOutputDir, "directory for generated matrix workloads")
	symbol := fs.String("symbol", "", "optional symbol filter")
	jsonOut := fs.Bool("json", false, "emit raw JSON matrix instead of Markdown table")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runs <= 0 {
		return fmt.Errorf("--runs must be positive")
	}
	if *bytes <= 0 {
		return fmt.Errorf("--bytes must be positive")
	}

	results, err := bench.RunMatrix(bench.MatrixOptions{
		OutputDir:     *outputDir,
		Runs:          *runs,
		WorkloadBytes: *bytes,
		Symbol:        *symbol,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		return writeJSON(results)
	}
	printBenchMatrixTable(results)
	return nil
}

func runReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	file := fs.String("file", "", "market event file path")
	format := fs.String("format", "auto", "input format: auto, jsonl, csv")
	symbol := fs.String("symbol", "", "optional symbol filter")
	speedValue := fs.String("speed", "max", "replay speed: max, 1x, 10x")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return fmt.Errorf("replay requires --file")
	}

	resolved, err := parser.ResolveFormat(*file, parser.Format(*format))
	if err != nil {
		return err
	}
	speed, err := replay.ParseSpeed(*speedValue)
	if err != nil {
		return err
	}
	summary, err := replay.File(*file, resolved, *symbol, speed)
	if err != nil {
		return err
	}
	return writeJSON(summary)
}

func runGenerateWorkload(args []string) error {
	fs := flag.NewFlagSet("generate-workload", flag.ContinueOnError)
	source := fs.String("source", "", "source fixture path")
	output := fs.String("output", "", "output workload path")
	targetBytes := fs.Int64("bytes", 10*1024*1024, "target output bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *source == "" || *output == "" {
		return fmt.Errorf("generate-workload requires --source and --output")
	}
	if *targetBytes <= 0 {
		return fmt.Errorf("--bytes must be positive")
	}
	result, err := workload.GenerateFile(workload.GenerateOptions{
		SourcePath:  *source,
		OutputPath:  *output,
		TargetBytes: *targetBytes,
	})
	if err != nil {
		return err
	}
	return writeJSON(result)
}

func writeJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func printBenchTable(row event.ReplayMetric) {
	fmt.Println("| workload | rows | events | malformed | gaps | duration | rows/sec | events/sec | p95 latency | peak alloc MB | allocs/event |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	fmt.Printf("| %s | %d | %d | %d | %d | %s | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
		row.WorkloadFilePath,
		row.Rows,
		row.Events,
		row.MalformedEvents,
		row.SequenceGaps,
		row.Duration.Round(time.Millisecond),
		row.RowsPerSecond,
		row.EventsPerSecond,
		float64(row.P95Latency.Microseconds())/1000,
		float64(row.PeakAllocBytes)/(1024*1024),
		row.AllocsPerEvent,
	)
}

func printBenchMatrixTable(rows []bench.MatrixResult) {
	fmt.Println("| name | mode | format | workload | bytes | runs | median rows/sec | median events/sec | median p95 latency | median peak alloc MB | malformed | gaps |")
	fmt.Println("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, row := range rows {
		median := row.Median
		fmt.Printf("| %s | %s | %s | %s | %d | %d | %.2f | %.2f | %.2f | %.2f | %d | %d |\n",
			row.Name,
			row.Mode,
			row.Format,
			row.WorkloadFilePath,
			row.WorkloadBytes,
			len(row.Runs),
			median.RowsPerSecond,
			median.EventsPerSecond,
			float64(median.P95Latency.Microseconds())/1000,
			float64(median.PeakAllocBytes)/(1024*1024),
			median.MalformedEvents,
			median.SequenceGaps,
		)
	}
}

func printDBBenchTable(row servicebench.DBResult) {
	fmt.Println("| rows | lookups | insert rows | row insert rows/sec | COPY rows/sec | COPY speedup | no-index p95 | indexed p95 | lookup p95 improvement |")
	fmt.Println("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
	fmt.Printf("| %d | %d | %d | %.2f | %.2f | %.2fx | %.3f ms | %.3f ms | %.2fx |\n",
		row.Rows,
		row.Lookups,
		row.InsertRows,
		row.RowInsertRowsPerSecond,
		row.CopyInsertRowsPerSecond,
		row.CopyVsRowInsertSpeedup,
		float64(row.LookupNoIndexP95.Microseconds())/1000,
		float64(row.LookupIndexedP95.Microseconds())/1000,
		row.LookupP95ImprovementMultiple,
	)
}

func printQueueBenchTable(row servicebench.QueueResult) {
	fmt.Println("| queue | jobs | jobs/min | enqueue p50 | enqueue p95 | duplicate rejected |")
	fmt.Println("|---|---:|---:|---:|---:|---:|")
	fmt.Printf("| %s | %d | %.2f | %.3f ms | %.3f ms | %t |\n",
		row.QueueName,
		row.Jobs,
		row.JobsPerMinute,
		float64(row.EnqueueP50.Microseconds())/1000,
		float64(row.EnqueueP95.Microseconds())/1000,
		row.DuplicateRejected,
	)
}

func usage() {
	fmt.Fprintln(os.Stderr, `market-replay streams and validates historical market-data event files.

Usage:
  market-replay validate --file testdata/btcusdt_depth.jsonl
  market-replay replay --file testdata/btcusdt_depth.jsonl --symbol BTCUSDT --speed max
  market-replay bench --file testdata/btcusdt_depth.jsonl
  market-replay bench-matrix --runs 3 --bytes 65536
  market-replay db-bench --database-url postgres://market:market@localhost:5432/market_replay?sslmode=disable
  market-replay queue-bench --redis-addr localhost:6379 --jobs 1000
  market-replay generate-workload --source testdata/btcusdt_depth.jsonl --output tmp/btcusdt_10mb.jsonl --bytes 10485760

Commands:
  validate   stream a file and report malformed events, gaps, and ordering failures
  replay     stream a file with max, 10x, or 1x speed control
  bench      stream a file and print throughput/memory metrics
  bench-matrix run local fixture/generated and JSONL/CSV benchmark comparisons
  db-bench   run Postgres row insert, COPY insert, and lookup index benchmarks
  queue-bench run Redis/Asynq enqueue and duplicate rejection benchmarks
  generate-workload repeat a fixture into a deterministic benchmark workload`)
}
