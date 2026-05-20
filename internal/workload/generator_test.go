package workload

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/orynwilder/market-replay-service/internal/parser"
	"github.com/orynwilder/market-replay-service/internal/validate"
)

func TestGenerateFileRepeatsSourceRowsUntilTargetBytes(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.jsonl")
	output := filepath.Join(dir, "out.jsonl")
	if err := os.WriteFile(source, []byte("a\nb\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	result, err := GenerateFile(GenerateOptions{
		SourcePath:  source,
		OutputPath:  output,
		TargetBytes: 7,
	})
	if err != nil {
		t.Fatalf("GenerateFile returned error: %v", err)
	}
	if result.BytesWritten < 7 {
		t.Fatalf("bytes written = %d, want at least 7", result.BytesWritten)
	}
	if result.RowsWritten != 4 {
		t.Fatalf("rows written = %d, want 4", result.RowsWritten)
	}
	content, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(content) != "a\nb\na\nb\n" {
		t.Fatalf("output = %q, want repeated source", string(content))
	}
}

func TestGenerateFileRejectsEmptySource(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "empty.jsonl")
	output := filepath.Join(dir, "out.jsonl")
	if err := os.WriteFile(source, nil, 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	if _, err := GenerateFile(GenerateOptions{SourcePath: source, OutputPath: output, TargetBytes: 1}); err == nil {
		t.Fatal("GenerateFile succeeded for empty source")
	}
}

func TestGenerateFilePreservesContiguousDepthSequences(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "depth.jsonl")

	_, err := GenerateFile(GenerateOptions{
		SourcePath:  filepath.Join("..", "..", "testdata", "btcusdt_depth.jsonl"),
		OutputPath:  output,
		TargetBytes: 4096,
	})
	if err != nil {
		t.Fatalf("GenerateFile returned error: %v", err)
	}
	result, err := validate.File(output, parser.FormatJSONL, validate.Options{})
	if err != nil {
		t.Fatalf("validate generated file: %v", err)
	}
	if result.SequenceGaps != 0 || result.MalformedEvents != 0 {
		t.Fatalf("generated valid depth fixture has failures: %#v", result)
	}
}

func TestGenerateFilePreservesCSVHeaderOnce(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "trades.csv")

	_, err := GenerateFile(GenerateOptions{
		SourcePath:  filepath.Join("..", "..", "testdata", "solusdt_aggtrade.csv"),
		OutputPath:  output,
		TargetBytes: 4096,
	})
	if err != nil {
		t.Fatalf("GenerateFile returned error: %v", err)
	}
	result, err := validate.File(output, parser.FormatCSV, validate.Options{})
	if err != nil {
		t.Fatalf("validate generated file: %v", err)
	}
	if result.MalformedEvents != 0 || result.SequenceGaps != 0 {
		t.Fatalf("generated csv fixture has failures: %#v", result)
	}
}
