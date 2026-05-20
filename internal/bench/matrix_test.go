package bench

import (
	"path/filepath"
	"testing"
)

func TestRunMatrixComparesFixtureGeneratedAndFormats(t *testing.T) {
	results, err := RunMatrix(MatrixOptions{
		RootDir:       filepath.Join("..", ".."),
		OutputDir:     filepath.Join(t.TempDir(), "bench-matrix"),
		Runs:          3,
		WorkloadBytes: 2048,
	})
	if err != nil {
		t.Fatalf("RunMatrix returned error: %v", err)
	}

	if len(results) != 4 {
		t.Fatalf("result count = %d, want 4", len(results))
	}

	seenMode := map[string]bool{}
	seenFormat := map[string]bool{}
	for _, result := range results {
		seenMode[result.Mode] = true
		seenFormat[result.Format] = true
		if len(result.Runs) != 3 {
			t.Fatalf("%s run count = %d, want 3", result.Name, len(result.Runs))
		}
		if result.Median.Rows == 0 || result.Median.Events == 0 {
			t.Fatalf("%s median did not include replay metrics: %#v", result.Name, result.Median)
		}
		if result.WorkloadFilePath == "" {
			t.Fatalf("%s workload path was empty", result.Name)
		}
		if result.WorkloadBytes <= 0 {
			t.Fatalf("%s workload bytes = %d, want positive", result.Name, result.WorkloadBytes)
		}
		if result.Mode == MatrixModeGenerated && result.SourcePath == result.WorkloadFilePath {
			t.Fatalf("%s generated workload reused source path", result.Name)
		}
	}

	if !seenMode[MatrixModeFixture] || !seenMode[MatrixModeGenerated] {
		t.Fatalf("modes seen = %#v, want fixture and generated", seenMode)
	}
	if !seenFormat[string(MatrixFormatJSONL)] || !seenFormat[string(MatrixFormatCSV)] {
		t.Fatalf("formats seen = %#v, want jsonl and csv", seenFormat)
	}
}
