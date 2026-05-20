package workload

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type GenerateOptions struct {
	SourcePath  string
	OutputPath  string
	TargetBytes int64
}

type GenerateResult struct {
	SourcePath   string `json:"source_path"`
	OutputPath   string `json:"output_path"`
	TargetBytes  int64  `json:"target_bytes"`
	BytesWritten int64  `json:"bytes_written"`
	RowsWritten  int64  `json:"rows_written"`
}

func GenerateFile(opts GenerateOptions) (GenerateResult, error) {
	source, err := os.Open(opts.SourcePath)
	if err != nil {
		return GenerateResult{}, err
	}
	defer source.Close()

	switch strings.ToLower(filepath.Ext(opts.SourcePath)) {
	case ".csv":
		return generateCSV(opts, source)
	default:
		return generateJSONL(opts, source)
	}
}

func generateJSONL(opts GenerateOptions, source io.Reader) (GenerateResult, error) {
	lines, err := readLines(source)
	if err != nil {
		return GenerateResult{}, err
	}
	if len(lines) == 0 {
		return GenerateResult{}, fmt.Errorf("source workload has no rows")
	}

	output, err := os.Create(opts.OutputPath)
	if err != nil {
		return GenerateResult{}, err
	}
	defer output.Close()

	writer := bufio.NewWriter(output)
	defer writer.Flush()
	normalizer := newJSONLNormalizer(lines)

	result := GenerateResult{
		SourcePath:  opts.SourcePath,
		OutputPath:  opts.OutputPath,
		TargetBytes: opts.TargetBytes,
	}
	for result.BytesWritten < opts.TargetBytes {
		for _, line := range lines {
			line = normalizer.normalize(line, result.RowsWritten)
			n, err := writer.WriteString(line)
			if err != nil {
				return GenerateResult{}, err
			}
			result.BytesWritten += int64(n)
			result.RowsWritten++
			if result.BytesWritten >= opts.TargetBytes {
				break
			}
		}
	}
	return result, nil
}

type jsonlNormalizer struct {
	enabled       bool
	baseFirstID   int64
	lastFinalID   int64
	baseTime      int64
	lastEventTime int64
	rowCount      int64
}

func newJSONLNormalizer(lines []string) jsonlNormalizer {
	var normalizer jsonlNormalizer
	for _, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			return jsonlNormalizer{}
		}
		if obj["event_type"] != "depth" {
			return jsonlNormalizer{}
		}
		first, okFirst := jsonInt(obj["first_update_id"])
		final, okFinal := jsonInt(obj["final_update_id"])
		eventTime, okTime := jsonInt(obj["event_time"])
		if !okFirst || !okFinal || !okTime {
			return jsonlNormalizer{}
		}
		if !normalizer.enabled {
			normalizer.enabled = true
			normalizer.baseFirstID = first
			normalizer.baseTime = eventTime
		}
		normalizer.lastFinalID = final
		normalizer.lastEventTime = eventTime
		normalizer.rowCount++
	}
	return normalizer
}

func (n jsonlNormalizer) normalize(line string, row int64) string {
	if !n.enabled {
		return line
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return line
	}
	first, okFirst := jsonInt(obj["first_update_id"])
	final, okFinal := jsonInt(obj["final_update_id"])
	eventTime, okTime := jsonInt(obj["event_time"])
	if !okFirst || !okFinal || !okTime {
		return line
	}
	sequenceSpan := n.lastFinalID - n.baseFirstID + 1
	timeSpan := n.lastEventTime - n.baseTime + 1
	if sequenceSpan <= 0 || timeSpan <= 0 {
		return line
	}
	cycle := row / n.rowCount
	idOffset := cycle * sequenceSpan
	timeOffset := cycle * timeSpan
	obj["first_update_id"] = first + idOffset
	obj["final_update_id"] = final + idOffset
	obj["event_time"] = eventTime + timeOffset
	encoded, err := json.Marshal(obj)
	if err != nil {
		return line
	}
	return string(encoded) + "\n"
}

func jsonInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), typed == float64(int64(typed))
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

func generateCSV(opts GenerateOptions, source io.Reader) (GenerateResult, error) {
	reader := csv.NewReader(source)
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return GenerateResult{}, err
	}
	if len(records) < 2 {
		return GenerateResult{}, fmt.Errorf("source workload must include csv header and at least one row")
	}

	output, err := os.Create(opts.OutputPath)
	if err != nil {
		return GenerateResult{}, err
	}
	defer output.Close()
	writer := csv.NewWriter(output)
	defer writer.Flush()

	result := GenerateResult{SourcePath: opts.SourcePath, OutputPath: opts.OutputPath, TargetBytes: opts.TargetBytes}
	header := records[0]
	if err := writer.Write(header); err != nil {
		return GenerateResult{}, err
	}
	headerBytes := csvRecordBytes(header)
	result.BytesWritten += headerBytes

	rows := records[1:]
	headerIndex := make(map[string]int, len(header))
	for i, name := range header {
		headerIndex[strings.ToLower(strings.TrimSpace(name))] = i
	}
	normalizer := newCSVNormalizer(headerIndex, rows)
	for result.BytesWritten < opts.TargetBytes {
		for _, row := range rows {
			next := append([]string(nil), row...)
			normalizer.normalize(next, result.RowsWritten)
			if err := writer.Write(next); err != nil {
				return GenerateResult{}, err
			}
			result.BytesWritten += csvRecordBytes(next)
			result.RowsWritten++
			if result.BytesWritten >= opts.TargetBytes {
				break
			}
		}
	}
	if err := writer.Error(); err != nil {
		return GenerateResult{}, err
	}
	return result, nil
}

type csvNormalizer struct {
	enabled        bool
	tradeIDIndex   int
	tradeTimeIndex int
	baseTradeID    int64
	lastTradeID    int64
	baseTradeTime  int64
	lastTradeTime  int64
	rowCount       int64
}

func newCSVNormalizer(header map[string]int, rows [][]string) csvNormalizer {
	idIndex, okID := header["trade_id"]
	timeIndex, okTime := header["trade_time"]
	if !okID || !okTime || len(rows) == 0 {
		return csvNormalizer{}
	}
	var normalizer csvNormalizer
	for i, row := range rows {
		if idIndex >= len(row) || timeIndex >= len(row) {
			return csvNormalizer{}
		}
		id, err := strconv.ParseInt(row[idIndex], 10, 64)
		if err != nil {
			return csvNormalizer{}
		}
		tradeTime, err := strconv.ParseInt(row[timeIndex], 10, 64)
		if err != nil {
			return csvNormalizer{}
		}
		if i == 0 {
			normalizer.enabled = true
			normalizer.tradeIDIndex = idIndex
			normalizer.tradeTimeIndex = timeIndex
			normalizer.baseTradeID = id
			normalizer.baseTradeTime = tradeTime
		}
		normalizer.lastTradeID = id
		normalizer.lastTradeTime = tradeTime
		normalizer.rowCount++
	}
	return normalizer
}

func (n csvNormalizer) normalize(row []string, emittedRows int64) {
	if !n.enabled || n.rowCount <= 0 {
		return
	}
	id, errID := strconv.ParseInt(row[n.tradeIDIndex], 10, 64)
	tradeTime, errTime := strconv.ParseInt(row[n.tradeTimeIndex], 10, 64)
	if errID != nil || errTime != nil {
		return
	}
	idSpan := n.lastTradeID - n.baseTradeID + 1
	timeSpan := n.lastTradeTime - n.baseTradeTime + 1
	cycle := emittedRows / n.rowCount
	row[n.tradeIDIndex] = strconv.FormatInt(id+cycle*idSpan, 10)
	row[n.tradeTimeIndex] = strconv.FormatInt(tradeTime+cycle*timeSpan, 10)
}

func csvRecordBytes(record []string) int64 {
	var b strings.Builder
	writer := csv.NewWriter(&b)
	_ = writer.Write(record)
	writer.Flush()
	return int64(b.Len())
}

func readLines(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text()+"\n")
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}
