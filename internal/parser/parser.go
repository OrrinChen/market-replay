package parser

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/orynwilder/market-replay-service/internal/event"
)

type Format string

const (
	FormatAuto  Format = "auto"
	FormatJSONL Format = "jsonl"
	FormatCSV   Format = "csv"
)

var ErrUnsupportedFormat = errors.New("unsupported market event format")

type Record struct {
	Line  int64
	Event event.MarketEvent
	Raw   []byte
	Err   error
}

type Stream struct {
	format Format
	scan   *bufio.Scanner
	csv    *csv.Reader
	header map[string]int
	line   int64
	close  io.Closer
}

func Open(path string, format Format) (*Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	resolved, err := ResolveFormat(path, format)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	stream, err := NewStream(file, resolved)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	stream.close = file
	return stream, nil
}

func NewStream(r io.Reader, format Format) (*Stream, error) {
	switch format {
	case FormatJSONL:
		scan := bufio.NewScanner(r)
		scan.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
		return &Stream{format: format, scan: scan}, nil
	case FormatCSV:
		reader := csv.NewReader(r)
		reader.FieldsPerRecord = -1
		reader.TrimLeadingSpace = true
		header, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("read csv header: %w", err)
		}
		return &Stream{format: format, csv: reader, header: indexHeader(header), line: 1}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedFormat, format)
	}
}

func (s *Stream) Close() error {
	if s.close == nil {
		return nil
	}
	return s.close.Close()
}

func (s *Stream) Next() (Record, error) {
	switch s.format {
	case FormatJSONL:
		return s.nextJSONL()
	case FormatCSV:
		return s.nextCSV()
	default:
		return Record{}, fmt.Errorf("%w: %s", ErrUnsupportedFormat, s.format)
	}
}

func ResolveFormat(path string, format Format) (Format, error) {
	if format != "" && format != FormatAuto {
		if format == FormatJSONL || format == FormatCSV {
			return format, nil
		}
		return "", fmt.Errorf("%w: %s", ErrUnsupportedFormat, format)
	}

	switch strings.ToLower(filepath.Ext(path)) {
	case ".jsonl", ".ndjson":
		return FormatJSONL, nil
	case ".csv":
		return FormatCSV, nil
	default:
		return "", fmt.Errorf("%w: cannot infer from extension %q", ErrUnsupportedFormat, filepath.Ext(path))
	}
}

func (s *Stream) nextJSONL() (Record, error) {
	if !s.scan.Scan() {
		if err := s.scan.Err(); err != nil {
			return Record{}, err
		}
		return Record{}, io.EOF
	}

	s.line++
	raw := append([]byte(nil), s.scan.Bytes()...)
	ev, err := ParseJSONLine(raw, s.line)
	if err != nil {
		return Record{Line: s.line, Raw: raw, Err: err}, nil
	}
	return Record{Line: s.line, Event: ev, Raw: raw}, nil
}

func (s *Stream) nextCSV() (Record, error) {
	fields, err := s.csv.Read()
	if err == io.EOF {
		return Record{}, io.EOF
	}
	s.line++
	if err != nil {
		return Record{Line: s.line, Err: err}, nil
	}

	ev, err := parseCSV(fields, s.header, s.line)
	if err != nil {
		return Record{Line: s.line, Err: err}, nil
	}
	return Record{Line: s.line, Event: ev}, nil
}

func indexHeader(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[strings.ToLower(strings.TrimSpace(name))] = i
	}
	return index
}

func csvValue(fields []string, header map[string]int, key string) string {
	i, ok := header[key]
	if !ok || i >= len(fields) {
		return ""
	}
	return strings.TrimSpace(fields[i])
}

func csvInt(fields []string, header map[string]int, key string) (int64, error) {
	value := csvValue(fields, header, key)
	if value == "" {
		return 0, fmt.Errorf("missing %s", key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	return parsed, nil
}
