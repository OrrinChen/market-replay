package servicebench

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	DefaultDBRows       = 50000
	DefaultDBLookups    = 1000
	DefaultDBInsertRows = 10000
)

type DBOptions struct {
	DatabaseURL string
	Rows        int
	Lookups     int
	InsertRows  int
}

type DBResult struct {
	Rows                         int           `json:"rows"`
	Lookups                      int           `json:"lookups"`
	InsertRows                   int           `json:"insert_rows"`
	RowInsertDuration            time.Duration `json:"row_insert_duration"`
	RowInsertRowsPerSecond       float64       `json:"row_insert_rows_per_second"`
	CopyInsertDuration           time.Duration `json:"copy_insert_duration"`
	CopyInsertRowsPerSecond      float64       `json:"copy_insert_rows_per_second"`
	CopyVsRowInsertSpeedup       float64       `json:"copy_vs_row_insert_speedup"`
	LookupNoIndexP50             time.Duration `json:"lookup_no_index_p50"`
	LookupNoIndexP95             time.Duration `json:"lookup_no_index_p95"`
	LookupIndexedP50             time.Duration `json:"lookup_indexed_p50"`
	LookupIndexedP95             time.Duration `json:"lookup_indexed_p95"`
	LookupP95ImprovementMultiple float64       `json:"lookup_p95_improvement_multiple"`
}

func RunDB(ctx context.Context, opts DBOptions) (DBResult, error) {
	opts = normalizeDBOptions(opts)
	if opts.DatabaseURL == "" {
		return DBResult{}, fmt.Errorf("database URL is required")
	}

	pool, err := pgxpool.New(ctx, opts.DatabaseURL)
	if err != nil {
		return DBResult{}, err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return DBResult{}, err
	}

	result := DBResult{
		Rows:       opts.Rows,
		Lookups:    opts.Lookups,
		InsertRows: opts.InsertRows,
	}

	rowInsert, err := measureRowInsert(ctx, pool, opts.InsertRows)
	if err != nil {
		return DBResult{}, err
	}
	result.RowInsertDuration = rowInsert
	result.RowInsertRowsPerSecond = perSecond(opts.InsertRows, rowInsert)

	copyInsert, err := measureCopyInsert(ctx, pool, opts.InsertRows)
	if err != nil {
		return DBResult{}, err
	}
	result.CopyInsertDuration = copyInsert
	result.CopyInsertRowsPerSecond = perSecond(opts.InsertRows, copyInsert)
	result.CopyVsRowInsertSpeedup = ratio(result.CopyInsertRowsPerSecond, result.RowInsertRowsPerSecond)

	noIndex, indexed, err := measureLookupIndex(ctx, pool, opts.Rows, opts.Lookups)
	if err != nil {
		return DBResult{}, err
	}
	result.LookupNoIndexP50 = percentileDuration(noIndex, 0.50)
	result.LookupNoIndexP95 = percentileDuration(noIndex, 0.95)
	result.LookupIndexedP50 = percentileDuration(indexed, 0.50)
	result.LookupIndexedP95 = percentileDuration(indexed, 0.95)
	result.LookupP95ImprovementMultiple = ratio(float64(result.LookupNoIndexP95), float64(result.LookupIndexedP95))
	return result, nil
}

func normalizeDBOptions(opts DBOptions) DBOptions {
	if opts.Rows <= 0 {
		opts.Rows = DefaultDBRows
	}
	if opts.Lookups <= 0 {
		opts.Lookups = DefaultDBLookups
	}
	if opts.InsertRows <= 0 {
		opts.InsertRows = DefaultDBInsertRows
	}
	return opts
}

func measureRowInsert(ctx context.Context, pool *pgxpool.Pool, rows int) (time.Duration, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `CREATE TEMP TABLE bench_row_insert (id TEXT PRIMARY KEY, payload TEXT NOT NULL) ON COMMIT PRESERVE ROWS`); err != nil {
		return 0, err
	}
	started := time.Now()
	for i := 0; i < rows; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO bench_row_insert (id, payload) VALUES ($1, $2)`, benchID(i), benchPayload(i)); err != nil {
			return 0, err
		}
	}
	return time.Since(started), nil
}

func measureCopyInsert(ctx context.Context, pool *pgxpool.Pool, rows int) (time.Duration, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `CREATE TEMP TABLE bench_copy_insert (id TEXT PRIMARY KEY, payload TEXT NOT NULL) ON COMMIT PRESERVE ROWS`); err != nil {
		return 0, err
	}
	source := pgx.CopyFromSlice(rows, func(i int) ([]any, error) {
		return []any{benchID(i), benchPayload(i)}, nil
	})
	started := time.Now()
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"bench_copy_insert"}, []string{"id", "payload"}, source); err != nil {
		return 0, err
	}
	return time.Since(started), nil
}

func measureLookupIndex(ctx context.Context, pool *pgxpool.Pool, rows int, lookups int) ([]time.Duration, []time.Duration, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `CREATE TEMP TABLE bench_lookup_jobs (
		id TEXT PRIMARY KEY,
		idempotency_key TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now()
	) ON COMMIT PRESERVE ROWS`); err != nil {
		return nil, nil, err
	}
	source := pgx.CopyFromSlice(rows, func(i int) ([]any, error) {
		return []any{benchID(i), benchKey(i), "queued"}, nil
	})
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{"bench_lookup_jobs"}, []string{"id", "idempotency_key", "status"}, source); err != nil {
		return nil, nil, err
	}
	if _, err := conn.Exec(ctx, `ANALYZE bench_lookup_jobs`); err != nil {
		return nil, nil, err
	}

	keys := lookupKeys(rows, lookups)
	noIndex, err := measureLookups(ctx, conn, keys)
	if err != nil {
		return nil, nil, err
	}
	if _, err := conn.Exec(ctx, `CREATE INDEX bench_lookup_jobs_idempotency_key_idx ON bench_lookup_jobs (idempotency_key)`); err != nil {
		return nil, nil, err
	}
	if _, err := conn.Exec(ctx, `ANALYZE bench_lookup_jobs`); err != nil {
		return nil, nil, err
	}
	indexed, err := measureLookups(ctx, conn, keys)
	if err != nil {
		return nil, nil, err
	}
	return noIndex, indexed, nil
}

func measureLookups(ctx context.Context, conn *pgxpool.Conn, keys []string) ([]time.Duration, error) {
	latencies := make([]time.Duration, 0, len(keys))
	for _, key := range keys {
		started := time.Now()
		var id string
		if err := conn.QueryRow(ctx, `SELECT id FROM bench_lookup_jobs WHERE idempotency_key = $1`, key).Scan(&id); err != nil {
			return nil, err
		}
		latencies = append(latencies, time.Since(started))
	}
	return latencies, nil
}

func lookupKeys(rows int, lookups int) []string {
	keys := make([]string, 0, lookups)
	for i := 0; i < lookups; i++ {
		index := (i*7919 + 17) % rows
		keys = append(keys, benchKey(index))
	}
	return keys
}

func benchID(i int) string {
	return fmt.Sprintf("bench-job-%08d", i)
}

func benchKey(i int) string {
	return fmt.Sprintf("bench-idempotency-%08d", i)
}

func benchPayload(i int) string {
	return fmt.Sprintf("payload-%08d", i)
}

func percentileDuration(values []time.Duration, pct float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	index := int(float64(len(cp)-1) * pct)
	return cp[index]
}

func perSecond(count int, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(count) / duration.Seconds()
}

func ratio(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}
