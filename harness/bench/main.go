// Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Benchmarks Go Pebble on the workload Oxia runs, in the phases and the output
// format bench/ reports for roble, so the two can be read side by side. See
// bench/README.md.
//
// A run benchmarks one engine in a process of its own, so the CPU time and the
// size it reports are that engine's alone. The keys, the value size, the
// random lookup order and the phase order are the same as the Rust side's —
// the same xorshift seeded the same way — so the two walk the same database
// and probe it in the same sequence.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const usage = `usage: bench --dir DIR [options]

Creates the store at DIR/pebble, which must not exist yet.

  --shards N      stores to open, as Oxia opens one per shard (1)
  --batches N     batches written to each shard (200)
  --records N     records in each batch (100)
  --value-bytes N bytes in each value (128)
  --reads N       lookups in each get and floor phase (100000)
  --cache-mb N    block cache for every shard together, in MiB (64)
  --value-chunk N bytes of random data each value is built from, repeated to
                  fill it (default: the whole value, so incompressible)
  --memtable-mb N bytes a memtable holds before a flush, in MiB (4)
  --seed N        seed for the keys and the lookup order (1)
  --reuse         open the store already at DIR/<engine> instead of refusing
                  it, so a load and the reads over it can be two processes
  --layout        print every table of every level, with its size and key
                  range, after the store has settled
  --tsv           one tab-separated line per phase instead of a table, for a
                  script that puts several engines side by side
  --no-wal        turn the WAL off, as Oxia runs it
  --phases LIST   comma-separated subset of
                  load,settle,scan-rev,scan,get,floor,get-cold,checkpoint
                  (always run in that order, whatever order they are named in)`

type config struct {
	dir        string
	shards     int
	batches    int
	records    int
	valueBytes int
	valueChunk int
	reads      int
	cacheMB    int64
	memtableMB int64
	seed       uint64
	wal        bool
	layout     bool
	reuse      bool
	tsv        bool
	phases     map[string]bool
}

func main() {
	cfg := parse()
	path := filepath.Join(cfg.dir, "pebble")
	_, statErr := os.Stat(path)
	if statErr == nil && !cfg.reuse {
		fail(fmt.Sprintf("%s exists: remove it first, or pass --reuse to read the store already there", path))
	}
	if cfg.reuse && statErr != nil {
		fail(fmt.Sprintf("--reuse was given but %s does not exist: load it first", path))
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		fail(err.Error())
	}

	if !cfg.tsv {
		walState := "on"
		if !cfg.wal {
			walState = "off"
		}
		fmt.Printf("pebble: %d shards x %d batches x %d records, %d-byte values, %d MiB cache, "+
			"wal=%s, seed=%d\n",
			cfg.shards, cfg.batches, cfg.records, cfg.valueBytes, cfg.cacheMB, walState, cfg.seed)
	}
	// Said here rather than only in the README, so it travels with the
	// numbers. Go's os.File.Sync is fcntl(F_FULLFSYNC) on macOS, which
	// flushes the drive's own write cache; compio's is fsync, which does not.
	// On Linux — which is what Oxia runs — both are fsync.
	if cfg.wal && cfg.wants("load") && runtime.GOOS == "darwin" && !cfg.tsv {
		fmt.Println("note: on macOS Pebble's sync is F_FULLFSYNC and roble's is fsync, so the load")
		fmt.Println("note: row is not comparable between the two. Run both with --no-wal, which is")
		fmt.Println("note: Oxia's own configuration, for a load comparison that is.")
	}
	if !cfg.tsv {
		fmt.Println(header())
	}

	// One block cache for every shard, which is how Oxia runs: hundreds of
	// shards inside one budget. roble shares its caches the same way (§8).
	cache := pebble.NewCache(cfg.cacheMB * 1024 * 1024)

	shards := make([]*pebble.DB, cfg.shards)
	for i := range shards {
		db, err := pebble.Open(filepath.Join(path, fmt.Sprintf("shard-%04d", i)), options(cfg, cache))
		if err != nil {
			fail(fmt.Sprintf("open shard %d: %v", i, err))
		}
		shards[i] = db
	}
	// Closed at the end rather than deferred per store, because `get-cold`
	// replaces both the stores and the cache half way through and a deferred
	// close would then be of the wrong ones.
	defer func() {
		for _, db := range shards {
			_ = db.Close()
		}
	}()

	if cfg.wants("load") {
		load(cfg, shards)
	}
	if cfg.wants("settle") {
		settle(cfg, shards)
	}
	// Reverse first, for the reason the Rust side gives: the phases share a
	// block cache, so whichever runs second reads one the first warmed, and
	// the suspect should not be the one that benefits.
	// What shape the load and the settle left: a scan's cost is mostly how
	// many iterators it merges, so without this a reader cannot tell a slow
	// iterator from a differently-shaped LSM.
	if (cfg.wants("scan") || cfg.wants("scan-rev") || cfg.layout) && !cfg.tsv {
		fmt.Printf("levels %s\n", levelShape(shards))
	}
	if cfg.layout && !cfg.tsv {
		printLayout(shards)
	}
	if cfg.wants("scan-rev") {
		scan(cfg, shards, true)
	}
	if cfg.wants("scan") {
		scan(cfg, shards, false)
	}
	if cfg.wants("get-buf") {
		// Pebble's `Get` hands back a value borrowed from the block it is in,
		// with a closer, and `getWith` consumes it without copying. So its
		// `get` row below *is* its buffered read, and a second row would be
		// the same number twice. Which also means roble's `get` row is not
		// like-for-like with this one and its `get-buf` row is.
		skipped(cfg, "get-buf", "its get already borrows the value rather than copying it")
	}
	if cfg.wants("get") {
		lookups(cfg, shards, false)
	}
	if cfg.wants("floor") {
		lookups(cfg, shards, true)
	}
	// Last of the read phases, because it closes the stores and opens them
	// again: everything above it wants the caches the one before left.
	if cfg.wants("get-cold") {
		shards = reopen(cfg, shards, path, cache)
		coldGet(cfg, shards)
	}
	if cfg.wants("checkpoint") {
		checkpoint(cfg, shards, path)
	}

	if cfg.tsv {
		fmt.Printf("pebble\tpeak-rss-mib\t%d\n", peakRSSMiB())
	} else {
		fmt.Printf("peak rss %d MiB\n", peakRSSMiB())
	}
}

// options is Oxia's configuration of Pebble, from kv_pebble.go: 16 KiB blocks,
// L0 uncompressed and the levels below it Snappy, 10-bit Bloom filters, and
// the format major version roble pins.
func options(cfg config, cache *pebble.Cache) *pebble.Options {
	var levels [7]pebble.LevelOptions
	levels[0] = pebble.LevelOptions{
		BlockSize:    16 * 1024,
		Compression:  func() *sstable.CompressionProfile { return sstable.NoCompression },
		FilterPolicy: bloom.FilterPolicy(10),
	}
	for i := 1; i < len(levels); i++ {
		levels[i] = pebble.LevelOptions{
			BlockSize:    16 * 1024,
			Compression:  func() *sstable.CompressionProfile { return sstable.SnappyCompression },
			FilterPolicy: bloom.FilterPolicy(10),
		}
	}
	return &pebble.Options{
		FS:                 vfs.Default,
		Cache:              cache,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		Levels:             levels,
		DisableWAL:         !cfg.wal,
		// Pebble's own default is this already; set explicitly so that all
		// three engines flush at the same point and the load phase compares
		// the same amount of work.
		MemTableSize: uint64(cfg.memtableMB) * 1024 * 1024,
	}
}

// load writes every batch of every shard, timing each commit.
//
// Through an indexed batch, as Oxia does — its PebbleBatch reads back through
// itself before committing — so what is measured includes the index the repr
// is built under, as on the Rust side.
func load(cfg config, shards []*pebble.DB) {
	r := newReport(cfg, "load")
	write := pebble.NoSync
	if cfg.wal {
		write = pebble.Sync
	}
	for s, db := range shards {
		for b := 0; b < cfg.batches; b++ {
			batch := db.NewIndexedBatch()
			for j := 0; j < cfg.records; j++ {
				i := b*cfg.records + j
				if err := batch.Set(keyFor(s, i), valueFor(cfg, s, i), nil); err != nil {
					fail(err.Error())
				}
			}
			at := time.Now()
			if err := batch.Commit(write); err != nil {
				fail(err.Error())
			}
			r.saw(time.Since(at))
			r.count += int64(cfg.records)
			if err := batch.Close(); err != nil {
				fail(err.Error())
			}
		}
	}
	r.finish(shards)
}

// settle waits for the compactions the load left running.
//
// Pebble compacts on threads of its own, so unlike roble — which compacts on
// the thread that owns the store, and whose settle phase therefore does the
// work itself (§9) — there is nothing here to drive, only something to wait
// for. The CPU column is what makes the two comparable: Pebble's background
// threads are counted in it.
func settle(cfg config, shards []*pebble.DB) {
	r := newReport(cfg, "settle")
	for _, db := range shards {
		at := time.Now()
		if err := db.Flush(); err != nil {
			fail(err.Error())
		}
		// Pebble has no "wait until the picker is done", so this polls the
		// one number that says whether a compaction is running.
		for db.Metrics().Compact.NumInProgress > 0 {
			time.Sleep(time.Millisecond)
		}
		r.saw(time.Since(at))
		r.count++
	}
	_ = cfg
	r.finish(shards)
}

// scan steps every record of every shard, forward or backward.
//
// The latency recorded is one step, not the whole scan: the question is what
// prev costs against next, and a total hides a tail.
// stepSample is how many steps there are per timed step, in the scan phases.
const stepSample = 64

func scan(cfg config, shards []*pebble.DB, reverse bool) {
	name := "scan"
	if reverse {
		name = "scan-rev"
	}
	r := newReport(cfg, name)
	for _, db := range shards {
		it, err := db.NewIter(nil)
		if err != nil {
			fail(err.Error())
		}
		ok := it.First()
		if reverse {
			ok = it.Last()
		}
		for ok {
			sink(it.Key())
			sink(it.Value())
			r.count++
			// Sampled, not every step, for the reason the Rust side gives: a
			// step costs tens to hundreds of nanoseconds and a clock reading
			// is the same order, so timing all of them would charge the
			// faster engine the larger share of the measurement.
			sampled := r.count%stepSample == 0
			var at time.Time
			if sampled {
				at = time.Now()
			}
			if reverse {
				ok = it.Prev()
			} else {
				ok = it.Next()
			}
			if sampled {
				r.saw(time.Since(at))
			}
		}
		if err := it.Close(); err != nil {
			fail(err.Error())
		}
	}
	expected := int64(cfg.shards * cfg.batches * cfg.records)
	if r.count != expected {
		fail(fmt.Sprintf("the scan saw %d records, not the %d that were written", r.count, expected))
	}
	r.finish(shards)
}

// lookups looks up random keys, every one of which has an answer.
//
// get is Oxia's Get with ComparisonEqual, a plain db.Get. floor is its
// getFloor: an exact Get first, and a new iterator bounded above by the probe
// with Last() called on it when that misses — which is what the probes here
// are built to make it do, each one a byte past a stored key.
func lookups(cfg config, shards []*pebble.DB, floor bool) {
	name := "get"
	if floor {
		name = "floor"
	}
	r := newReport(cfg, name)
	total := cfg.batches * cfg.records
	rng := newRng(cfg.seed ^ 0x9e3779b9)
	for n := 0; n < cfg.reads; n++ {
		s := int(rng.next() % uint64(cfg.shards))
		i := int(rng.next() % uint64(total))
		key := keyFor(s, i)
		probe := key
		if floor {
			probe = append(append([]byte{}, key...), '~')
		}
		at := time.Now()
		answer, err := getWith(shards[s], probe, floor)
		r.saw(time.Since(at))
		if err != nil {
			fail(fmt.Sprintf("%s: %v", name, err))
		}
		if answer == nil {
			fail(fmt.Sprintf("%s found no answer for %q", name, probe))
		}
		if floor && !bytes.Equal(answer, key) {
			fail(fmt.Sprintf("%s answered %q, not the key below the probe", name, answer))
		}
		r.count++
	}
	r.finish(shards)
}

// getWith returns the key the lookup landed on, Oxia's two steps for a floor.
func getWith(db *pebble.DB, probe []byte, floor bool) ([]byte, error) {
	value, closer, err := db.Get(probe)
	switch {
	case err == nil:
		sink(value)
		return probe, closer.Close()
	case !errors.Is(err, pebble.ErrNotFound):
		return nil, err
	case !floor:
		return nil, nil
	}
	// The exact lookup missed, so seek below the probe, as Oxia's getLower
	// does: an iterator bounded above by the probe, positioned at its last
	// key.
	it, err := db.NewIter(&pebble.IterOptions{UpperBound: probe})
	if err != nil {
		return nil, err
	}
	if !it.Last() {
		return nil, it.Close()
	}
	landed := append([]byte{}, it.Key()...)
	sink(it.Value())
	return landed, it.Close()
}

// checkpoint writes a checkpoint of every shard, which is how Oxia moves one.
func checkpoint(cfg config, shards []*pebble.DB, path string) {
	r := newReport(cfg, "checkpoint")
	for s, db := range shards {
		at := time.Now()
		if err := db.Checkpoint(filepath.Join(path, fmt.Sprintf("checkpoint-%04d", s))); err != nil {
			fail(err.Error())
		}
		r.saw(time.Since(at))
		r.count++
	}
	_ = cfg
	r.finish(shards)
}

// ---------------------------------------------------------------- reporting

type report struct {
	name      string
	cfg       config
	count     int64
	started   time.Time
	cpuAt     time.Duration
	latencies []time.Duration
}

func header() string {
	return fmt.Sprintf("%-11s %10s %12s %9s %9s %9s %8s %9s",
		"phase", "count", "rate/s", "p50 us", "p99 us", "max us", "cpu s", "size MiB")
}

func newReport(cfg config, name string) *report {
	return &report{name: name, cfg: cfg, started: time.Now(), cpuAt: cpuTime()}
}

func (r *report) saw(took time.Duration) {
	r.latencies = append(r.latencies, took)
}

func (r *report) finish(shards []*pebble.DB) {
	wall := time.Since(r.started)
	cpu := cpuTime() - r.cpuAt
	var size int64
	for _, db := range shards {
		size += int64(db.Metrics().DiskSpaceUsage())
	}
	sort.Slice(r.latencies, func(i, j int) bool { return r.latencies[i] < r.latencies[j] })
	us := func(q float64) int64 {
		if len(r.latencies) == 0 {
			return 0
		}
		i := int(float64(len(r.latencies)-1)*q + 0.5)
		return r.latencies[i].Microseconds()
	}
	rate := float64(0)
	if wall > 0 {
		rate = float64(r.count) / wall.Seconds()
	}
	sizeMiB := float64(size) / (1024 * 1024)
	if r.cfg.tsv {
		fmt.Printf("pebble\t%s\t%d\t%.0f\t%d\t%d\t%d\t%.2f\t%.1f\n",
			r.name, r.count, rate, us(0.50), us(0.99), us(1.0), cpu.Seconds(), sizeMiB)
		return
	}
	fmt.Printf("%-11s %10d %12.0f %9d %9d %9d %8.2f %9.1f\n",
		r.name, r.count, rate, us(0.50), us(0.99), us(1.0),
		cpu.Seconds(), sizeMiB)
}

// skipped says a phase did not happen, and why, in place of its line, in the
// formats roble-bench's own `Report::skipped` prints — a dash rather than a
// zero, which would read as "it did this and it took no time".
func skipped(cfg config, name, why string) {
	if cfg.tsv {
		fmt.Printf("pebble\t%s\tskipped\t%s\n", name, why)
		return
	}
	fmt.Printf("%-11s %10s \u2014 %s\n", name, "-", why)
}

// ------------------------------------------------------------------- the key

// keyFor is the key of record i in shard s, byte for byte the Rust side's.
//
// Oxia's shape, with roble's one constraint on it: the keys are raw and
// bytewise ordered, because roble refuses the oxia-slash-spans comparer (§3),
// so a shard roble can serve is one using Oxia's natural encoder. The id is
// zero-padded so bytewise order is numeric order.
func keyFor(s, i int) []byte {
	return []byte(fmt.Sprintf("/shard-%04d/bucket-%02d/key-%010d", s, i%64, i))
}

// reopen closes every store and opens it again, so the next phase reads
// nothing the last one warmed.
//
// A new block cache with it: Pebble's is passed in at open, so reusing the one
// above would keep exactly what this is trying to drop.
func reopen(cfg config, shards []*pebble.DB, path string, old *pebble.Cache) []*pebble.DB {
	for _, db := range shards {
		if err := db.Close(); err != nil {
			fail(err.Error())
		}
	}
	old.Unref()
	fresh := pebble.NewCache(cfg.cacheMB * 1024 * 1024)
	out := make([]*pebble.DB, len(shards))
	for i := range out {
		db, err := pebble.Open(filepath.Join(path, fmt.Sprintf("shard-%04d", i)), options(cfg, fresh))
		if err != nil {
			fail(fmt.Sprintf("reopen shard %d: %v", i, err))
		}
		out[i] = db
	}
	return out
}

// coldGet does one point lookup per shard, on a store just reopened.
//
// What the get phase cannot say. That one runs --reads lookups in a row after
// the scans have walked every block, so it reports a warm steady state. A
// shard Oxia has just been handed is in none of those states, and the first
// read of it pays for the footer, the index and the filter of whatever table
// holds the key.
//
// Exactly one lookup per shard, which is the point — a second would be warm.
// It is the engine's caches this empties, not the operating system's.
func coldGet(cfg config, shards []*pebble.DB) {
	r := newReport(cfg, "get-cold")
	total := cfg.batches * cfg.records
	rng := newRng(cfg.seed ^ 0x51ed270b)
	for s, db := range shards {
		key := keyFor(s, int(rng.next()%uint64(total)))
		at := time.Now()
		value, closer, err := db.Get(key)
		r.saw(time.Since(at))
		if err != nil {
			fail(fmt.Sprintf("get-cold %q: %v", key, err))
		}
		sink(value)
		if err := closer.Close(); err != nil {
			fail(err.Error())
		}
		r.count++
	}
	r.finish(shards)
}

// printLayout prints every table of every level: which file, how large, and
// what range of keys it holds, in the format the Rust side prints so the two
// can be diffed.
//
// Sorted by level and then by key, not by file number, because what matters is
// whether a level's tables partition the key space or overlap it — the first
// is a scan that reads one table at a time, the second is a scan that merges
// them all.
func printLayout(shards []*pebble.DB) {
	type table struct {
		level             int
		name              string
		size              uint64
		smallest, largest []byte
	}
	var tables []table
	for _, db := range shards {
		levels, err := db.SSTables()
		if err != nil {
			fail(err.Error())
		}
		for level, at := range levels {
			for _, t := range at {
				tables = append(tables, table{
					level:    level,
					name:     fmt.Sprintf("%06d.sst", t.FileNum),
					size:     t.Size,
					smallest: append([]byte{}, t.Smallest.UserKey...),
					largest:  append([]byte{}, t.Largest.UserKey...),
				})
			}
		}
	}
	if len(tables) == 0 {
		fmt.Println("layout — this engine has no tables")
		return
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].level != tables[j].level {
			return tables[i].level < tables[j].level
		}
		return bytes.Compare(tables[i].smallest, tables[j].smallest) < 0
	})
	fmt.Printf("layout %d tables\n", len(tables))
	for _, t := range tables {
		fmt.Printf("  L%d %-16s %8.1f MiB  %s .. %s\n",
			t.level, t.name, float64(t.size)/(1024*1024), t.smallest, t.largest)
	}
	// Whether each level below L0 really partitions the key space, which is
	// the property that lets a scan read its tables one after another instead
	// of merging them. L0 is allowed to overlap by design: it holds whole
	// flushed memtables.
	for level := 1; level < 7; level++ {
		var at []table
		for _, t := range tables {
			if t.level == level {
				at = append(at, t)
			}
		}
		overlaps := 0
		for i := 1; i < len(at); i++ {
			if bytes.Compare(at[i].smallest, at[i-1].largest) <= 0 {
				overlaps++
			}
		}
		if overlaps > 0 {
			fmt.Printf("  L%d: %d overlapping pair(s), which a scan has to merge\n", level, overlaps)
		}
	}
}

// levelShape is how many tables sit at each level, summed over the shards.
func levelShape(shards []*pebble.DB) string {
	var counts [7]int
	for _, db := range shards {
		for level, l := range db.Metrics().Levels {
			if level < len(counts) {
				counts[level] += int(l.TablesCount)
			}
		}
	}
	var parts []string
	for level, n := range counts {
		if n > 0 {
			parts = append(parts, fmt.Sprintf("L%d=%d", level, n))
		}
	}
	return strings.Join(parts, " ")
}

// valueFor is the value of record i in shard s: pseudo-random bytes, byte for
// byte the Rust side's.
//
// Random rather than a repeated byte, and not a detail: a value of one byte
// over and over compresses to nothing, so the size column would be measuring
// the generator rather than what Snappy does to Oxia's data.
func valueFor(cfg config, s, i int) []byte {
	r := newRng(cfg.seed ^ (uint64(s) << 40) ^ uint64(i))
	// --value-chunk is the dial between incompressible and compressible: one
	// random chunk repeated to fill the value, so a chunk the size of the
	// value gives Snappy nothing and a smaller one gives it an exact repeat.
	chunk := cfg.valueChunk
	if chunk <= 0 || chunk > cfg.valueBytes {
		chunk = cfg.valueBytes
	}
	pattern := make([]byte, 0, chunk+8)
	for len(pattern) < chunk {
		var word [8]byte
		n := r.next()
		for b := 0; b < 8; b++ {
			word[b] = byte(n >> (8 * b))
		}
		pattern = append(pattern, word[:]...)
	}
	pattern = pattern[:chunk]
	value := make([]byte, 0, cfg.valueBytes)
	for len(value) < cfg.valueBytes {
		take := chunk
		if left := cfg.valueBytes - len(value); take > left {
			take = left
		}
		value = append(value, pattern[:take]...)
	}
	return value
}

// ------------------------------------------------------------------ the rest

// rng is xorshift64*, the same generator the Rust side uses, so both probe the
// same keys in the same order.
type rng struct{ state uint64 }

func newRng(seed uint64) *rng { return &rng{state: seed | 1} }

func (r *rng) next() uint64 {
	x := r.state
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	r.state = x
	return x * 0x2545f4914f6cdd1d
}

// sink keeps the compiler from dropping a read whose value is unused, as
// black_box does on the Rust side.
var sunk byte

func sink(b []byte) {
	if len(b) > 0 {
		sunk ^= b[0]
	}
}

// cpuTime is this process's CPU time, user and system, its threads included.
//
// The column that makes the two engines comparable at all: Pebble compacts on
// threads of its own and roble compacts on the thread that owns the store
// (§9), so wall-clock time alone would credit Pebble with work it still paid
// for.
func cpuTime() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	secs := func(t syscall.Timeval) time.Duration {
		return time.Duration(t.Sec)*time.Second + time.Duration(t.Usec)*time.Microsecond
	}
	return secs(usage.Utime) + secs(usage.Stime)
}

// peakRSSMiB is peak resident memory, in MiB.
func peakRSSMiB() int64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	// ru_maxrss is bytes on macOS and kilobytes everywhere else.
	bytes := int64(usage.Maxrss)
	if !isDarwin {
		bytes *= 1024
	}
	return bytes / (1024 * 1024)
}

func parse() config {
	cfg := config{
		shards:     1,
		batches:    200,
		records:    100,
		valueBytes: 128,
		reads:      100000,
		cacheMB:    64,
		memtableMB: 4,
		seed:       1,
		wal:        true,
	}
	args := os.Args[1:]
	sawDir := false
	for i := 0; i < len(args); {
		value := func() string {
			if i+1 >= len(args) {
				fail(fmt.Sprintf("%s needs a value", args[i]))
			}
			return args[i+1]
		}
		tookValue := true
		switch args[i] {
		case "--dir":
			cfg.dir = value()
			sawDir = true
		case "--shards":
			cfg.shards = int(number(value()))
		case "--batches":
			cfg.batches = int(number(value()))
		case "--records":
			cfg.records = int(number(value()))
		case "--value-bytes":
			cfg.valueBytes = int(number(value()))
		case "--value-chunk":
			cfg.valueChunk = int(number(value()))
		case "--reads":
			cfg.reads = int(number(value()))
		case "--cache-mb":
			cfg.cacheMB = int64(number(value()))
		case "--memtable-mb":
			cfg.memtableMB = int64(number(value()))
		case "--seed":
			cfg.seed = number(value())
		case "--phases":
			cfg.phases = map[string]bool{}
			for _, p := range bytes.Split([]byte(value()), []byte(",")) {
				cfg.phases[string(bytes.TrimSpace(p))] = true
			}
		case "--no-wal":
			cfg.wal = false
			tookValue = false
		case "--layout":
			cfg.layout = true
			tookValue = false
		case "--reuse":
			cfg.reuse = true
			tookValue = false
		case "--tsv":
			cfg.tsv = true
			tookValue = false
		case "-h", "--help":
			fmt.Println(usage)
			os.Exit(0)
		default:
			fail(fmt.Sprintf("unknown argument %s\n\n%s", args[i], usage))
		}
		if tookValue {
			i += 2
		} else {
			i++
		}
	}
	if !sawDir {
		fail(fmt.Sprintf("--dir is required\n\n%s", usage))
	}
	if cfg.shards < 1 || cfg.batches < 1 || cfg.records < 1 {
		fail("--shards, --batches and --records all have to be at least 1")
	}
	return cfg
}

func (c config) wants(phase string) bool {
	if c.phases == nil {
		return true
	}
	return c.phases[phase]
}

func number(s string) uint64 {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		fail(fmt.Sprintf("%q is not a number", s))
	}
	return n
}

func fail(message string) {
	fmt.Fprintf(os.Stderr, "bench: %s\n", message)
	os.Exit(1)
}
