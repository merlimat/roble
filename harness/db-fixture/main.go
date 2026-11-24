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

// Writes whole Pebble databases and dumps the version Pebble reads back out of
// them. The dump is the ground truth roble's open is judged against
// (DESIGN.md §12).
//
// Unlike golden-sst, which pins one file, these fixtures pin the thing that
// only exists once the files are put together: a format-version marker, a
// manifest marker naming one of several MANIFESTs, and a MANIFEST whose edits
// have to be replayed in order to say which table sits at which level.
//
// Automatic compactions are off and every flush and compaction is explicit, so
// the layout is the same on every run and the fixture can be committed.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// pin is the format major version roble reads: FormatVirtualSSTables, from
// DESIGN.md §3. Asserted rather than assumed — the constants around it are an
// iota chain with unexported entries, so the number is not something to read
// off the source, and a Pebble upgrade that renumbers it should fail here
// rather than somewhere subtler.
const pin = 16

// minSupported is the oldest format major version Pebble itself will open. A
// database below it is refused by Pebble, so roble has nothing to be
// compatible with there.
const minSupported = 13

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: db-fixture <output-dir>")
		os.Exit(2)
	}
	out := os.Args[1]

	if got := int(pebble.FormatVirtualSSTables); got != pin {
		fail(fmt.Errorf("FormatVirtualSSTables is %d, not the %d DESIGN.md §3 pins", got, pin))
	}
	if got := int(pebble.FormatMinSupported); got != minSupported {
		fail(fmt.Errorf("FormatMinSupported is %d, not %d", got, minSupported))
	}
	if pebble.FormatVirtualSSTables.MaxTableFormat() != sstable.TableFormatPebblev4 {
		fail(fmt.Errorf("the pin's max table format is no longer Pebblev4"))
	}

	for _, f := range []struct {
		name  string
		build func(string) error
	}{
		{"db-simple", buildSimple},
		{"db-unflushed-wal", buildUnflushedWAL},
		{"db-created", buildCreated},
		// A pair built identically, so their tables get the same file
		// numbers, holding different keys. Two stores on one thread look
		// like this, and a cache keyed by file number alone would serve one
		// the other's table.
		{"db-twin-a", buildTwin("a")},
		{"db-twin-b", buildTwin("b")},
		{"db-obsolete", buildObsolete},
	} {
		dir := filepath.Join(out, f.name)
		if err := os.RemoveAll(dir); err != nil {
			fail(err)
		}
		if err := f.build(dir); err != nil {
			fail(fmt.Errorf("%s: %w", f.name, err))
		}
		dump, err := describe(dir)
		if err != nil {
			fail(fmt.Errorf("%s: %w", f.name, err))
		}
		if err := os.WriteFile(filepath.Join(out, f.name+".dump"), []byte(dump), 0o644); err != nil {
			fail(err)
		}
		fmt.Printf("%s: %d files\n", f.name, countFiles(dir))
	}

	// The twins are only useful if their tables really do collide.
	a := tableNumbers(filepath.Join(out, "db-twin-a"))
	b := tableNumbers(filepath.Join(out, "db-twin-b"))
	if len(a) == 0 || !slices.Equal(a, b) {
		fail(fmt.Errorf("the twins' tables are %v and %v; they have to be the same file "+
			"numbers or the shared-cache test proves nothing", a, b))
	}
	fmt.Printf("db-twin-a and db-twin-b both hold %v\n", a)
}

// uncompressed turns compression off for every level.
//
// Not for realism — Pebble defaults to Snappy — but so that the table sizes in
// the dump are the same on every architecture. Go's Snappy has per-platform
// assembly, so a compressed table's length is not something a fixture
// generated on arm64 can promise to a CI runner on amd64, and the dump carries
// sizes. It also happens to match roble's own default (DESIGN.md §8).
func uncompressed(o *pebble.Options) *pebble.Options {
	o.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		return pebble.DBCompressionNone
	})
	return o
}

// buildSimple is Oxia's configuration: no WAL, so a flush is the only
// durability point and an opener has nothing to replay.
//
// Three flushes leave three tables in L0, then one explicit compaction moves a
// key range down, so the version has more than one level and replaying the
// MANIFEST in the wrong order would show it.
// buildCreated is a database Pebble made from an empty directory and wrote
// exactly one key to.
//
// It is here for one number: the sequence number that key gets. Pebble keeps
// 0 for a key a compaction has proven to be the last of its kind and 1 to 9
// for uses it has not found yet, so the first key assigned in a fresh
// database is numbered base.SeqNumStart, which is 10. roble has to start in
// the same place, and the constant is not importable — base is one of
// Pebble's internal packages — so the fixture's dump is what pins it.
//
// The write goes through a flush so that the number reaches a table, which is
// where the dump can read it off: an unflushed write lives in a memtable the
// dump cannot see into.
func buildCreated(dir string) error {
	db, err := pebble.Open(dir, uncompressed(&pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		DisableWAL:                  true,
	}))
	if err != nil {
		return err
	}

	// One key, in a batch of its own, so the smallest and largest sequence
	// numbers of the table it lands in are both the first number assigned.
	b := db.NewBatch()
	if err := b.Set([]byte("first"), []byte("v"), nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}
	return db.Close()
}

func buildSimple(dir string) error {
	db, err := pebble.Open(dir, uncompressed(&pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		DisableWAL:                  true,
	}))
	if err != nil {
		return err
	}

	// Batch one, flushed: the keys a compaction will later move down.
	b := db.NewBatch()
	for _, k := range []string{"alpha", "bravo", "charlie"} {
		if err := b.Set([]byte(k), []byte("v1-"+k), nil); err != nil {
			return err
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}

	// Batch two, flushed: an overwrite and a point delete, so the lower level
	// is not simply the union of what came before.
	b = db.NewBatch()
	if err := b.Set([]byte("bravo"), []byte("v2-bravo"), nil); err != nil {
		return err
	}
	if err := b.Delete([]byte("charlie"), nil); err != nil {
		return err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}

	// Compact what exists so far down out of L0.
	if err := db.Compact(context.Background(), []byte("a"), []byte("d"), true); err != nil {
		return err
	}

	// Batch three, flushed and left in L0: a range deletion, which Oxia uses
	// and which must survive the replay as an ordinary table.
	b = db.NewBatch()
	if err := b.DeleteRange([]byte("m"), []byte("p"), nil); err != nil {
		return err
	}
	for _, k := range []string{"delta", "echo"} {
		if err := b.Set([]byte(k), []byte("v3-"+k), nil); err != nil {
			return err
		}
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}

	return db.Close()
}

// buildUnflushedWAL leaves writes in the log that no table holds, which is the
// shape roble must refuse until log replay exists: opening without the log
// would silently drop writes that were already acknowledged (DESIGN.md §6).
func buildUnflushedWAL(dir string) error {
	db, err := pebble.Open(dir, uncompressed(&pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
	}))
	if err != nil {
		return err
	}
	// Flushed, so a table holds it.
	if err := db.Set([]byte("flushed"), []byte("in a table"), pebble.NoSync); err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return err
	}
	// Synced to the log and never flushed, so only the log holds it.
	if err := db.Set([]byte("logged"), []byte("only in the log"), pebble.Sync); err != nil {
		return err
	}
	return db.Close()
}

// buildTwin writes a one-table database whose keys are prefixed with `tag`.
//
// Both twins are built by exactly the same sequence, so Pebble hands their
// tables the same file numbers — which is the whole point: the test that two
// stores sharing a cache cannot read each other's tables needs the file
// numbers to collide, or it proves nothing. `main` asserts the collision
// rather than hoping for it.
func buildTwin(tag string) func(string) error {
	return func(dir string) error {
		db, err := pebble.Open(dir, uncompressed(&pebble.Options{
			FS:                          vfs.Default,
			FormatMajorVersion:          pebble.FormatVirtualSSTables,
			DisableAutomaticCompactions: true,
			DisableWAL:                  true,
		}))
		if err != nil {
			return err
		}
		b := db.NewBatch()
		for i := 0; i < 4; i++ {
			k := fmt.Sprintf("%s%d", tag, i)
			if err := b.Set([]byte(k), []byte("twin-"+k), nil); err != nil {
				return err
			}
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return err
		}
		if err := db.Flush(); err != nil {
			return err
		}
		return db.Close()
	}
}

// buildObsolete leaves two tables holding keys Pebble marked with the
// Pebblev4 obsolete bit, for each of the three reasons Pebble sets it
// (sstable/rowblk_writer.go):
//
//   - an older version of a key, below a newer one in the same table;
//   - a point deletion of any of the three kinds, in a table written to L6;
//   - a key a range deletion in the same table covers, which the flush or
//     compaction writing it forces (compact.Iter.ForceObsoleteDueToRangeDel).
//
// Only a snapshot makes Pebble keep keys like these, so each table is written
// while one holds the older writes, and the snapshots are released just
// before the database closes. Every marked key is then one no reader can
// see, and a compaction that kept one would have to write the bit, which
// roble never does (DESIGN.md §3).
//
// The L6 table is a compaction of two flushes rather than one, so it is a
// merge and not a move that leaves a flushed table there as it was: only a
// table written into L6 gets the second reason. The L0 table is one flush,
// with the first and third. Each table also holds two keys no mark touches:
// one written once, and one at the end of the range deletion's span, written
// before it. The first of those sorts first, because Pebble never marks a
// deletion that is a table's first point key: its writer decides nothing
// about a key until the table holds one.
func buildObsolete(dir string) error {
	db, err := pebble.Open(dir, uncompressed(&pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		DisableWAL:                  true,
	}))
	if err != nil {
		return err
	}

	var snaps []*pebble.Snapshot
	for _, level := range []string{"l6-", "l0-"} {
		// Every key once, so each SINGLEDEL below meets exactly the one SET
		// it is for.
		b := db.NewBatch()
		for _, k := range []string{"alive", "deleted", "overwritten", "range-1", "range-2",
			"range-3", "single-deleted", "size-deleted"} {
			if err := b.Set([]byte(level+k), []byte("old-"+level+k), nil); err != nil {
				return err
			}
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return err
		}
		if level == "l6-" {
			// The first of the two flushes the compaction merges.
			if err := db.Flush(); err != nil {
				return err
			}
		}

		snaps = append(snaps, db.NewSnapshot())

		b = db.NewBatch()
		if err := b.Set([]byte(level+"overwritten"), []byte("new-"+level+"overwritten"), nil); err != nil {
			return err
		}
		if err := b.Delete([]byte(level+"deleted"), nil); err != nil {
			return err
		}
		if err := b.SingleDelete([]byte(level+"single-deleted"), nil); err != nil {
			return err
		}
		size := uint32(len("old-" + level + "size-deleted"))
		if err := b.DeleteSized([]byte(level+"size-deleted"), size, nil); err != nil {
			return err
		}
		if err := b.DeleteRange([]byte(level+"range-1"), []byte(level+"range-3"), nil); err != nil {
			return err
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return err
		}
		if err := db.Flush(); err != nil {
			return err
		}
		if level == "l6-" {
			if err := db.Compact(context.Background(), []byte("l6-"), []byte("l6-\xff"), false); err != nil {
				return err
			}
		}
	}

	for _, s := range snaps {
		if err := s.Close(); err != nil {
			return err
		}
	}
	if err := db.Close(); err != nil {
		return err
	}

	// What a reader sees, by Pebble's own reader: none of the marked keys.
	return expectContents(dir, map[string]string{
		"l0-alive":       "old-l0-alive",
		"l0-overwritten": "new-l0-overwritten",
		"l0-range-3":     "old-l0-range-3",
		"l6-alive":       "old-l6-alive",
		"l6-overwritten": "new-l6-overwritten",
		"l6-range-3":     "old-l6-range-3",
	})
}

// expectContents fails unless Pebble, reopening dir, reads exactly want.
func expectContents(dir string, want map[string]string) error {
	db, err := pebble.Open(dir, uncompressed(&pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		ReadOnly:                    true,
	}))
	if err != nil {
		return err
	}
	defer db.Close()
	iter, err := db.NewIter(nil)
	if err != nil {
		return err
	}
	got := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		got[string(iter.Key())] = string(iter.Value())
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if !maps.Equal(got, want) {
		return fmt.Errorf("Pebble reads %v, not %v", got, want)
	}
	return nil
}

// tableNumbers lists the sstable file numbers in a directory.
func tableNumbers(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

// describe renders what Pebble reads back out of the directory, in the text
// roble's own dump produces. Reopening is the point: the ground truth is what
// Pebble believes the files say, not what this program fed in.
func describe(dir string) (string, error) {
	var out strings.Builder

	// The markers are filesystem truth, so they are read from the listing
	// rather than from an open database.
	names, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	var markers []string
	for _, e := range names {
		if strings.HasPrefix(e.Name(), "marker.") {
			markers = append(markers, e.Name())
		}
	}
	sort.Strings(markers)
	for _, m := range markers {
		fmt.Fprintf(&out, "marker %s\n", m)
	}

	db, err := pebble.Open(dir, uncompressed(&pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		ReadOnly:                    true,
	}))
	if err != nil {
		return "", err
	}
	defer db.Close()

	fmt.Fprintf(&out, "format-version %d\n", db.FormatMajorVersion())

	levels, err := db.SSTables()
	if err != nil {
		return "", err
	}
	for level, tables := range levels {
		if len(tables) == 0 {
			continue
		}
		fmt.Fprintf(&out, "level %d\n", level)
		// Pebble returns L0 newest-first; roble sorts by file number so the
		// two agree without either having to describe the other's order.
		sorted := make([]pebble.SSTableInfo, len(tables))
		copy(sorted, tables)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].FileNum < sorted[j].FileNum
		})
		for _, t := range sorted {
			fmt.Fprintf(&out, "  table %06d size=%d seq=%d..%d\n",
				t.FileNum, t.Size, t.SmallestSeqNum, t.LargestSeqNum)
			fmt.Fprintf(&out, "    smallest %s\n", key(t.Smallest))
			fmt.Fprintf(&out, "    largest  %s\n", key(t.Largest))
		}
	}
	return out.String(), nil
}

// key spells an internal key the way roble's dump does.
func key(k pebble.InternalKey) string {
	return fmt.Sprintf("%s #%d %s", hex.EncodeToString(k.UserKey), k.SeqNum(), kindName(k.Kind()))
}

// kindName spells a key kind the way roble's Kind::name does, which is
// lowercase. Identical to golden-sst's, including the panic: a fallback name
// for an unknown kind would let a mismatch through as a matching string.
func kindName(k pebble.InternalKeyKind) string {
	switch k {
	case pebble.InternalKeyKindDelete:
		return "del"
	case pebble.InternalKeyKindSet:
		return "set"
	case pebble.InternalKeyKindMerge:
		return "merge"
	case pebble.InternalKeyKindSingleDelete:
		return "singledel"
	case pebble.InternalKeyKindRangeDelete:
		return "rangedel"
	case pebble.InternalKeyKindSetWithDelete:
		return "setwithdel"
	case pebble.InternalKeyKindDeleteSized:
		return "delsized"
	default:
		panic(fmt.Sprintf("fixture uses kind %d, which the dump has no name for", k))
	}
}

func countFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	return len(entries)
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "db-fixture: %v\n", err)
	os.Exit(1)
}
