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
		// A pair built identically, so their tables get the same file
		// numbers, holding different keys. Two stores on one thread look
		// like this, and a cache keyed by file number alone would serve one
		// the other's table.
		{"db-twin-a", buildTwin("a")},
		{"db-twin-b", buildTwin("b")},
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
