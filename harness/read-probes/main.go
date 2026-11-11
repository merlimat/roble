// Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Builds a database with a history messy enough to be worth reading, and
// writes down what Pebble answers to each of roble's five comparisons for
// every probe key. roble's `get` is judged against it (DESIGN.md §12).
//
// Pebble is the oracle here rather than a model written beside roble. DESIGN.md
// §12 asks for model-based tests against a BTreeMap, and those become worth
// writing in M3, when roble can write its own databases and the model can be
// driven by the same operations. Today roble can only read, so the operations
// are Pebble's and the only model worth comparing against is Pebble itself —
// which is also the stronger oracle, since it is the thing roble has to agree
// with.
//
// The history is generated from a fixed seed, so the fixture is reproducible,
// and is deliberately unkind: a small key alphabet so overwrites and deletes
// pile up on the same keys, range deletions that cut across flushed tables,
// and explicit flushes and compactions so keys end up at different levels with
// L0 tables that overlap.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const seed = 20261111

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: read-probes <output-dir>")
		os.Exit(2)
	}
	out := os.Args[1]
	dir := filepath.Join(out, "db-reads")
	if err := os.RemoveAll(dir); err != nil {
		fail(err)
	}
	keys, err := build(dir)
	if err != nil {
		fail(err)
	}
	dump, err := probe(dir, keys)
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(filepath.Join(out, "db-reads.probes"), []byte(dump), 0o644); err != nil {
		fail(err)
	}
	fmt.Printf("db-reads: %d probe keys\n", len(probesFor(keys)))
}

// alphabet is small on purpose: with few distinct keys, a few hundred
// operations leave many versions, tombstones and resurrections per key.
func alphabet() [][]byte {
	var keys [][]byte
	for _, a := range "abcdef" {
		for _, b := range "0123" {
			keys = append(keys, []byte{byte(a), byte(b)})
		}
	}
	// A few longer keys that share prefixes with the short ones, so prefix
	// compression and the successor-of-a-user-key step get exercised.
	for _, extra := range []string{"a0", "a00", "a000", "c2x", "f3zzz"} {
		keys = append(keys, []byte(extra))
	}
	return keys
}

// scripted keys are written by hand rather than at random, because the kinds
// they exercise have invariants a random mix would break.
//
// Their names deliberately use letters the random alphabet does not, so that a
// later round cannot overwrite the sequence and leave the kind in the database
// but not as any key's newest entry — which is where its visibility is
// decided.
//
// SINGLEDEL is only defined when the key was Set exactly once since its last
// deletion; DELSIZED is only defined when the size given is the real one.
// Both are kinds roble reads and never writes (§3), and leaving them out
// entirely would mean the engine's view of a deletion was only ever tested
// against the ordinary DELETE. So they get an exact, legal sequence each, on
// keys the random rounds never touch.
func scripted() [][]byte {
	return [][]byte{
		[]byte("s0"), // Set, flush, SingleDelete: gone.
		[]byte("s1"), // Set, flush, SingleDelete, flush, Set: present again.
		[]byte("t0"), // Set, then DeleteSized with the true size: gone.
		[]byte("t1"), // Set, DeleteSized, Set: present again.
		// A range deletion's bounds, exactly. `[start, end)` is the whole
		// semantics of a tombstone, and a probe only tests it if it lands on
		// an endpoint of one while otherwise being visible, which no random
		// range did. So: u0 == start is deleted, u2 == end survives.
		[]byte("u0"),
		[]byte("u1"),
		[]byte("u2"),
		// Keys whose bytes are not text. The least key after `w0` is
		// `w0\x00`, so a reader that steps past a user key by appending
		// anything other than a zero byte skips this one — and nothing else
		// in this fixture, or in Oxia's path-shaped keys, would ever notice.
		[]byte("w0"),
		{'w', '0', 0x00},
		{'w', '0', 0x00, 0x00},
		{'w', '0', 0xff},
		{'w', '1', 0xff, 0x00},
	}
}

func build(dir string) ([][]byte, error) {
	opts := &pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		DisableWAL:                  true,
	}
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		return pebble.DBCompressionNone
	})
	// Small blocks, so every table holds several of them.
	//
	// With one data block per table every block sits at offset 0, and a block
	// cache that lost the offset from its key would still find the right
	// block every time — the fixture would look like it exercised the cache
	// and would not.
	smallBlocks(opts)
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	keys := alphabet()
	rng := rand.New(rand.NewSource(seed))

	// The scripted sequences first, each legal for the kind it uses.
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	// SINGLEDEL has to be separated from the Set it cancels by a flush.
	// Eliding that pair is exactly what SingleDelete is for, so in one batch
	// Pebble drops both and the kind never reaches a table — which leaves a
	// reader's handling of it untested while looking like it is tested.
	first := db.NewBatch()
	must(first.Set([]byte("s0"), []byte("gone-by-singledel"), nil))
	must(first.Set([]byte("s1"), []byte("first"), nil))
	if err := first.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}

	second := db.NewBatch()
	must(second.SingleDelete([]byte("s0"), nil))
	must(second.SingleDelete([]byte("s1"), nil))
	if err := second.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}

	pre := db.NewBatch()
	must(pre.Set([]byte("s1"), []byte("set-again-after-singledel"), nil))
	const sized = "gone-by-delsized"
	must(pre.Set([]byte("t0"), []byte(sized), nil))
	must(pre.DeleteSized([]byte("t0"), uint32(len(sized)), nil))
	must(pre.Set([]byte("t1"), []byte(sized), nil))
	must(pre.DeleteSized([]byte("t1"), uint32(len(sized)), nil))
	must(pre.Set([]byte("t1"), []byte("set-again-after-delsized"), nil))
	if err := pre.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}

	// Keys with zero and high bytes in them, so that stepping past a user key
	// is tested on the one shape where appending a zero byte matters.
	bytesy := db.NewBatch()
	for _, k := range [][]byte{
		[]byte("w0"),
		{'w', '0', 0x00},
		{'w', '0', 0x00, 0x00},
		{'w', '0', 0xff},
		{'w', '1', 0xff, 0x00},
	} {
		must(bytesy.Set(k, append([]byte("binary-"), k...), nil))
	}
	// And one of them deleted, so a search has to step over it onto the next.
	must(bytesy.Delete([]byte{'w', '0', 0x00}, nil))
	if err := bytesy.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}

	// The tombstone boundary case, newer than the keys it covers.
	bounds := db.NewBatch()
	must(bounds.Set([]byte("u0"), []byte("at-the-start-deleted"), nil))
	must(bounds.Set([]byte("u1"), []byte("inside-deleted"), nil))
	must(bounds.Set([]byte("u2"), []byte("at-the-end-survives"), nil))
	if err := bounds.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}
	cut := db.NewBatch()
	must(cut.DeleteRange([]byte("u0"), []byte("u2"), nil))
	if err := cut.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}

	for round := 0; round < 12; round++ {
		b := db.NewBatch()
		for i := 0; i < 24; i++ {
			k := keys[rng.Intn(len(keys))]
			switch rng.Intn(10) {
			case 0, 1:
				if err := b.Delete(k, nil); err != nil {
					return nil, err
				}
			case 2:
				// A range deletion over a span of the alphabet, which is the
				// thing a reader has to keep consulting (§2).
				lo := keys[rng.Intn(len(keys))]
				hi := keys[rng.Intn(len(keys))]
				if string(lo) > string(hi) {
					lo, hi = hi, lo
				}
				if string(lo) != string(hi) {
					if err := b.DeleteRange(lo, hi, nil); err != nil {
						return nil, err
					}
				}
			default:
				v := fmt.Sprintf("r%d-i%d-%s", round, i, k)
				if err := b.Set(k, []byte(v), nil); err != nil {
					return nil, err
				}
			}
		}
		if err := b.Commit(pebble.NoSync); err != nil {
			return nil, err
		}
		if err := db.Flush(); err != nil {
			return nil, err
		}
		// Compact a narrow slice now and then, so some keys settle deep while
		// others stay in L0. Compacting the whole alphabet — which an earlier
		// version of this did — collapses the database to a single table and
		// leaves the cross-level merge untested.
		switch round {
		case 3:
			if err := db.Compact(context.Background(), []byte("a"), []byte("b"), true); err != nil {
				return nil, err
			}
		case 7:
			if err := db.Compact(context.Background(), []byte("c"), []byte("d"), true); err != nil {
				return nil, err
			}
		}
	}
	if err := db.Flush(); err != nil {
		return nil, err
	}

	// The fixture is only worth having if the read path has somewhere to go
	// wrong, so the shape it ended up in is asserted rather than hoped for.
	levels, err := db.SSTables()
	if err != nil {
		return nil, err
	}
	occupied, inL0 := 0, 0
	for level, tables := range levels {
		if len(tables) > 0 {
			occupied++
			if level == 0 {
				inL0 = len(tables)
			}
			fmt.Printf("  L%d: %d tables\n", level, len(tables))
		}
	}
	if occupied < 2 {
		return nil, fmt.Errorf("every table ended up in one level; the cross-level merge would go untested")
	}
	if inL0 < 2 {
		return nil, fmt.Errorf("L0 holds %d tables; overlapping tables in L0 would go untested", inL0)
	}
	return keys, db.Close()
}

// smallBlocks makes each level write small data blocks.
func smallBlocks(o *pebble.Options) {
	for i := range o.Levels {
		o.Levels[i].BlockSize = 128
		o.Levels[i].IndexBlockSize = 128
	}
}

// probesFor is every key worth asking about: the alphabet, the keys just
// either side of each, and a few outside it entirely.
func probesFor(keys [][]byte) [][]byte {
	seen := map[string]bool{}
	var out [][]byte
	add := func(p []byte) {
		if !seen[string(p)] {
			seen[string(p)] = true
			out = append(out, p)
		}
	}
	add([]byte{})
	add([]byte("\x00"))
	add([]byte("zzz"))
	add([]byte{0xff})
	for _, k := range append(append([][]byte(nil), keys...), scripted()...) {
		add(k)
		add(append(append([]byte(nil), k...), 0x00))
		if k[len(k)-1] > 0 {
			before := append([]byte(nil), k...)
			before[len(before)-1]--
			add(before)
		}
	}
	sort.Slice(out, func(i, j int) bool { return string(out[i]) < string(out[j]) })
	return out
}

func probe(dir string, keys [][]byte) (string, error) {
	opts := &pebble.Options{
		FS:                          vfs.Default,
		FormatMajorVersion:          pebble.FormatVirtualSSTables,
		DisableAutomaticCompactions: true,
		ReadOnly:                    true,
	}
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		return pebble.DBCompressionNone
	})
	smallBlocks(opts)
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return "", err
	}
	defer db.Close()

	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return "", err
	}
	defer iter.Close()

	// successor is the least key strictly greater than k, which is how the
	// strict comparisons are expressed in terms of SeekGE and SeekLT.
	successor := func(k []byte) []byte {
		return append(append([]byte(nil), k...), 0x00)
	}

	var out strings.Builder
	for _, p := range probesFor(keys) {
		fmt.Fprintf(&out, "probe %s\n", hex.EncodeToString(p))

		// equal
		v, closer, err := db.Get(p)
		switch {
		case err == nil:
			fmt.Fprintf(&out, "  equal %s\n", hex.EncodeToString(v))
			if err := closer.Close(); err != nil {
				return "", err
			}
		case errors.Is(err, pebble.ErrNotFound):
			out.WriteString("  equal none\n")
		default:
			return "", err
		}

		// The four ordered comparisons, each one seek.
		fmt.Fprintf(&out, "  ceiling %s\n", at(iter, iter.SeekGE(p)))
		fmt.Fprintf(&out, "  higher %s\n", at(iter, iter.SeekGE(successor(p))))
		fmt.Fprintf(&out, "  floor %s\n", at(iter, iter.SeekLT(successor(p))))
		fmt.Fprintf(&out, "  lower %s\n", at(iter, iter.SeekLT(p)))
	}
	return out.String(), iter.Error()
}

func at(iter *pebble.Iterator, ok bool) string {
	if !ok {
		return "none"
	}
	return fmt.Sprintf("%s %s",
		hex.EncodeToString(iter.Key()), hex.EncodeToString(iter.Value()))
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "read-probes: %v\n", err)
	os.Exit(1)
}
