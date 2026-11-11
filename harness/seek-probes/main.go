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
// Dumps what Pebble's own iterator answers to SeekGE and SeekLT over each
// table in the golden corpus. roble's seeks are judged against this
// (DESIGN.md §12).
//
// Why a separate harness rather than more of golden-sst's dump: that dump is a
// full scan, and a full scan cannot tell a correct seek from one that lands on
// the wrong side of a block boundary, or from one that cannot be stepped away
// from afterwards. Seeking is the part of the read path with its own failure
// modes, so it gets its own ground truth.
//
// The probes are derived from each table's own keys — every key, and the keys
// immediately on either side of it — because the interesting targets are the
// ones that fall exactly on, just before and just after a stored key, and
// between two blocks.
package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: seek-probes <golden-dir>")
		os.Exit(2)
	}
	dir := os.Args[1]

	paths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		fail(err)
	}
	sort.Strings(paths)

	var out strings.Builder
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".sst")
		// A table roble refuses outright has no seeks to compare.
		if strings.HasPrefix(name, "refuse-") {
			continue
		}
		section, err := probe(path, name)
		if err != nil {
			fail(fmt.Errorf("%s: %w", name, err))
		}
		out.WriteString(section)
	}

	dest := filepath.Join(dir, "seeks.txt")
	if err := os.WriteFile(dest, []byte(out.String()), 0o644); err != nil {
		fail(err)
	}
	fmt.Printf("wrote %s\n", dest)
}

func probe(path, name string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	r, err := sstable.NewMemReader(data, sstable.ReaderOptions{
		Comparer: sstable.DefaultComparer,
		Filters: map[string]sstable.FilterPolicy{
			"rocksdb.BuiltinBloomFilter": bloom.FilterPolicy(10),
		},
	})
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer r.Close()

	iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		return "", fmt.Errorf("iter: %w", err)
	}
	defer iter.Close()

	// Every user key in the table, in order.
	var keys [][]byte
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		k := append([]byte(nil), kv.K.UserKey...)
		if len(keys) == 0 || !bytes.Equal(keys[len(keys)-1], k) {
			keys = append(keys, k)
		}
	}
	if err := iter.Error(); err != nil {
		return "", err
	}

	var out strings.Builder
	fmt.Fprintf(&out, "fixture %s\n", name)
	for _, p := range probesFor(sample(keys)) {
		fmt.Fprintf(&out, "probe %s\n", hex.EncodeToString(p))

		// `kv` is only ever assigned with `:=`, never declared: its type
		// lives in pebble's internal/base and cannot be named from here.
		kv := iter.SeekGE(p, 0)
		if kv == nil {
			out.WriteString("  ge none\n")
		} else {
			val, _, err := kv.V.Value(nil)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&out, "  ge %s\n",
				render(kv.K.UserKey, uint64(kv.K.SeqNum()), kv.K.Kind(), val))
		}

		kv = iter.SeekLT(p, 0)
		if kv == nil {
			out.WriteString("  lt none\n")
		} else {
			val, _, err := kv.V.Value(nil)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(&out, "  lt %s\n",
				render(kv.K.UserKey, uint64(kv.K.SeqNum()), kv.K.Kind(), val))
		}
	}
	if err := iter.Error(); err != nil {
		return "", err
	}
	return out.String(), nil
}

// sample thins a large table's keys down to a fixed, spread-out set.
//
// Every key of every table would make this corpus several megabytes, almost
// all of it near-identical long keys from the bloom fixture, which buys very
// little: what a seek can get wrong is decided by a key's position relative to
// block boundaries and to its neighbours, not by how many keys there are. The
// ends are kept whole because first and last are the edge cases, and the
// middle is spread evenly so that a table of many small blocks still gets
// probes landing in different blocks. Exhaustive coverage of block boundaries
// is the job of the unit tests in block/restart.rs, which build blocks on
// purpose.
func sample(keys [][]byte) [][]byte {
	const (
		all  = 48
		ends = 8
		mid  = 32
	)
	if len(keys) <= all {
		return keys
	}
	out := append([][]byte(nil), keys[:ends]...)
	inner := keys[ends : len(keys)-ends]
	for i := 0; i < mid; i++ {
		out = append(out, inner[i*len(inner)/mid])
	}
	return append(out, keys[len(keys)-ends:]...)
}

// probesFor builds the seek targets for a table: each key, and the keys
// immediately either side of it, which is where a seek goes wrong.
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
	add([]byte{0xff})
	for _, k := range keys {
		add(k)
		// Just after k.
		add(append(append([]byte(nil), k...), 0x00))
		// Just before k: drop its last byte, and decrement it.
		if len(k) > 1 {
			add(append([]byte(nil), k[:len(k)-1]...))
		}
		if len(k) > 0 && k[len(k)-1] > 0 {
			before := append([]byte(nil), k...)
			before[len(before)-1]--
			add(before)
			// And a key that sorts between `before` and k.
			add(append(before, 0xff))
		}
	}

	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i], out[j]) < 0 })
	return out
}

// render spells one seek result the way roble's test does.
//
// The value is given as its length and first bytes rather than whole. What a
// seek can get wrong about a value is the range it reads it from, and a wrong
// offset or length shows up in either the length or the leading bytes; the
// values themselves are already pinned byte for byte by golden-sst's full
// dump, and writing them out again here would make this file several times
// larger for no new claim.
func render(userKey []byte, seq uint64, kind pebble.InternalKeyKind, val []byte) string {
	const lead = 8
	head := val
	if len(head) > lead {
		head = head[:lead]
	}
	return fmt.Sprintf("%s #%d %s v=%d:%s",
		hex.EncodeToString(userKey), seq, kindName(kind), len(val), hex.EncodeToString(head))
}

// kindName spells a key kind the way roble's Kind::name does, lowercase, and
// panics on one the dump has no name for, as golden-sst does.
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

func fail(err error) {
	fmt.Fprintf(os.Stderr, "seek-probes: %v\n", err)
	os.Exit(1)
}
