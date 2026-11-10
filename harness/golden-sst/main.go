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

// Writes golden sstables with Go Pebble and dumps what Pebble reads back out
// of them. The dump is the ground truth roble's reader is judged against
// (DESIGN.md §12): it is produced by Pebble's own reader, not by the data this
// program fed in, so it reflects what Pebble believes the file says.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type entry struct {
	userKey []byte
	seqNum  pebble.SeqNum
	kind    pebble.InternalKeyKind
	value   []byte
}

type fixture struct {
	name      string
	format    sstable.TableFormat
	blockSize int
	compress  *block.CompressionProfile
	checksum  block.ChecksumType
	entries   []entry
	// tombstones are written through Writer.DeleteRange, which fixes their
	// sequence number at 0 — the only tombstone API reachable from outside
	// the Pebble module. Several sequence numbers over one range are covered
	// from the other direction, by compat/sst-roundtrip.
	tombstones [][2]string
	filter     sstable.FilterPolicy
}

// checksumOrDefault keeps crc32c as the default, as Pebble does, so only the
// fixture that means to exercise xxhash64 says so.
func (f fixture) checksumOrDefault() block.ChecksumType {
	if f.checksum == block.ChecksumTypeNone {
		return block.ChecksumTypeCRC32c
	}
	return f.checksum
}

func checksumName(c block.ChecksumType) string {
	switch c {
	case block.ChecksumTypeCRC32c:
		return "crc32c"
	case block.ChecksumTypeXXHash64:
		return "xxhash64"
	default:
		panic(fmt.Sprintf("checksum type %d has no dump name", c))
	}
}

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

// formatName spells a table format the way roble's TableFormat::name does.
// Pebble's own String() prints "(Pebble,v4)", which is not a name a dump can
// be compared on.
func formatName(tf sstable.TableFormat) string {
	switch tf {
	case sstable.TableFormatRocksDBv2:
		return "rocksdbv2"
	case sstable.TableFormatPebblev1:
		return "pebblev1"
	case sstable.TableFormatPebblev2:
		return "pebblev2"
	case sstable.TableFormatPebblev3:
		return "pebblev3"
	case sstable.TableFormatPebblev4:
		return "pebblev4"
	default:
		panic(fmt.Sprintf("table format %s is above roble's pin", tf))
	}
}

// oxiaKey imitates the keys Oxia's compare.Encoder produces: a hierarchical,
// slash-separated path, so that fixtures exercise long shared prefixes.
func oxiaKey(shard, i int) []byte {
	return []byte(fmt.Sprintf("/oxia/default/shard-%04d/users/%08d", shard, i))
}

func fixtures() []fixture {
	var fs []fixture

	// One data block, uncompressed, every byte visible by eye.
	simple := fixture{name: "simple", format: sstable.TableFormatPebblev4,
		blockSize: 4096, compress: block.NoCompression}
	for i, k := range []string{"/a", "/b", "/c", "/d/e", "/d/f", "/zzz"} {
		simple.entries = append(simple.entries, entry{
			userKey: []byte(k), seqNum: pebble.SeqNum(10 + i),
			kind: pebble.InternalKeyKindSet, value: []byte(fmt.Sprintf("value-of-%s", k)),
		})
	}
	fs = append(fs, simple)

	// Every point kind roble serves, so that kind decoding is exercised and a
	// mis-masked obsolete bit shows up.
	kinds := fixture{name: "kinds", format: sstable.TableFormatPebblev4,
		blockSize: 4096, compress: block.NoCompression}
	kindSpecs := []struct {
		key  string
		kind pebble.InternalKeyKind
		val  string
	}{
		{"/k01", pebble.InternalKeyKindSet, "set"},
		{"/k02", pebble.InternalKeyKindDelete, ""},
		{"/k03", pebble.InternalKeyKindSetWithDelete, "setwithdel"},
		{"/k04", pebble.InternalKeyKindSingleDelete, ""},
		{"/k05", pebble.InternalKeyKindSet, ""},
		{"/k06", pebble.InternalKeyKindSet, strings.Repeat("x", 300)},
	}
	for i, s := range kindSpecs {
		kinds.entries = append(kinds.entries, entry{
			userKey: []byte(s.key), seqNum: pebble.SeqNum(100 + i),
			kind: s.kind, value: []byte(s.val),
		})
	}
	fs = append(fs, kinds)

	// Many small blocks, so that the reader has to walk the index rather than
	// stopping after the first data block.
	multi := fixture{name: "multiblock", format: sstable.TableFormatPebblev4,
		blockSize: 512, compress: block.NoCompression}
	for i := 0; i < 400; i++ {
		multi.entries = append(multi.entries, entry{
			userKey: oxiaKey(3, i), seqNum: pebble.SeqNum(1000 + i),
			kind:  pebble.InternalKeyKindSet,
			value: []byte(fmt.Sprintf("{\"version\":%d,\"payload\":\"%s\"}", i, strings.Repeat("p", i%64))),
		})
	}
	fs = append(fs, multi)

	// Snappy, which is what Oxia configures for L1 and below. Values compress,
	// so most blocks really are compressed rather than stored raw by
	// MinReductionPercent.
	snappy := fixture{name: "snappy", format: sstable.TableFormatPebblev4,
		blockSize: 4096, compress: block.SnappyCompression}
	for i := 0; i < 400; i++ {
		snappy.entries = append(snappy.entries, entry{
			userKey: oxiaKey(7, i), seqNum: pebble.SeqNum(2000 + i),
			kind:  pebble.InternalKeyKindSet,
			value: []byte(strings.Repeat("compressible-", 20)),
		})
	}
	fs = append(fs, snappy)

	// Pebblev2: no value prefix byte on SET values, which is the branch a
	// reader gets wrong by one byte if it assumes the newest format.
	old := fixture{name: "pebblev2", format: sstable.TableFormatPebblev2,
		blockSize: 4096, compress: block.NoCompression}
	for i := 0; i < 20; i++ {
		old.entries = append(old.entries, entry{
			userKey: oxiaKey(1, i), seqNum: pebble.SeqNum(300 + i),
			kind: pebble.InternalKeyKindSet, value: []byte(fmt.Sprintf("old-%d", i)),
		})
	}
	fs = append(fs, old)

	// Zstd data blocks. ZstdCompression is used directly rather than
	// GoodCompression: Good embeds fastestCompression, which is MinLZ on
	// amd64, and Pebble then silently swaps the whole profile for Snappy
	// below Pebblev6 — so Good would produce different bytes depending on
	// the architecture that generated the corpus.
	zstd := fixture{name: "zstd", format: sstable.TableFormatPebblev4,
		blockSize: 2048, compress: block.ZstdCompression}
	for i := 0; i < 300; i++ {
		zstd.entries = append(zstd.entries, entry{
			userKey: oxiaKey(5, i), seqNum: pebble.SeqNum(4000 + i),
			kind:  pebble.InternalKeyKindSet,
			value: []byte(strings.Repeat("zstd-compressible-", 15)),
		})
	}
	fs = append(fs, zstd)

	// xxhash64 block checksums. Oxia never selects these — Pebble defaults to
	// crc32c — but it is one option away, so roble reads them.
	xx := fixture{name: "xxhash64", format: sstable.TableFormatPebblev4,
		blockSize: 1024, compress: block.NoCompression,
		checksum: block.ChecksumTypeXXHash64}
	for i := 0; i < 120; i++ {
		xx.entries = append(xx.entries, entry{
			userKey: oxiaKey(9, i), seqNum: pebble.SeqNum(6000 + i),
			kind: pebble.InternalKeyKindSet, value: []byte(fmt.Sprintf("x-%d", i)),
		})
	}
	fs = append(fs, xx)

	// A bloom filter at Oxia's 10 bits per key. The point of this fixture is
	// that roble can rebuild the identical bits from the same keys, which is
	// what catches the sign-extended tail in the hash.
	bl := fixture{name: "bloom", format: sstable.TableFormatPebblev4,
		blockSize: 1024, compress: block.NoCompression,
		filter: bloom.FilterPolicy(10)}
	for i := 0; i < 500; i++ {
		// The hash folds its last one to three bytes in separately, sign
		// extended. oxiaKey is 39 bytes, so appending one to four high bytes
		// covers every length modulo 4 — including the three that reach the
		// sign-extended tail at all. A fixture whose keys are all a multiple
		// of four bytes long never exercises it, and an unsigned fold then
		// produces identical bits.
		tail := make([]byte, 1+i%4)
		for j := range tail {
			tail[j] = byte(0x80 + (i+j)%0x7f)
		}
		k := append(oxiaKey(4, i), tail...)
		bl.entries = append(bl.entries, entry{
			userKey: k, seqNum: pebble.SeqNum(8000 + i),
			kind: pebble.InternalKeyKindSet, value: []byte(fmt.Sprintf("b-%d", i)),
		})
	}
	fs = append(fs, bl)

	// Range deletions, which live in their own block and never in a data
	// block. Before this fixture existed, a table with tombstones read as
	// though it had none and no test noticed.
	rd := fixture{name: "rangedel", format: sstable.TableFormatPebblev4,
		blockSize: 1024, compress: block.NoCompression}
	for i := 0; i < 60; i++ {
		rd.entries = append(rd.entries, entry{
			userKey: oxiaKey(2, i), seqNum: pebble.SeqNum(7000 + i),
			kind: pebble.InternalKeyKindSet, value: []byte(fmt.Sprintf("r-%d", i)),
		})
	}
	rd.tombstones = [][2]string{
		{"/oxia/default/shard-0002/users/00000005", "/oxia/default/shard-0002/users/00000010"},
		{"/oxia/default/shard-0002/users/00000020", "/oxia/default/shard-0002/users/00000021"},
		{"/oxia/default/shard-0002/users/00000030", "/oxia/default/shard-0002/users/00000050"},
	}
	fs = append(fs, rd)

	return fs
}

func write(path string, f fixture) error {
	file, err := vfs.Default.Create(path, "golden-sst")
	if err != nil {
		return err
	}
	opts := sstable.WriterOptions{
		TableFormat:  f.format,
		BlockSize:    f.blockSize,
		Compression:  f.compress,
		Checksum:     f.checksumOrDefault(),
		Comparer:     sstable.DefaultComparer,
		MergerName:   "pebble.concatenate",
		FilterPolicy: f.filter,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), opts)
	// An sstable needs strictly increasing internal keys.
	sorted := append([]entry(nil), f.entries...)
	sort.Slice(sorted, func(i, j int) bool {
		if c := bytes.Compare(sorted[i].userKey, sorted[j].userKey); c != 0 {
			return c < 0
		}
		return sorted[i].seqNum > sorted[j].seqNum
	})
	for _, e := range sorted {
		ik := pebble.MakeInternalKey(e.userKey, e.seqNum, e.kind)
		if err := w.Raw().Add(ik, e.value, false /* forceObsolete */); err != nil {
			return fmt.Errorf("add %q: %w", e.userKey, err)
		}
	}
	// DeleteRange needs already-fragmented spans in sorted order.
	for _, t := range f.tombstones {
		if err := w.DeleteRange([]byte(t[0]), []byte(t[1])); err != nil {
			return fmt.Errorf("delete range %q..%q: %w", t[0], t[1], err)
		}
	}
	return w.Close()
}

func dump(path string, f fixture) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	r, err := sstable.NewMemReader(data, sstable.ReaderOptions{
		Comparer: sstable.DefaultComparer,
		Merger:   nil,
	})
	if err != nil {
		return "", err
	}
	defer r.Close()

	props, err := r.ReadPropertiesBlock(context.Background(), nil)
	if err != nil {
		return "", err
	}
	// These fixtures are meant to hold no value blocks: with the bytewise
	// comparer a value only goes to a value block when the same user key
	// repeats inside one table, which none of them does. Assert it rather than
	// assume it, so the fixture cannot drift out from under the dump.
	if props.NumValueBlocks != 0 {
		return "", fmt.Errorf("%s: %d value blocks, fixtures must have none",
			f.name, props.NumValueBlocks)
	}

	var b strings.Builder
	tf, err := r.TableFormat()
	if err != nil {
		return "", err
	}
	fmt.Fprintf(&b, "format %s\n", formatName(tf))
	fmt.Fprintf(&b, "checksum %s\n", checksumName(f.checksumOrDefault()))
	fmt.Fprintf(&b, "datablocks %d\n", props.NumDataBlocks)

	iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		return "", err
	}
	defer iter.Close()

	var lines []string
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		val, _, err := kv.V.Value(nil)
		if err != nil {
			return "", err
		}
		lines = append(lines, fmt.Sprintf("entry %016x %s %s %s",
			uint64(kv.K.SeqNum()), kindName(kv.K.Kind()),
			hex.EncodeToString(kv.K.UserKey), hex.EncodeToString(val)))
	}
	if err := iter.Error(); err != nil {
		return "", err
	}
	var tombs []string
	rdIter, err := r.NewRawRangeDelIter(context.Background(), sstable.NoFragmentTransforms, sstable.ReadEnv{})
	if err != nil {
		return "", fmt.Errorf("rangedel iter: %w", err)
	}
	if rdIter != nil {
		defer rdIter.Close()
		span, err := rdIter.First()
		for ; span != nil && err == nil; span, err = rdIter.Next() {
			// A fragment iterator coalesces one [start,end) with every
			// sequence number that deletes it, so one span can be several
			// tombstones — which is exactly how they sit in the block.
			for _, k := range span.Keys {
				tombs = append(tombs, fmt.Sprintf("tombstone %016x %s %s",
					uint64(k.SeqNum()), hex.EncodeToString(span.Start),
					hex.EncodeToString(span.End)))
			}
		}
		if err != nil {
			return "", fmt.Errorf("rangedel iterate: %w", err)
		}
	}

	fmt.Fprintf(&b, "entries %d\n", len(lines))
	fmt.Fprintf(&b, "tombstones %d\n", len(tombs))
	for _, l := range lines {
		b.WriteString(l)
		b.WriteString("\n")
	}
	for _, l := range tombs {
		b.WriteString(l)
		b.WriteString("\n")
	}
	return b.String(), nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: golden-sst <output-dir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	fmt.Printf("%-14s %10s %10s %8s  %s\n", "fixture", "entries", "bytes", "blocks", "format")
	for _, f := range fixtures() {
		sst := filepath.Join(dir, f.name+".sst")
		if err := write(sst, f); err != nil {
			panic(fmt.Sprintf("%s: write: %v", f.name, err))
		}
		d, err := dump(sst, f)
		if err != nil {
			panic(fmt.Sprintf("%s: dump: %v", f.name, err))
		}
		if err := os.WriteFile(filepath.Join(dir, f.name+".dump"), []byte(d), 0o644); err != nil {
			panic(err)
		}
		st, _ := os.Stat(sst)
		var blocks, entries string
		for _, line := range strings.Split(d, "\n") {
			if after, ok := strings.CutPrefix(line, "datablocks "); ok {
				blocks = after
			}
			if after, ok := strings.CutPrefix(line, "entries "); ok {
				entries = after
			}
		}
		fmt.Printf("%-14s %10s %10d %8s  %s\n", f.name, entries, st.Size(), blocks,
			formatName(f.format))
	}
}
