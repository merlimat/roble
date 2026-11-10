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

// Opens tables roble wrote with Go Pebble and dumps what Pebble reads out of
// them, in the same canonical format roble's own dump uses. This is the M1
// gate from DESIGN.md §13: Go Pebble opens and reads what roble wrote.
//
// It deliberately opens them the way Oxia does — the default bytewise
// comparer, no merger configured, no filter policy — rather than with options
// tailored to make them load.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

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
		panic(fmt.Sprintf("table holds kind %d, which the dump has no name for", k))
	}
}

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

func dump(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	r, err := sstable.NewMemReader(data, sstable.ReaderOptions{
		Comparer: sstable.DefaultComparer,
	})
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer r.Close()

	props, err := r.ReadPropertiesBlock(context.Background(), nil)
	if err != nil {
		return "", fmt.Errorf("properties: %w", err)
	}
	tf, err := r.TableFormat()
	if err != nil {
		return "", fmt.Errorf("table format: %w", err)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "format %s\n", formatName(tf))
	fmt.Fprintf(&b, "checksum crc32c\n")
	fmt.Fprintf(&b, "datablocks %d\n", props.NumDataBlocks)

	iter, err := r.NewIter(sstable.NoTransforms, nil, nil, sstable.AssertNoBlobHandles)
	if err != nil {
		return "", fmt.Errorf("iter: %w", err)
	}
	defer iter.Close()

	var lines []string
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		val, _, err := kv.V.Value(nil)
		if err != nil {
			return "", fmt.Errorf("value for %q: %w", kv.K.UserKey, err)
		}
		lines = append(lines, fmt.Sprintf("entry %016x %s %s %s",
			uint64(kv.K.SeqNum()), kindName(kv.K.Kind()),
			hex.EncodeToString(kv.K.UserKey), hex.EncodeToString(val)))
	}
	if err := iter.Error(); err != nil {
		return "", fmt.Errorf("iterate: %w", err)
	}
	fmt.Fprintf(&b, "entries %d\n", len(lines))
	for _, l := range lines {
		b.WriteString(l)
		b.WriteString("\n")
	}
	return b.String(), nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: verify-sst <dir-of-sst-files>")
		os.Exit(2)
	}
	dir := os.Args[1]
	matches, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		panic(err)
	}
	if len(matches) == 0 {
		fmt.Fprintf(os.Stderr, "no .sst files in %s\n", dir)
		os.Exit(1)
	}
	sort.Strings(matches)
	failed := 0
	for _, sst := range matches {
		name := strings.TrimSuffix(filepath.Base(sst), ".sst")
		d, err := dump(sst)
		if err != nil {
			fmt.Printf("%-14s FAILED: %v\n", name, err)
			failed++
			continue
		}
		out := filepath.Join(dir, name+".godump")
		if err := os.WriteFile(out, []byte(d), 0o644); err != nil {
			panic(err)
		}
		entries := ""
		for _, line := range strings.Split(d, "\n") {
			if after, ok := strings.CutPrefix(line, "entries "); ok {
				entries = after
			}
		}
		fmt.Printf("%-14s read by Go Pebble, %s entries\n", name, entries)
	}
	if failed > 0 {
		os.Exit(1)
	}
}
