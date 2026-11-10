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

// Produces real record-framed files for roble to read: a WAL, whose chunks are
// recyclable, and a MANIFEST, whose chunks are legacy.
//
// Pebble's own record.NewReader and record.NewWriter cannot be driven from
// outside the module — their log-number parameter is base.DiskFileNum, which
// lives in internal/base — so instead of synthesising chunks this opens a real
// database and copies out what Pebble wrote. The files are more realistic than
// anything a harness would construct, and roble is then required both to
// decode them and to re-frame the same payloads into the same bytes.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: record-files <output-dir>")
		os.Exit(2)
	}
	outDir := os.Args[1]
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		panic(err)
	}

	dir, err := os.MkdirTemp("", "record-files")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// WAL enabled, which is the opposite of Oxia's configuration, because the
	// point is to obtain a WAL to read. Several batches of different shapes so
	// that the framing sees both small records and ones that span blocks.
	db, err := pebble.Open(dir, &pebble.Options{DisableWAL: false})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 20; i++ {
		b := db.NewBatch()
		for j := 0; j < 1+i%5; j++ {
			key := fmt.Sprintf("/oxia/default/shard-0001/users/%08d", i*10+j)
			if err := b.Set([]byte(key), []byte(strings.Repeat("v", 1+(i*j)%4096)), nil); err != nil {
				panic(err)
			}
		}
		if err := b.Commit(pebble.Sync); err != nil {
			panic(err)
		}
		if err := b.Close(); err != nil {
			panic(err)
		}
	}
	// One batch large enough to span several 32 KiB blocks on its own.
	big := db.NewBatch()
	if err := big.Set([]byte("/big"), []byte(strings.Repeat("B", 200*1024)), nil); err != nil {
		panic(err)
	}
	if err := big.Commit(pebble.Sync); err != nil {
		panic(err)
	}
	if err := big.Close(); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	// A flush rotates the WAL, so the database above is deliberately never
	// flushed: all of its batches land in one log. The MANIFEST needs the
	// opposite — flushes, to accumulate version edits — so it comes from a
	// second database.
	manifestDir, err := os.MkdirTemp("", "record-files-manifest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(manifestDir)
	mdb, err := pebble.Open(manifestDir, &pebble.Options{DisableWAL: false})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 8; i++ {
		if err := mdb.Set([]byte(fmt.Sprintf("/m/%04d", i)), []byte("v"), pebble.Sync); err != nil {
			panic(err)
		}
		if err := mdb.Flush(); err != nil {
			panic(err)
		}
	}
	if err := mdb.Close(); err != nil {
		panic(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	var logs []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			logs = append(logs, e.Name())
		}
	}
	manifestEntries, err := os.ReadDir(manifestDir)
	if err != nil {
		panic(err)
	}
	var manifests []string
	for _, e := range manifestEntries {
		if strings.HasPrefix(e.Name(), "MANIFEST-") {
			manifests = append(manifests, e.Name())
		}
	}
	sort.Strings(logs)
	sort.Strings(manifests)
	if len(logs) == 0 || len(manifests) == 0 {
		panic(fmt.Sprintf("expected a log and a manifest, got logs=%v manifests=%v", logs, manifests))
	}

	var index strings.Builder
	index.WriteString("# Record-framed files written by Go Pebble v2.1.7.\n")
	index.WriteString("# Regenerate with: cd harness && go run ./record-files ../format/tests/golden\n")

	// The WAL's chunks carry the log number, which is its file name.
	logName := logs[len(logs)-1]
	logNum, err := strconv.ParseUint(strings.TrimSuffix(logName, ".log"), 10, 32)
	if err != nil {
		panic(fmt.Sprintf("log name %q: %v", logName, err))
	}
	copyFile(filepath.Join(dir, logName), filepath.Join(outDir, "records-wal.log"))
	fmt.Fprintf(&index, "wal records-wal.log lognum=%d\n", logNum)

	// The MANIFEST's chunks are legacy and carry no log number.
	manifestName := manifests[len(manifests)-1]
	copyFile(filepath.Join(manifestDir, manifestName), filepath.Join(outDir, "records-manifest"))
	fmt.Fprintf(&index, "manifest records-manifest\n")

	if err := os.WriteFile(filepath.Join(outDir, "records.txt"), []byte(index.String()), 0o644); err != nil {
		panic(err)
	}

	walInfo, _ := os.Stat(filepath.Join(outDir, "records-wal.log"))
	manInfo, _ := os.Stat(filepath.Join(outDir, "records-manifest"))
	fmt.Printf("%-22s %10s %8s\n", "file", "bytes", "lognum")
	fmt.Printf("%-22s %10d %8d\n", "records-wal.log", walInfo.Size(), logNum)
	fmt.Printf("%-22s %10d %8s\n", "records-manifest", manInfo.Size(), "-")
}

func copyFile(from, to string) {
	data, err := os.ReadFile(from)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(to, data, 0o644); err != nil {
		panic(err)
	}
}
