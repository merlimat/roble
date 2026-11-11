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
// Opens a database roble wrote to and checks that Pebble finds what roble
// says it wrote.
//
// Nothing roble wrote has been flushed, so every key here can only have come
// out of the log. Pebble finding them means Pebble replayed roble's record
// framing, roble's batch representation and roble's sequence numbers — the
// M3 gate of DESIGN.md §13, in the direction that matters most.
//
// In `swept` mode roble wrote nothing and deleted instead, and the keys are
// every key roble can still read. Pebble finding all of them is the claim
// that the files roble collected took nothing with them.
package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: verify-wal <database-dir> [log|flushed|rotated|compacted|no-wal|swept]")
		os.Exit(2)
	}
	dir := os.Args[1]
	mode := "log"
	if len(os.Args) == 3 {
		mode = os.Args[2]
	}

	expected, deleted, err := readExpectation(filepath.Join(dir, "roble-wrote.txt"))
	if err != nil {
		fail(err)
	}
	if len(expected) == 0 {
		fail(errors.New("roble says it wrote nothing, so this proves nothing"))
	}

	opts := &pebble.Options{
		FS:                 vfs.Default,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		ReadOnly:           true,
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		fail(fmt.Errorf("Pebble could not open the database roble wrote to: %w", err))
	}
	defer db.Close()

	bad := 0
	for _, kv := range expected {
		got, closer, err := db.Get(kv.key)
		switch {
		case errors.Is(err, pebble.ErrNotFound):
			fmt.Printf("  %s: MISSING — Pebble did not replay it\n", hex.EncodeToString(kv.key))
			bad++
			continue
		case err != nil:
			fail(err)
		}
		if string(got) != string(kv.value) {
			fmt.Printf("  %s: WRONG VALUE — %d bytes, wanted %d\n",
				hex.EncodeToString(kv.key), len(got), len(kv.value))
			bad++
		}
		if err := closer.Close(); err != nil {
			fail(err)
		}
	}

	for _, k := range deleted {
		_, closer, err := db.Get(k)
		switch {
		case errors.Is(err, pebble.ErrNotFound):
			// Right: roble's delete was replayed over the table's value.
		case err != nil:
			fail(err)
		default:
			fmt.Printf("  %s: STILL THERE — roble's delete was not replayed\n",
				hex.EncodeToString(k))
			bad++
			if err := closer.Close(); err != nil {
				fail(err)
			}
		}
	}

	if bad != 0 {
		fmt.Printf("\n%d of %d checks failed\n", bad, len(expected)+len(deleted))
		os.Exit(1)
	}
	fmt.Printf("%d keys and %d deletes checked\n", len(expected), len(deleted))
	// Which claim this run made depends on where the keys had to come from.
	// Flushed, the MANIFEST's min_unflushed has moved past the log roble
	// wrote, so Pebble does not replay it: every key came out of the table.
	switch mode {
	case "flushed":
		fmt.Println("\nVERDICT: Go Pebble reads the table roble wrote, through the MANIFEST roble appended")
	case "rotated":
		fmt.Println("\nVERDICT: Go Pebble reads a database through a MANIFEST roble wrote from nothing")
	case "compacted":
		fmt.Println("\nVERDICT: Go Pebble reads a table roble compacted out of others")
	case "no-wal":
		fmt.Println("\nVERDICT: Go Pebble reads what roble wrote with the WAL off, as Oxia runs it")
	case "swept":
		fmt.Println("\nVERDICT: Go Pebble reads a database roble has deleted obsolete files from")
	default:
		fmt.Println("\nVERDICT: Go Pebble replays the log roble wrote, key for key")
	}
}

type pair struct {
	key   []byte
	value []byte
}

func readExpectation(path string) ([]pair, [][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	var pairs []pair
	var deleted [][]byte
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		switch {
		case len(fields) == 2 && fields[0] == "deleted":
			k, err := hex.DecodeString(fields[1])
			if err != nil {
				return nil, nil, err
			}
			deleted = append(deleted, k)
		case len(fields) == 2:
			k, err := hex.DecodeString(fields[0])
			if err != nil {
				return nil, nil, err
			}
			v, err := hex.DecodeString(fields[1])
			if err != nil {
				return nil, nil, err
			}
			pairs = append(pairs, pair{k, v})
		default:
			return nil, nil, fmt.Errorf("cannot read %q", scanner.Text())
		}
	}
	return pairs, deleted, scanner.Err()
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "verify-wal: %v\n", err)
	os.Exit(1)
}
