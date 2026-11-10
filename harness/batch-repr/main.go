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

// Emits the batch representations Go Pebble builds for fixed sequences of
// operations, so that roble can be required to produce the same bytes.
//
// This matters more than it looks: Oxia checksums the whole repr, header
// included, and compares that checksum across replicas
// (oxiad/dataserver/database/db.go). A Go batch and a roble batch of the same
// operations that differ by one byte give different checksums, and the
// disagreement surfaces as a replica mismatch rather than as a format error.
//
// The checksum is taken *before* Commit, so these reprs are uncommitted ones:
// whatever Pebble leaves in the header's sequence-number field at that point
// is part of what gets checksummed, which is why it is dumped rather than
// assumed.
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type op struct {
	kind  string
	key   string
	value string
	end   string
}

type program struct {
	name string
	ops  []op
}

func programs() []program {
	long := strings.Repeat("v", 300)
	return []program{
		{"empty", nil},
		{"one_set", []op{{kind: "set", key: "/a", value: "1"}}},
		{"one_delete", []op{{kind: "delete", key: "/a"}}},
		{"set_empty_value", []op{{kind: "set", key: "/a", value: ""}}},
		{"empty_key", []op{{kind: "set", key: "", value: "x"}}},
		{"mixed", []op{
			{kind: "set", key: "/users/alice", value: "a"},
			{kind: "delete", key: "/users/bob"},
			{kind: "set", key: "/users/carol", value: long},
			{kind: "delete-range", key: "/sessions/", end: "/sessions0"},
			{kind: "single-delete", key: "/tmp/x"},
		}},
		// A key and a value whose lengths cross the one-byte varint boundary,
		// which is where a length prefix written as a fixed byte would pass the
		// small cases and fail here.
		{"varint_boundary", []op{
			{kind: "set", key: strings.Repeat("k", 127), value: strings.Repeat("v", 128)},
			{kind: "set", key: strings.Repeat("K", 128), value: strings.Repeat("V", 127)},
			{kind: "set", key: strings.Repeat("q", 300), value: strings.Repeat("w", 16384)},
		}},
		// High bytes everywhere, in case anything treats keys as text.
		{"high_bytes", []op{
			{kind: "set", key: "\xff\xfe\x00\x80", value: "\x00\xff\x7f\x80"},
			{kind: "delete-range", key: "\x00", end: "\xff\xff"},
		}},
		{"many", nil},
	}
}

func fill(p *program) {
	if p.name != "many" {
		return
	}
	for i := 0; i < 200; i++ {
		p.ops = append(p.ops, op{
			kind:  "set",
			key:   fmt.Sprintf("/oxia/default/shard-0001/users/%08d", i),
			value: fmt.Sprintf("{\"version\":%d}", i),
		})
	}
}

func build(db *pebble.DB, p program) (*pebble.Batch, error) {
	b := db.NewBatch()
	for _, o := range p.ops {
		var err error
		switch o.kind {
		case "set":
			err = b.Set([]byte(o.key), []byte(o.value), nil)
		case "delete":
			err = b.Delete([]byte(o.key), nil)
		case "single-delete":
			err = b.SingleDelete([]byte(o.key), nil)
		case "delete-range":
			err = b.DeleteRange([]byte(o.key), []byte(o.end), nil)
		default:
			err = fmt.Errorf("unknown op %q", o.kind)
		}
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: batch-repr <output-file>")
		os.Exit(2)
	}
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem(), DisableWAL: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var out strings.Builder
	out.WriteString("# Batch representations built by Go Pebble v2.1.7, uncommitted.\n")
	out.WriteString("# Regenerate with: cd harness && go run ./batch-repr ../format/tests/golden/batches.txt\n")

	fmt.Printf("%-18s %6s %8s\n", "program", "count", "bytes")
	for _, p := range programs() {
		fill(&p)
		b, err := build(db, p)
		if err != nil {
			panic(fmt.Sprintf("%s: %v", p.name, err))
		}
		repr := b.Repr()
		fmt.Fprintf(&out, "batch %s count=%d len=%d\n", p.name, b.Count(), len(repr))
		fmt.Fprintf(&out, "repr %s\n", hex.EncodeToString(repr))
		fmt.Printf("%-18s %6d %8d\n", p.name, b.Count(), len(repr))
		if err := b.Close(); err != nil {
			panic(err)
		}
	}
	if err := os.WriteFile(os.Args[1], []byte(out.String()), 0o644); err != nil {
		panic(err)
	}
}
