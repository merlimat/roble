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
// Runs one segment of a differential script with Go Pebble, on a directory
// roble and Pebble take turns at (compat/differential).
//
// The other half of `differential run`, and the same rules: replay the
// segments before this one into a model, apply this one to the store and the
// model together, and at every check read the store every way and require
// the model's answers. Pebble runs as Oxia configures it — the format major
// version of the pin, a bloom filter at ten bits a key, L0 uncompressed and
// Snappy below — with memtables and tables small enough that flushes and
// compactions keep happening, and its own automatic compactions on, so that
// what roble opens next is laid out the way Pebble chose.
//
//	differential <dir> <script> <segment>
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// keys is the size of the script's alphabet, as compat/differential has it.
const keys = 1000

func key(n int) string { return fmt.Sprintf("k%04d", n) }

// value is a put's value: its step's tag, then bytes drawn from the step,
// to length bytes, as compat/differential builds it.
func value(step, length int) []byte {
	out := []byte(fmt.Sprintf("v%d.", step))
	x := uint64(step)*0x9e3779b97f4a7c15 | 1
	for len(out) < length {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		out = append(out, 'a'+byte(x%26))
	}
	return out[:length]
}

type op struct {
	name string
	args []string
}

func segments(script string) ([][]op, error) {
	var out [][]op
	for _, line := range strings.Split(script, "\n") {
		w := strings.Fields(line)
		if len(w) == 0 {
			continue
		}
		if w[0] == "segment" {
			out = append(out, nil)
			continue
		}
		if len(out) == 0 {
			return nil, errors.New("an operation before the first segment")
		}
		out[len(out)-1] = append(out[len(out)-1], op{w[0], w[1:]})
	}
	return out, nil
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		fail(fmt.Errorf("not a number: %q", s))
	}
	return n
}

func apply(model map[string][]byte, o op) {
	switch o.name {
	case "put":
		model[o.args[0]] = value(atoi(o.args[1]), atoi(o.args[2]))
	case "del":
		delete(model, o.args[0])
	case "delrange":
		for k := range model {
			if o.args[0] <= k && k < o.args[1] {
				delete(model, k)
			}
		}
	}
}

func sorted(model map[string][]byte) []string {
	out := make([]string, 0, len(model))
	for k := range model {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func show(k string, v []byte, ok bool) string {
	if !ok {
		return "nothing"
	}
	head := v
	if len(head) > 12 {
		head = head[:12]
	}
	return fmt.Sprintf("%s = %d bytes starting %q", k, len(v), head)
}

func check(db *pebble.DB, model map[string][]byte, at string) {
	want := sorted(model)
	iter, err := db.NewIter(nil)
	if err != nil {
		fail(fmt.Errorf("%s: iterator: %w", at, err))
	}
	defer iter.Close()

	var forward []string
	for ok := iter.First(); ok; ok = iter.Next() {
		k := string(iter.Key())
		if v, in := model[k]; !in || !bytes.Equal(v, iter.Value()) {
			fail(fmt.Errorf("%s: a forward scan found %s, the history %s",
				at, show(k, iter.Value(), true), show(k, v, in)))
		}
		forward = append(forward, k)
	}
	if len(forward) != len(want) {
		fail(fmt.Errorf("%s: a forward scan found %d keys, the history says %d", at, len(forward), len(want)))
	}
	n := len(want)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		n--
		if n < 0 || string(iter.Key()) != want[n] {
			fail(fmt.Errorf("%s: a reverse scan disagrees with the history at %q", at, iter.Key()))
		}
	}
	if n != 0 {
		fail(fmt.Errorf("%s: a reverse scan stopped %d keys short", at, n))
	}

	probes := []string{"a", "z"}
	for i := 0; i < keys; i++ {
		probes = append(probes, key(i)+"\x00", key(i))
	}
	for _, p := range probes {
		// Equal.
		got, closer, err := db.Get([]byte(p))
		v, in := model[p]
		switch {
		case errors.Is(err, pebble.ErrNotFound):
			if in {
				fail(fmt.Errorf("%s: Equal of %q: Pebble found nothing, the history %s", at, p, show(p, v, in)))
			}
		case err != nil:
			fail(fmt.Errorf("%s: Equal of %q: %w", at, p, err))
		default:
			if !in || !bytes.Equal(got, v) {
				fail(fmt.Errorf("%s: Equal of %q: Pebble found %s, the history %s",
					at, p, show(p, got, true), show(p, v, in)))
			}
			closer.Close()
		}
		// Ceiling and Floor, as one seek each.
		i := sort.SearchStrings(want, p)
		var wantCeil, wantFloor string
		if i < len(want) {
			wantCeil = want[i]
		}
		j := sort.SearchStrings(want, p+"\x00")
		if j > 0 {
			wantFloor = want[j-1]
		}
		for _, c := range []struct {
			name string
			seek func() bool
			want string
		}{
			{"Ceiling", func() bool { return iter.SeekGE([]byte(p)) }, wantCeil},
			{"Floor", func() bool { return iter.SeekLT([]byte(p + "\x00")) }, wantFloor},
		} {
			// Each seek just before its answer is read: the iterator holds
			// one position.
			got := ""
			if c.seek() {
				got = string(iter.Key())
				if !bytes.Equal(iter.Value(), model[got]) {
					fail(fmt.Errorf("%s: %s of %q: Pebble found %s, the history %s", at, c.name, p,
						show(got, iter.Value(), true), show(got, model[got], true)))
				}
			}
			if got != c.want {
				fail(fmt.Errorf("%s: %s of %q: Pebble found %q, the history %q", at, c.name, p, got, c.want))
			}
		}
	}
	if err := iter.Error(); err != nil {
		fail(fmt.Errorf("%s: %w", at, err))
	}
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: differential <dir> <script> <segment>")
		os.Exit(2)
	}
	dir, segment := os.Args[1], atoi(os.Args[3])
	text, err := os.ReadFile(os.Args[2])
	if err != nil {
		fail(err)
	}
	all, err := segments(string(text))
	if err != nil {
		fail(err)
	}
	model := map[string][]byte{}
	for _, s := range all[:segment] {
		for _, o := range s {
			apply(model, o)
		}
	}

	opts := &pebble.Options{
		FS:                 vfs.Default,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		MemTableSize:       64 << 10,
		LBaseMaxBytes:      64 << 10,
	}
	opts.TargetFileSizes[0] = 16 << 10
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = 4096
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		cs := pebble.UniformDBCompressionSettings(block.SnappyCompression)
		cs.Name = "Oxia"
		cs.Levels[0] = block.NoCompression
		return cs
	})
	// Writes are not synced: what the other engine opens next is what this
	// one closed, through the page cache, and durability is not the subject
	// (SimFs and tests/crash_loop.rs are).
	db, err := pebble.Open(dir, opts)
	if err != nil {
		fail(fmt.Errorf("segment %d: Pebble could not open what roble left: %w", segment, err))
	}

	for i, o := range all[segment] {
		at := fmt.Sprintf("segment %d (pebble), operation %d", segment, i)
		switch o.name {
		case "put":
			err = db.Set([]byte(o.args[0]), value(atoi(o.args[1]), atoi(o.args[2])), pebble.NoSync)
		case "del":
			err = db.Delete([]byte(o.args[0]), pebble.NoSync)
		case "delrange":
			err = db.DeleteRange([]byte(o.args[0]), []byte(o.args[1]), pebble.NoSync)
		case "flush":
			err = db.Flush()
		case "compact":
			err = db.Compact(context.Background(), []byte("a"), []byte("z"), false)
		case "check":
			check(db, model, at)
		default:
			err = fmt.Errorf("cannot read %q", o.name)
		}
		if err != nil {
			fail(fmt.Errorf("%s: %w", at, err))
		}
		apply(model, o)
	}
	if err := db.Close(); err != nil {
		fail(fmt.Errorf("segment %d: close: %w", segment, err))
	}
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "differential (pebble): %v\n", err)
	os.Exit(1)
}
