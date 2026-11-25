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
// the model's answers, and at every walk move a bounded iterator as the
// script says and require the model's answer after every move, and do both
// through the snapshots the script takes, against the model as it was when
// each was taken, and through the indexed batch it opens, against the model
// with the batch's writes laid over it; an iterator held through the batch
// reads it as it was when the iterator was made, while the batch takes more
// writes. A turn run.sh kills syncs its writes, unless the WAL is off,
// and sends itself SIGKILL a given number of microseconds after it begins a
// given operation, or the open; every turn prints a line after each
// operation, and the turn after a killed
// one decides the writes it left in doubt (compat/differential says how).
// Pebble runs as Oxia configures it — the format major
// version of the pin, a bloom filter at ten bits a key, L0 uncompressed and
// Snappy below — with memtables and tables small enough that flushes and
// compactions keep happening, and its own automatic compactions on, so that
// what roble opens next is laid out the way Pebble chose. A segment that
// starts "segment wal-off" runs with DisableWAL, as Oxia runs Pebble, and
// flushes before each checkpoint and before closing a checkpoint it wrote
// to, as Oxia flushes before it takes one and before it closes. Pebble's
// own level check runs on every version it installs, its open's included,
// so what roble left is checked as it is opened.
//
//	differential <dir> <script> <segment> [kill <op|open> <micros>]
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

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

// segments reads a script's segments, and whether each one's turn writes
// with the WAL off.
func segments(script string) ([][]op, []bool, error) {
	var out [][]op
	var walOff []bool
	for _, line := range strings.Split(script, "\n") {
		w := strings.Fields(line)
		if len(w) == 0 {
			continue
		}
		if w[0] == "segment" {
			out = append(out, nil)
			walOff = append(walOff, len(w) == 2 && w[1] == "wal-off")
			continue
		}
		if len(out) == 0 {
			return nil, nil, errors.New("an operation before the first segment")
		}
		out[len(out)-1] = append(out[len(out)-1], op{w[0], w[1:]})
	}
	return out, walOff, nil
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		fail(fmt.Errorf("not a number: %q", s))
	}
	return n
}

// scriptKey is a key of a script line: a key of the alphabet, or, ending
// in "+", the key with a zero byte appended.
func scriptKey(w string) []byte {
	if k, ok := strings.CutSuffix(w, "+"); ok {
		return append([]byte(k), 0)
	}
	return []byte(w)
}

// walkWant is where a walk's iterator should be after a move, by the
// model: want holds the model's keys in [lower, upper), sorted, at is the
// key the iterator is on before the move if valid says it is on one, and
// back says whether its last move went backwards. A step from nothing turns
// round: off the end, or made and not yet moved, a prev lands on the last
// key, and off the start a next lands on the first, while a step the way the
// iterator was going stays on nothing.
func walkWant(want []string, mv, at string, valid, back bool) (string, bool) {
	// The first index at or after k, and the last before it.
	ge := func(k string) (string, bool) {
		i := sort.SearchStrings(want, k)
		if i < len(want) {
			return want[i], true
		}
		return "", false
	}
	lt := func(k string) (string, bool) {
		i := sort.SearchStrings(want, k)
		if i > 0 {
			return want[i-1], true
		}
		return "", false
	}
	last := func() (string, bool) {
		if len(want) == 0 {
			return "", false
		}
		return want[len(want)-1], true
	}
	switch {
	case mv == "f":
		return ge("")
	case mv == "l":
		return last()
	case mv == "n" && !valid:
		if back {
			return ge("")
		}
		return "", false
	case mv == "p" && !valid:
		if !back {
			return last()
		}
		return "", false
	case mv == "n":
		return ge(at + "\x00")
	case mv == "p":
		return lt(at)
	case strings.HasPrefix(mv, "g:"):
		return ge(string(scriptKey(mv[2:])))
	case strings.HasPrefix(mv, "t:"):
		return lt(string(scriptKey(mv[2:])))
	}
	fail(fmt.Errorf("cannot read the move %q", mv))
	return "", false
}

// walk moves an iterator over [lower, upper) as the script says and
// requires the model's answer after every move, a step from nothing
// included (walkWant).
func walk(db pebble.Reader, model map[string][]byte, args []string, at string) {
	w := newWalker(db, model, args[0], args[1], at)
	defer w.iter.Close()
	w.walk(args[2:], at)
}

// walker is an iterator over a range, the model it is to agree with, and
// the moves made with it so far: a walk's, or a held iterator's, which keeps
// its position and its model from one op to the next while the store
// changes under it.
type walker struct {
	iter         *pebble.Iterator
	model        map[string][]byte
	want         []string
	lower, upper string
	valid        bool
	// Whether the last move went backwards: l, p or t. A new iterator's has
	// not, as Pebble's starts out going forward (iterPosCurForward).
	back bool
	done []string
}

func newWalker(db pebble.Reader, model map[string][]byte, lower, upper, at string) *walker {
	var opts pebble.IterOptions
	if lower != "-" {
		opts.LowerBound = scriptKey(lower)
	}
	if upper != "-" {
		opts.UpperBound = scriptKey(upper)
	}
	w := &walker{model: model, lower: lower, upper: upper}
	for _, k := range sorted(model) {
		if (opts.LowerBound == nil || k >= string(opts.LowerBound)) &&
			(opts.UpperBound == nil || k < string(opts.UpperBound)) {
			w.want = append(w.want, k)
		}
	}
	iter, err := db.NewIter(&opts)
	if err != nil {
		fail(fmt.Errorf("%s: walk: %w", at, err))
	}
	w.iter = iter
	return w
}

func (w *walker) walk(moves []string, at string) {
	iter := w.iter
	for _, mv := range moves {
		before := ""
		if w.valid {
			before = string(iter.Key())
		}
		wantKey, wantOK := walkWant(w.want, mv, before, w.valid, w.back)
		switch {
		case mv == "f":
			w.valid = iter.First()
		case mv == "l":
			w.valid = iter.Last()
		case mv == "n":
			w.valid = iter.Next()
		case mv == "p":
			w.valid = iter.Prev()
		case strings.HasPrefix(mv, "g:"):
			w.valid = iter.SeekGE(scriptKey(mv[2:]))
		case strings.HasPrefix(mv, "t:"):
			w.valid = iter.SeekLT(scriptKey(mv[2:]))
		}
		w.back = mv == "l" || mv == "p" || strings.HasPrefix(mv, "t:")
		w.done = append(w.done, mv)
		if err := iter.Error(); err != nil {
			fail(fmt.Errorf("%s: walk %s: %w", at, strings.Join(w.done, " "), err))
		}
		got, gotV := "", []byte(nil)
		if w.valid {
			got, gotV = string(iter.Key()), iter.Value()
		}
		if w.valid != wantOK || got != wantKey || (w.valid && !bytes.Equal(gotV, w.model[got])) {
			fail(fmt.Errorf("%s: walk over [%s, %s) after %s: Pebble is on %s, the history %s",
				at, w.lower, w.upper, strings.Join(w.done, " "),
				show(got, gotV, w.valid), show(wantKey, w.model[wantKey], wantOK)))
		}
	}
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

// history is the model of what is committed, and the writes of a batch
// that is open.
type history struct {
	model map[string][]byte
	batch []op
	// maybes are the writes the killed turn before this one left in doubt,
	// in order, not yet decided: the next turn finds how many landed.
	maybes []maybe
	// checkpoints holds each checkpoint taken, by id, with the segment it
	// was taken in and the model as it stood: what is committed, without an
	// open batch's writes. segment is the segment of the ops being applied.
	checkpoints map[string]checkpoint
	segment     int
}

type checkpoint struct {
	segment int
	model   map[string][]byte
}

// maybe is a write in doubt, with the batch that was open at it.
type maybe struct {
	write op
	batch []op
}

func (h *history) apply(o op) {
	switch o.name {
	case "batch", "abort":
		h.batch = nil
	case "bput", "bdel", "bdelrange":
		h.batch = append(h.batch, op{o.name[1:], o.args})
	case "commit":
		for _, w := range h.batch {
			apply(h.model, w)
		}
		h.batch = nil
	case "maybe":
		w := op{o.args[0], o.args[1:]}
		h.maybes = append(h.maybes, maybe{w, slices.Clone(h.batch)})
		// A commit ends its batch, whether or not it landed.
		if w.name == "commit" {
			h.batch = nil
		}
	case "checkpoint":
		h.checkpoints[o.args[0]] = checkpoint{h.segment, maps.Clone(h.model)}
	case "cpcheck":
		if cp, ok := h.checkpoints[o.args[0]]; ok {
			for _, p := range o.args[1:] {
				k, step, len := cpput(p)
				cp.model[k] = value(step, len)
			}
		}
	default:
		apply(h.model, o)
	}
}

// throughBatch is the model as a read through the open batch sees it: the
// batch's writes over what is committed.
func (h *history) throughBatch() map[string][]byte {
	out := maps.Clone(h.model)
	for _, w := range h.batch {
		apply(out, w)
	}
	return out
}

// decide settles the maybes the killed turn before this one left: the write
// it had under way and, with the WAL off, every one it made after the last
// flush or checkpoint it finished. What survived the kill is some first ones
// of them, in order, since a flush writes a memtable out whole and the older
// ones first. It reads Pebble whole, finds how many of them make what Pebble
// holds, and prints that for run.sh to resolve into the script. When several
// counts do, since one write can undo another, the most is taken: each
// leaves the same map.
func decide(db *pebble.DB, h *history, segment int) {
	if len(h.maybes) == 0 {
		return
	}
	at := fmt.Sprintf("segment %d: deciding what the killed turn left", segment)
	held := contents(db, at)
	model := maps.Clone(h.model)
	landed := -1
	// The count whose map differs from Pebble's at the fewest keys, for the
	// message if none matches.
	closest, closestN := -1, 0
	var closestModel map[string][]byte
	for n := 0; n <= len(h.maybes); n++ {
		if n > 0 {
			m := h.maybes[n-1]
			then := history{model: model, batch: m.batch}
			then.apply(m.write)
		}
		if d := len(differing(model, held)); d == 0 {
			landed = n
		} else if closest < 0 || d < closest {
			closest, closestN, closestModel = d, n, maps.Clone(model)
		}
	}
	if landed < 0 {
		k := differing(closestModel, held)[0]
		v, in := held[k]
		w, inW := closestModel[k]
		fail(fmt.Errorf("%s: Pebble holds none of the %d maps its %d writes in doubt could have left; "+
			"the closest, with the first %d landed, differs from it at %d keys, first at %s: Pebble holds %s, the history %s",
			at, len(h.maybes)+1, len(h.maybes), closestN, closest, k, show(k, v, in), show(k, w, inW)))
	}
	// In place, since the caller holds this map.
	clear(h.model)
	maps.Copy(h.model, held)
	h.maybes = nil
	fmt.Printf("resolved %d\n", landed)
}

// contents is everything Pebble holds, read with one forward scan.
func contents(db *pebble.DB, at string) map[string][]byte {
	iter, err := db.NewIter(nil)
	if err != nil {
		fail(fmt.Errorf("%s: iterator: %w", at, err))
	}
	defer iter.Close()
	out := map[string][]byte{}
	for ok := iter.First(); ok; ok = iter.Next() {
		out[string(iter.Key())] = slices.Clone(iter.Value())
	}
	if err := iter.Error(); err != nil {
		fail(fmt.Errorf("%s: %w", at, err))
	}
	return out
}

// differing is the keys two maps disagree on, sorted.
func differing(a, b map[string][]byte) []string {
	var out []string
	for k, v := range a {
		if w, in := b[k]; !in || !bytes.Equal(v, w) {
			out = append(out, k)
		}
	}
	for k := range b {
		if _, in := a[k]; !in {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
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

func check(db pebble.Reader, model map[string][]byte, at string) {
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
	if len(os.Args) != 4 && (len(os.Args) != 7 || os.Args[4] != "kill") {
		fmt.Fprintln(os.Stderr, "usage: differential <dir> <script> <segment> [kill <op|open> <micros>]")
		os.Exit(2)
	}
	dir, segment := os.Args[1], atoi(os.Args[3])
	text, err := os.ReadFile(os.Args[2])
	if err != nil {
		fail(err)
	}
	all, walOffs, err := segments(string(text))
	if err != nil {
		fail(err)
	}
	walOff := walOffs[segment]
	// A turn that is to be killed syncs: Pebble's unsynced writes wait in its
	// own buffers, which a kill loses, where roble's are already the OS's.
	// With the WAL off there is nothing to sync, and Pebble refuses a synced
	// write: a write lasts once a flush has written it out.
	wo := pebble.NoSync
	killAt, killAfter := -2, time.Duration(0)
	if len(os.Args) == 7 {
		if !walOff {
			wo = pebble.Sync
		}
		killAt = -1
		if os.Args[5] != "open" {
			killAt = atoi(os.Args[5])
		}
		killAfter = time.Duration(atoi(os.Args[6])) * time.Microsecond
	}
	arm := func(at int) {
		if at == killAt {
			time.AfterFunc(killAfter, func() { syscall.Kill(os.Getpid(), syscall.SIGKILL) })
		}
	}
	hist := history{model: map[string][]byte{}, checkpoints: map[string]checkpoint{}}
	for i, s := range all[:segment] {
		hist.segment = i
		for _, o := range s {
			hist.apply(o)
		}
	}
	hist.segment = segment
	// The same map throughout: a commit applies to it in place.
	model := hist.model

	opts := &pebble.Options{
		FS:                 vfs.Default,
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		MemTableSize:       64 << 10,
		LBaseMaxBytes:      64 << 10,
		DisableWAL:         walOff,
		// CheckLevels on every version Pebble installs: its open's, over
		// what roble left and the logs it replays, and every flush's and
		// compaction's after it. A fault ends the turn with it.
		DebugCheck: pebble.DebugCheckLevels,
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
	// Writes are not synced unless the turn may be killed: what the other
	// engine opens next is what this one left, through the page cache, and a
	// machine crash is not the subject (SimFs is).
	arm(-1)
	db, err := pebble.Open(dir, opts)
	if err != nil {
		fail(fmt.Errorf("segment %d: Pebble could not open what roble left: %w", segment, err))
	}
	// A kill before this line was a kill inside the open.
	fmt.Println("opened")
	decide(db, &hist, segment)

	// The live snapshots, each with the model as it was when it was taken.
	type held struct {
		snap  *pebble.Snapshot
		model map[string][]byte
	}
	var snaps []held
	// The held iterators, each with the model as it was when it was made.
	var iters []*walker
	var batch *pebble.Batch
	for i, o := range all[segment] {
		at := fmt.Sprintf("segment %d (pebble), operation %d", segment, i)
		arm(i)
		switch o.name {
		case "put":
			err = db.Set([]byte(o.args[0]), value(atoi(o.args[1]), atoi(o.args[2])), wo)
		case "del":
			err = db.Delete([]byte(o.args[0]), wo)
		case "delrange":
			err = db.DeleteRange([]byte(o.args[0]), []byte(o.args[1]), wo)
		case "flush":
			err = db.Flush()
		case "compact":
			err = db.Compact(context.Background(), []byte("a"), []byte("z"), false)
		case "check":
			check(db, model, at)
		case "walk":
			walk(db, model, o.args, at)
		case "iter":
			iters = append(iters, newWalker(db, maps.Clone(model), o.args[0], o.args[1], at))
		case "itermove":
			n := atoi(o.args[0])
			iters[n].walk(o.args[1:], fmt.Sprintf("%s, held iterator %d", at, n))
		case "uniter":
			n := atoi(o.args[0])
			err = iters[n].iter.Close()
			iters = append(iters[:n], iters[n+1:]...)
		case "snap":
			snaps = append(snaps, held{db.NewSnapshot(), maps.Clone(model)})
		case "snapcheck":
			h := snaps[atoi(o.args[0])]
			check(h.snap, h.model, fmt.Sprintf("%s, snapshot %s", at, o.args[0]))
		case "snapwalk":
			h := snaps[atoi(o.args[0])]
			walk(h.snap, h.model, o.args[1:], fmt.Sprintf("%s, snapshot %s", at, o.args[0]))
		case "unsnap":
			n := atoi(o.args[0])
			err = snaps[n].snap.Close()
			snaps = append(snaps[:n], snaps[n+1:]...)
		case "batch":
			batch = db.NewIndexedBatch()
		case "bput":
			err = batch.Set([]byte(o.args[0]), value(atoi(o.args[1]), atoi(o.args[2])), nil)
		case "bdel":
			err = batch.Delete([]byte(o.args[0]), nil)
		case "bdelrange":
			err = batch.DeleteRange([]byte(o.args[0]), []byte(o.args[1]), nil)
		case "bcheck":
			check(batch, hist.throughBatch(), at+", through the batch")
		case "bwalk":
			walk(batch, hist.throughBatch(), o.args, at+", through the batch")
		case "biter":
			// Held while the batch takes more writes, and not to see them:
			// nothing here calls SetOptions, which would refresh its view
			// of the batch.
			iters = append(iters, newWalker(batch, hist.throughBatch(), o.args[0], o.args[1], at+", through the batch"))
		case "commit":
			if err = batch.Commit(wo); err == nil {
				err = batch.Close()
			}
			batch = nil
		case "abort":
			if batch != nil {
				err = batch.Close()
			}
			batch = nil
		case "checkpoint":
			// With the WAL off the memtable's writes are in nothing a
			// checkpoint copies: flushed first, as Oxia flushes before it
			// takes one.
			if walOff {
				err = db.Flush()
			}
			// The log flushed and synced first: a turn not killed writes
			// without syncing, and a checkpoint taken without this may miss
			// them.
			if err == nil {
				err = db.Checkpoint(checkpointDir(dir, o.args[0]), pebble.WithFlushedWAL())
			}
		case "cpcheck":
			err = cpcheck(opts, dir, o.args, hist.checkpoints, wo, walOff, at)
		default:
			err = fmt.Errorf("cannot read %q", o.name)
		}
		if err != nil {
			fail(fmt.Errorf("%s: %w", at, err))
		}
		hist.apply(o)
		// How far a killed turn got: run.sh reads the last of these.
		fmt.Printf("done %d\n", i)
	}
	if batch != nil {
		fail(fmt.Errorf("segment %d: a batch is still open at the end", segment))
	}
	for _, w := range iters {
		if err := w.iter.Close(); err != nil {
			fail(fmt.Errorf("segment %d: iterator close: %w", segment, err))
		}
	}
	for _, h := range snaps {
		if err := h.snap.Close(); err != nil {
			fail(fmt.Errorf("segment %d: snapshot close: %w", segment, err))
		}
	}
	if err := db.Close(); err != nil {
		fail(fmt.Errorf("segment %d: close: %w", segment, err))
	}
}

// checkpointDir is where the checkpoint with this id goes: beside the store.
func checkpointDir(dir, id string) string {
	return dir + "-cp" + id
}

// cpput reads one put of a cpcheck: key:step:len.
func cpput(p string) (string, int, int) {
	f := strings.Split(p, ":")
	if len(f) != 3 {
		fail(fmt.Errorf("cannot read the checkpoint put %q", p))
	}
	return f[0], atoi(f[1]), atoi(f[2])
}

// cpcheck opens the checkpoint args[0] names, checks it against the model
// as it stood when it was taken with the puts of earlier cpchecks on top,
// writes the puts the rest of args names, and closes it.
func cpcheck(opts *pebble.Options, dir string, args []string, taken map[string]checkpoint, wo *pebble.WriteOptions, walOff bool, at string) error {
	id := args[0]
	cp, ok := taken[id]
	if !ok {
		// Not taken: the turn that was to take it was killed first.
		fmt.Fprintf(os.Stderr, "%s: checkpoint %s was cut with the turn that was to take it\n", at, id)
		return nil
	}
	// Not created if missing: an empty store would pass for a checkpoint of
	// an empty map.
	opts = opts.Clone()
	opts.ErrorIfNotExists = true
	db, err := pebble.Open(checkpointDir(dir, id), opts)
	if err != nil {
		return fmt.Errorf("Pebble could not open checkpoint %s, taken in segment %d: %w", id, cp.segment, err)
	}
	check(db, cp.model, fmt.Sprintf("%s, checkpoint %s", at, id))
	for _, p := range args[1:] {
		k, step, len := cpput(p)
		if err := db.Set([]byte(k), value(step, len), wo); err != nil {
			db.Close()
			return fmt.Errorf("checkpoint %s: %w", id, err)
		}
	}
	// With the WAL off its puts are in its memtable alone, which Close does
	// not write out: flushed first, as Oxia closes.
	if walOff {
		if err := db.Flush(); err != nil {
			db.Close()
			return fmt.Errorf("checkpoint %s: flush: %w", id, err)
		}
	}
	return db.Close()
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "differential (pebble): %v\n", err)
	os.Exit(1)
}
