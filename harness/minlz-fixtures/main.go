// Generates fixtures compressed with github.com/minio/minlz at the exact
// version and API Pebble v2.1.7 uses: minlz.Encode(dst, src, level), the block
// format, at LevelFastest and LevelBalanced.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/minio/minlz"
)

type fixture struct {
	name string
	data []byte
}

// Each fixture gets its own seed, so that two pseudorandom payloads are two
// payloads rather than the same bytes twice.
func rng(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

// indexBlockLike imitates a Pebble index block: separator keys that share long
// prefixes, each followed by a varint-encoded block handle.
func indexBlockLike(n int) []byte {
	out := make([]byte, 0, n)
	var tmp [16]byte
	off := uint64(0)
	for i := 0; len(out) < n; i++ {
		key := fmt.Sprintf("/oxia/namespace-default/shard-00%02d/users/%08d", i%7, i*37)
		out = append(out, key...)
		m := binary.PutUvarint(tmp[:], off)
		out = append(out, tmp[:m]...)
		m = binary.PutUvarint(tmp[:], 16384)
		out = append(out, tmp[:m]...)
		off += 16389
	}
	return out[:n]
}

// bloomLike imitates a filter block: pseudorandom bits, near-incompressible.
func bloomLike(n int, seed int64) []byte {
	b := make([]byte, n)
	rng(seed).Read(b)
	return b
}

// recordsLike imitates a data block of small protobuf records with shared
// structure, which is what Oxia stores.
func recordsLike(n int, seed int64) []byte {
	r := rng(seed)
	out := make([]byte, 0, n)
	for len(out) < n {
		rec := make([]byte, 0, 320)
		rec = append(rec, 0x0a, 0x20)
		for i := 0; i < 32; i++ {
			rec = append(rec, byte('a'+r.Intn(26)))
		}
		rec = append(rec, 0x12, 0x08)
		var ts [8]byte
		binary.LittleEndian.PutUint64(ts[:], uint64(r.Int63()))
		rec = append(rec, ts[:]...)
		rec = append(rec, 0x1a, 0x40)
		for i := 0; i < 64; i++ {
			rec = append(rec, byte(r.Intn(16)))
		}
		out = append(out, rec...)
	}
	return out[:n]
}

func repeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func compressible(n int) []byte {
	pat := []byte("oxia/shard/users/alice/session/00000000-0000-0000-0000-000000000000;")
	out := make([]byte, 0, n)
	for len(out) < n {
		out = append(out, pat...)
	}
	return out[:n]
}

func main() {
	dir := os.Args[1]
	fixtures := []fixture{
		{"empty", []byte{}},
		{"one_byte", []byte{0x7f}},
		{"tiny_hello", []byte("hello")},
		{"repetitive_16k", repeat('a', 16<<10)},
		{"index_block_like_16k", indexBlockLike(16 << 10)},
		{"bloom_like_16k", bloomLike(16<<10, 1)},
		{"random_16k", bloomLike(16<<10, 2)},
		{"repetitive_64k", repeat(0x5a, 64<<10)},
		{"records_64k", recordsLike(64<<10, 3)},
		{"random_64k", bloomLike(64<<10, 4)},
		{"compressible_1m", compressible(1 << 20)},
		{"max_block_size", compressible(minlz.MaxBlockSize)},
	}

	levels := map[string]int{"fastest": minlz.LevelFastest, "balanced": minlz.LevelBalanced}

	fmt.Printf("minlz.MaxBlockSize = %d\n", minlz.MaxBlockSize)
	fmt.Printf("LevelFastest = %d, LevelBalanced = %d\n\n", minlz.LevelFastest, minlz.LevelBalanced)
	fmt.Printf("%-22s %9s %9s %9s  %s\n", "fixture", "raw", "fastest", "balanced", "sha256(raw)[:16]")

	for _, f := range fixtures {
		sum := sha256.Sum256(f.data)
		if err := os.WriteFile(filepath.Join(dir, f.name+".raw"), f.data, 0o644); err != nil {
			panic(err)
		}
		sizes := map[string]int{}
		for lname, lvl := range levels {
			enc, err := minlz.Encode(nil, f.data, lvl)
			if err != nil {
				panic(fmt.Sprintf("%s/%s: encode: %v", f.name, lname, err))
			}
			// Harness self-check: Go must decode its own output.
			dl, err := minlz.DecodedLen(enc)
			if err != nil {
				panic(fmt.Sprintf("%s/%s: DecodedLen: %v", f.name, lname, err))
			}
			if dl != len(f.data) {
				panic(fmt.Sprintf("%s/%s: DecodedLen %d != %d", f.name, lname, dl, len(f.data)))
			}
			back, err := minlz.Decode(nil, enc)
			if err != nil {
				panic(fmt.Sprintf("%s/%s: self-decode: %v", f.name, lname, err))
			}
			if string(back) != string(f.data) {
				panic(fmt.Sprintf("%s/%s: self-decode mismatch", f.name, lname))
			}
			if err := os.WriteFile(filepath.Join(dir, f.name+"."+lname+".minlz"), enc, 0o644); err != nil {
				panic(err)
			}
			sizes[lname] = len(enc)
		}
		fmt.Printf("%-22s %9d %9d %9d  %s\n", f.name, len(f.data),
			sizes["fastest"], sizes["balanced"], hex.EncodeToString(sum[:8]))
	}
	fmt.Println("\nall fixtures round-tripped through Go")
}
