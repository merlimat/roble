// The Go side of roble's compatibility checks. Dependencies are pinned to the
// versions Pebble v2.1.7 requires, since that is roble's compatibility target
// (DESIGN.md §3) — minio/minlz here is the version in pebble/v2@v2.1.7's go.mod.
module roble/harness

go 1.24

require github.com/minio/minlz v1.0.1-0.20250507153514-87eb42fe8882

require github.com/klauspost/compress v1.17.11 // indirect
