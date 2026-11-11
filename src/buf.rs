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

//! The buffers a [`Disk`](crate::Disk) lends and hands back.
//!
//! A request owns its buffer for as long as the device may still touch it, and
//! returns it with the outcome. That is what makes a disk unable to make roble
//! read memory it still lends, and it is why these are owned values rather
//! than borrowed slices (DESIGN.md §10).
//!
//! They are plain heap buffers for now. §8 wants size-classed frames out of a
//! per-thread pool, shared with the block cache; that changes what backs a
//! buffer, not what a disk sees, so it can arrive without touching the trait.

/// A buffer a disk reads into.
#[derive(Debug)]
pub struct ReadBuf {
    bytes: Vec<u8>,
}

impl ReadBuf {
    /// A buffer of `len` zeroed bytes, to be filled by a read.
    pub fn with_len(len: usize) -> Self {
        ReadBuf {
            bytes: vec![0u8; len],
        }
    }

    /// Wraps bytes a disk handed back.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        ReadBuf { bytes }
    }

    /// The bytes read.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// The bytes, mutably, for a disk to fill.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    /// How many bytes the read was asked for.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Takes the bytes out.
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

/// A buffer a disk writes from.
#[derive(Debug)]
pub struct WriteBuf {
    bytes: Vec<u8>,
}

impl WriteBuf {
    /// A buffer holding `bytes`.
    pub fn new(bytes: Vec<u8>) -> Self {
        WriteBuf { bytes }
    }

    /// The bytes to write.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// How many bytes will be written.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Takes the bytes out.
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

impl From<Vec<u8>> for WriteBuf {
    fn from(bytes: Vec<u8>) -> Self {
        WriteBuf::new(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `is_empty` on both buffers, and the conversion into a write buffer.
    ///
    /// None of the three has a caller. They stay rather than go, which is the
    /// opposite of what #26 and #37 did with uncalled code, and for a reason
    /// that does not apply to those: clippy's `len_without_is_empty` requires
    /// an `is_empty` beside every `len`, so removing them would trade a lint
    /// for a line. Testing one costs less than arguing with either.
    #[test]
    fn an_empty_buffer_says_so() {
        assert!(ReadBuf::from_vec(Vec::new()).is_empty());
        assert!(!ReadBuf::from_vec(vec![1, 2, 3]).is_empty());
        assert_eq!(ReadBuf::from_vec(vec![1, 2, 3]).len(), 3);

        assert!(WriteBuf::new(Vec::new()).is_empty());
        assert!(!WriteBuf::new(vec![1, 2, 3]).is_empty());
        assert_eq!(WriteBuf::new(vec![1, 2, 3]).len(), 3);
    }

    #[test]
    fn a_vec_converts_into_a_write_buffer() {
        let buf: WriteBuf = vec![1u8, 2, 3].into();
        assert_eq!(buf.len(), 3);
        assert!(!buf.is_empty());
    }
}
