// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

namespace detail {

bool validate_ascii_fast(const char* src, size_t len);

// unpacks 8->7 encoded blob back to ascii.
// generally, we can not unpack inplace because ascii (dest) buffer is 8/7 bigger than
// the source buffer.
// however, if binary data is positioned on the right of the ascii buffer with empty space on the
// left than we can unpack inplace.
void ascii_unpack(const uint8_t* bin, size_t ascii_len, char* ascii);
void ascii_unpack_simd(const uint8_t* bin, size_t ascii_len, char* ascii);

// Access a single byte in a 7-bit ASCII-packed string without unpacking the entire buffer.
// These helpers read/write the ASCII byte at logical position `idx` in the unpacked string
// directly from/into the packed `bin` representation.
// It's up to caller to verify:
// `1. idx` must be less than `ascii_len` to avoid out-of-bounds access.
// 2. `ascii` must be less than 128 (7-bit ASCII) for packing.
uint8_t ascii_unpack_byte(const uint8_t* bin, size_t ascii_len, size_t idx);
void ascii_pack_byte(uint8_t* bin, size_t ascii_len, size_t idx, uint8_t ascii);

// packs ascii string (does not verify) into binary form saving 1 bit per byte on average (12.5%).
void ascii_pack(const char* ascii, size_t len, uint8_t* bin);
void ascii_pack2(const char* ascii, size_t len, uint8_t* bin);

// SIMD implementation 1 of ascii_pack.
void ascii_pack_simd(const char* ascii, size_t len, uint8_t* bin);

// SIMD implementation 2 of ascii_pack.
void ascii_pack_simd2(const char* ascii, size_t len, uint8_t* bin);

bool compare_packed(const uint8_t* packed, const char* ascii, size_t ascii_len);

// Chunked variant of ascii_unpack: decodes `count` decoded bytes starting at
// decoded offset `dec_offset` from the encoded `src` buffer into `dest`.
// `dec_offset` must be a multiple of 8 (i.e. chunks must start on a packed
// group boundary). `count` may be any value: callers typically use multiples
// of 8 for intermediate chunks and the remaining (possibly non-aligned) byte
// count for the final chunk that covers the unpacked tail.
inline void ascii_unpack_chunk(const uint8_t* src, size_t dec_offset, size_t count, char* dest) {
  ascii_unpack(src + (dec_offset / 8) * 7, count, dest);
}

// Chunk alignment for ascii_unpack_chunk: chunks must start on an 8-decoded-
// byte boundary because each group of 8 decoded bytes maps to a packed group
// of 7 encoded bytes.
inline constexpr size_t kAsciiChunkAlignment = 8;

// maps ascii len to 7-bit packed length. Each 8 bytes are converted to 7 bytes.
inline constexpr size_t binpacked_len(size_t ascii_len) {
  return (ascii_len * 7 + 7) / 8; /* rounded up */
}

// converts 7-bit packed length back to ascii length. Note that this conversion
// is not accurate since it maps 7 bytes to 8 bytes (rounds up), while we may have
// 7 byte strings converted to 7 byte as well.
inline constexpr size_t ascii_len(size_t bin_len) {
  return (bin_len * 8) / 7;
}

}  // namespace detail
}  // namespace dfly
