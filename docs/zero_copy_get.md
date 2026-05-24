# Zero-Copy GET for Large Strings

A `GET` on a large string value in Dragonfly transports the user-visible
bytes from the shard's `CompactObj` storage to the client socket without
materializing the string anywhere in between. The shard does not allocate
a `std::string`; the reply builder does not buffer the full payload; the
encoded representation (if any) is decoded one chunk at a time directly
into the reply builder's existing scratch buffer.

The borrowed pointer survives concurrent mutations of the same key via a
Copy-on-Write mechanism: a writer that races a reader installs a fresh
allocation on the `CompactObj` and leaves the old buffer owned by a
refcount-bearing pin, which is freed on the buffer's owning shard once
the last reader is done with it.

## What's covered

- `LARGE_STR_TAG` values (heap-allocated, > ~256 bytes after the small
  string carve-out).
- Encodings `NONE_ENC`, `ASCII1_ENC`, `ASCII2_ENC`. Huffman-encoded
  values use the materializing path (variable-length codes don't chunk
  trivially).
- All in-memory accesses. `EXTERNAL_TAG` (tiered) values require an
  asynchronous disk fetch and remain outside the zero-copy story.

`GET` is the only command that uses this path. Mutating reads
(`GETDEL`, `GETEX`, `GETSET`) and multi-key reads (`MGET`) keep the
materializing `pv.ToString()` path.

## Threading model

Dragonfly's shared-nothing design pins each shard to a single proactor
thread with a thread-local mimalloc heap. A connection is bound to one
proactor; `GET` for a foreign key hops to the owning shard via
`SingleHopT(cb)`, then resumes on the connection's proactor for reply
construction. The socket write itself is synchronous (`writev`) but may
yield the fiber while waiting on the kernel — other commands on the
same shard can run during that window. Cross-thread free is supported
by mimalloc but slow; the design routes all deallocations back to the
buffer's owning shard.

## Building blocks

### `detail::LargeString`

The 16-byte storage for non-inline raw strings in `CompactObj`:

```cpp
struct LargeString {
  void* ptr;                   // mimalloc allocation on owning shard
  uint64_t sz : 56;            // current length in bytes
  uint64_t read_pending : 1;   // at least one outstanding read pin
  uint64_t reserved : 7;
};
```

`read_pending` is set when one or more readers hold a borrowed view
into `ptr`. While the bit is set, `LargeString::SetString` and
`LargeString::Free` do not deallocate `ptr` directly; they invoke a
thread-local orphan callback that hands ownership of the buffer over to
the per-shard pending-read registry.

`LargeString::DefragIfNeeded` returns false (no defrag) while
`read_pending` is set — a reallocation would invalidate outstanding
borrowed views.

`LargeString::AppendString` mutates in place and would corrupt pinned
readers; it `DCHECK`s `!IsReadPending()`. The only caller is
`rdb_load`, which never produces values that have been pinned.

### `CompactObj::TryGetRaw`

Returns a borrowed view of the underlying `LargeString` along with the
metadata needed to decode it:

```cpp
struct RawBorrow {
  std::string_view encoded;   // bytes as stored
  size_t decoded_size;        // user-visible byte count
  uint8_t encoding;           // NONE / ASCII1 / ASCII2
};
std::optional<RawBorrow> CompactObj::TryGetRaw() const;
```

For `NONE_ENC` the view is the user-visible bytes
(`encoded.size() == decoded_size`). For `ASCII1`/`ASCII2` the view is
the packed source and `decoded_size` is computed from the packed
length and the first byte via the existing `StrEncoding::DecodedSize`.

`CompactObj::MarkReadPending() const` stamps the bit on the underlying
`LargeString` (via a controlled `const_cast` — the bit is
bookkeeping metadata, not part of the logical value).

### `PendingRead` and the per-shard registry

```cpp
struct PendingRead {
  void* ptr;                            // buffer being tracked
  std::atomic<uint32_t> refcnt;         // active reader count
  bool orphaned;                        // writer detached from CompactObj
  EngineShard* owner_shard;             // for cross-thread routing
  std::atomic<PendingRead*> mpsc_next;  // intrusive next for MPSC free list
};

class EngineShard {
  // Active pins. Single-threaded — only this shard touches it.
  absl::flat_hash_map<void*, PendingRead*> pending_read_map_;

  // Entries whose refcnt reached zero on a remote thread, awaiting
  // cleanup on this shard. Multi-producer / single-consumer.
  base::MPSCIntrusiveQueue<PendingRead> pending_read_free_list_;
};
```

The map is private to the shard thread. The free list takes entries
from any thread (typically IO threads dropping the last reference at
the end of a socket write) and is drained by the owning shard from
`Heartbeat()` and at `Shutdown`.

Method surface:

- `EngineShard::PinRead(ptr)` — shard thread. Finds or inserts an
  entry, increments `refcnt`, returns `PendingRead*`.
- `EngineShard::UnpinRead(pin)` — any thread, static. Decrements
  `refcnt`; if it transitions to zero, pushes `pin` to its owner
  shard's free list.
- `EngineShard::OrphanLargeStringPtr(ptr)` — shard thread. Marks the
  active entry orphaned and removes it from the map. Returns false if
  no entry was found (in which case the caller deallocates normally).
- `EngineShard::DrainPendingReads()` — shard thread. Pops entries from
  the free list; re-checks `refcnt` with acquire ordering and skips
  re-pinned entries; for orphaned entries frees the buffer on this
  shard's heap; for non-orphaned entries removes the map slot. Deletes
  the entry struct.

### Reply builder integration

`SinkReplyBuilder` is per-connection, lives on the connection's
proactor thread, and accumulates `iovec` entries via `WritePieces`
(copy into scratch) or `WriteRef` (push pointer only). `Send()` does a
synchronous `writev` and returns when the bytes are in the kernel.

The borrow path needs the source bytes to outlive the socket write
(which may suspend the fiber). `SinkReplyBuilder` carries a small list
of opaque post-send pins:

```cpp
absl::InlinedVector<void*, 4> post_send_pins_;
void AddPostSendPin(void* pin);
static void SetPostSendUnpinFn(void (*fn)(void*));
```

After `Send()`'s `writev` returns, each pin is passed to the
process-wide unpin function (installed once at startup to forward to
`EngineShard::UnpinRead`). The reply builder remains free of any
dependency on `server/engine_shard.h` — the only seam is the function
pointer.

`SinkReplyBuilder::WriteDecodedChunks(src, decoded_size, decode_fn,
chunk_alignment)` streams a decoded payload directly into the scratch
buffer. It loops: ensure the scratch has room for at least
`chunk_alignment` bytes (Flush if not); decode the next chunk into the
scratch's append region; extend the previous iovec or push a new one;
advance. When the scratch fills, an intermediate `Flush()` drains it
via `writev` and resets. One chunk's worth of decoded bytes lives in
the scratch at any time; the full decoded payload is never held.

`RedisReplyBuilderBase::SendBulkStringStreamed` wraps the `$N\r\n`
framing around `WriteDecodedChunks` and the trailing `\r\n`.

### Chunked ASCII decode

ASCII packing maps every 8 decoded chars to 7 packed bytes; after the
last full group, up to 7 unpacked bytes are stored verbatim.
`ascii_unpack(bin, count, dest)` handles partial sub-ranges naturally
as long as `count` is a multiple of 8 (or it's the final chunk
covering the unaligned tail). `detail::ascii_unpack_chunk(src,
dec_offset, count, dest)` codifies the encoded-offset math:

```cpp
inline void ascii_unpack_chunk(const uint8_t* src, size_t dec_offset,
                                size_t count, char* dest) {
  ascii_unpack(src + (dec_offset / 8) * 7, count, dest);
}
inline constexpr size_t kAsciiChunkAlignment = 8;
```

`SendBulkStringStreamed` is called with this function and the
8-decoded-byte chunk alignment; intermediate chunks decode the maximum
aligned amount that fits in the (8 KiB) scratch, and the final chunk
picks up any unaligned tail.

### Capture / replay (squashing, MULTI/EXEC)

`MultiCommandSquasher` runs commands against a
`CapturingReplyBuilder` that records replies into intermediate
payloads, then replays them to the real sink. Both the borrowed
(NONE_ENC) and streamed (ASCII) paths must survive this boundary
without copying.

`facade::payload::Payload` gains two alternatives:

```cpp
struct BulkStringView { std::string_view view; };

struct BulkStringStreamed {
  const void* src;
  size_t decoded_size;
  StreamingDecodeFn decode_fn;
  size_t chunk_alignment;
};
```

`BulkStringStreamed` is stored via `std::unique_ptr` so the variant
alternative stays at 8 bytes (preserves `sizeof(Payload) == 40`).
`CapturingReplyBuilder::SendBulkStringBorrowed` records the view
directly; `SendBulkStringStreamed` records the descriptor. The
`CaptureVisitor` replay path calls the corresponding method on the
real sink — chunked decode happens at replay time, not at capture
time. The encoded source's lifetime is the same `PendingRead` pin that
the shard registered; both paths share the post-send unpin discipline.

The HTTP API visitor produces a single contiguous JSON-escaped string
and materializes the streamed payload once at that layer; chunked
decoding into JSON output isn't a meaningful win.

## End-to-end path

```cpp
// shard thread, GET callback
if (auto raw = pv.TryGetRaw()) {                  // optional<RawBorrow>
  PendingRead* pin = es->PinRead(raw->encoded.data());
  pv.MarkReadPending();                            // sets read_pending bit
  return BorrowedString{raw->encoded, pin,
                        raw->decoded_size, raw->encoding};
}

// proactor thread, GetReplies::Send(BorrowedString)
if (bs.encoding == 0) {
  rb->SendBulkStringBorrowed(bs.encoded);          // iovec ref (no copy)
} else {
  rb->SendBulkStringStreamed(bs.encoded.data(), bs.decoded_size,
                             ascii_unpack_chunk_thunk,
                             detail::kAsciiChunkAlignment);
}
rb->AddPostSendPin(bs.pin);                        // released after Send()
```

`AddPostSendPin` runs after the bulk-string send call because the
latter may internally `Flush` on `IOV_MAX`, which drains existing
post-send pins. Queuing the new pin afterward ensures it is not
released before the bytes it protects hit the socket.

## Concurrency picture

```
                    Shard A                                  Connection X (proactor)
                    -------                                  -----------------------
 GET k callback
   raw = pv.TryGetRaw()
   pin = PinRead(raw.encoded.data())  // inserts into A's map
   pv.MarkReadPending()
   return BorrowedString{...}
                                  ─── SingleHopT result ──>
                                                            rb->SendBulkStringStreamed(...)
                                                              | streams decode into scratch
                                                              | (or SendBulkStringBorrowed
                                                              |  for NONE_ENC)
                                                            rb->AddPostSendPin(pin)

 (meanwhile, some other connection)
 SET k newval (also on shard A)
   LargeString::SetString sees read_pending=1
     -> orphan callback runs on A
       -> entry.orphaned = true
       -> A's pending_read_map_.erase(old_ptr)
   allocate new buffer, install in CompactObj
   read_pending = 0
                                                            rb->Flush() -> Send() -> sink->Write(vecs)
                                                            // bytes in kernel
                                                            for p in post_send_pins_:
                                                              UnpinRead(p) -> fetch_sub(1)
                                                              == 1 -> push to A's free_list_

 Heartbeat
   DrainPendingReads()
     pop entry, refcnt==0, orphaned
     mi_resource_.deallocate(old_ptr)   // freed on A's heap
     delete entry
```

If no mutation occurs between borrow and unpin, the drain sees the
entry as not-orphaned and just removes the map slot; the `CompactObj`
continues to own the buffer through its normal lifecycle.

## Race conditions and safety

### Drain vs re-pin

After `UnpinRead` pushes an entry whose `refcnt` reached zero, another
reader on the same shard can `PinRead` the same `ptr` and observe the
entry in the map with `refcnt` 0 → 1. When the drain finally pops the
entry, it re-loads `refcnt` with acquire ordering and skips entries
with `refcnt > 0`. The new readers will themselves push to the free
list when they unpin. The map slot continues to point at the same
entry; no double-free or leak.

### Post-drain mutation

The `LargeString::read_pending` bit can outlive the corresponding map
entry — after the last reader unpins and the drain has removed the
non-orphaned entry, the bit remains set on the `CompactObj`'s
`LargeString`. A subsequent mutation invokes the orphan callback,
which fails to find the `ptr` in the map and returns false. The
`LargeString` then deallocates the buffer normally. Correct, because
no active reader holds the pointer at that point.

### Defrag

The defrag task would reallocate a `LargeString` buffer in place and
invalidate any outstanding borrowed views. `DefragIfNeeded` returns
false (no defrag) when `read_pending=1`. Once the pins clear, the
next defrag pass picks up the same value.

### AppendString

In-place mutation is forbidden while a pin is active; `AppendString`
`DCHECK`s the bit. Today's only caller (`rdb_load`) never produces
pinned values.

### Cross-thread free

A pin allocated on shard A but unpinned by a different IO thread
arrives back at A's MPSC free list. The actual `deallocate` runs in
A's `DrainPendingReads` via `CompactObj::memory_resource()->deallocate(...)`
— the same thread-local MR that `LargeString::Free` uses. Because the
drain runs on shard A's thread, the call dispatches to A's
`MiMemoryResource` and hits the buffer's owning mimalloc heap. The
`PendingRead` entry itself is freed with a plain `delete` from the
same thread it was `new`'d on, so its allocation and free both live
on A's heap as well.

### Capture / replay window

Captured payloads (`BulkStringView`, `BulkStringStreamed`) hold raw
pointers into the borrowed source. The source's lifetime is governed
by the same `PendingRead` pin that the originating `CmdGet` queued via
`AddPostSendPin`. The final replay sink's `Send` releases the pin only
after `writev` returns, so captured payloads remain valid through the
entire squashing / `MULTI`-`EXEC` pipeline.

## What's out of scope

- **MGET.** `CollectKeys` today allocates a per-shard storage buffer
  and packs values into it. Zero-copy MGET would carry borrowed views
  per result instead.
- **Huffman-encoded large strings.** `TryGetRaw` returns `nullopt` for
  `HUFFMAN_ENC`. Chunked Huffman would require a stateful streaming
  decoder; Huffman codes are variable-length so chunk boundaries
  aren't fixed on the encoded side.
- **`EXTERNAL_TAG` (tiered) values.** Asynchronous disk fetch and
  materialization; outside the in-memory zero-copy story.
- **`SMALL_TAG` and inline values.** Already cheap to copy.

## Tag / flag / counter reference

| Symbol | Where | Meaning |
|---|---|---|
| `LARGE_STR_TAG` | `CompactObj::TagEnum` | Heap-allocated raw large string |
| `NONE_ENC` / `ASCII1_ENC` / `ASCII2_ENC` | `CompactObj::EncodingEnum` | Storage encoding |
| `read_pending` | `detail::LargeString` | One or more readers may be borrowing `ptr` |
| `--get_zero_copy` | `string_family.cc` flag | Master toggle for the borrow path |
| `borrowed_string_views_total` | `EngineShard::Stats` | Shard-side: borrow decisions taken |
| `borrowed_strings_sent_total` | `ServerState::Stats` | Reply-side: borrowed bulk strings sent (incl. via capture) |

## File map

| Concern | File |
|---|---|
| `read_pending` bit, `TryGetRaw`, orphan hook | `src/core/compact_object.{h,cc}` |
| Chunked ASCII decode primitive | `src/core/detail/bitpacking.h` |
| Per-shard pin registry + MPSC drain | `src/server/engine_shard.{h,cc}` |
| Reply builder pin release, `WriteDecodedChunks`, `SendBulkStringStreamed` | `src/facade/reply_builder.{h,cc}` |
| Capture/replay overrides (borrowed + streamed) | `src/facade/reply_capture.{h,cc}`, `src/facade/reply_payload.h` |
| GET fast path (borrow + pin + dispatch) | `src/server/string_family.cc` |
| HTTP-API visitor materialization | `src/server/http_api.cc` |
| Tests | `src/server/string_family_test.cc` |
