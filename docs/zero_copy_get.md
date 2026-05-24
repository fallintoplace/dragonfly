# Zero-Copy GET for Large Strings

This document describes the zero-copy `GET` path for large string values in
Dragonfly. The feature lands in two stages: an MVP that's safe under read-only
traffic and a Copy-on-Write (CoW) extension that makes it safe under arbitrary
concurrent mutations.

## Motivation

A `GET` on a value ≥1 KiB does ~2 transient copies between storage and the
socket:

1. **Shard-side allocation.** `ReadString` calls `pv.ToString()` which
   `malloc`s a `std::string` and `memcpy`s the value into it
   (`src/server/string_family.cc`, `CompactObj::ToString` →
   `CompactObj::GetString(string*)`).
2. **Reply-builder copy** (small replies only). For replies <4 KiB,
   `SinkReplyBuilder::FinishScope` may copy refs into its 8 KiB buffer so the
   source can die before the actual socket write. For replies ≥4 KiB,
   `FinishScope` early-flushes — no copy here.

Step 2 is already zero-copy for the size class we care about; the real waste
is step 1. Eliminating it requires teaching the GET handler to pass a
borrowed `string_view` directly into the shard's `CompactObj` storage to the
reply builder, *and* ensuring the underlying bytes outlive any potential
fiber yield during the socket write.

## Background — relevant components

### `detail::LargeString`

Storage for raw non-inline string values in `CompactObj`. Packed 16 bytes:

```
struct LargeString {
  void* ptr;            // mimalloc allocation
  uint64_t sz : 56;     // current length
  uint64_t read_pending : 1;  // CoW hook (see below)
  uint64_t reserved : 7;
};
```

Only `LARGE_STR_TAG` CompactObj values use `LargeString`. Smaller / encoded
/ inline strings live in different union members.

### `SinkReplyBuilder`

Per-connection, lives on the connection's IO/proactor thread. Accumulates
`iovec`s via `WritePieces` (copy-into-buffer) or `WriteRef` (push pointer
only). `Send()` does a synchronous `writev` on the socket and returns when
the bytes are in the kernel. `ReplyScope` is the explicit lifetime guard:
within a scope, callers promise that `WriteRef`'d views remain valid until
`FinishScope` runs.

### Threading model

Dragonfly is shared-nothing. Each shard owns a fiber-friendly proactor
thread (its mimalloc heap is thread-local). A connection is bound to one
proactor. Commands hop from the connection's proactor to the key's shard
via `SingleHopT(cb)`. After `cb` returns, control resumes on the
connection's proactor for reply construction.

For a `GET` on a same-thread key, the cb and reply run on the same thread.
For a cross-shard `GET`, the shard reads the value and returns a result;
the connection's proactor consumes it. Either way the borrowed pointer
may need to outlive a fiber yield (the socket `writev` can suspend).

## Stage 1 — MVP (read-only safe)

### What it changes

- New `CompactObj::TryGetRawView() const → optional<string_view>` —
  returns `u_.large_str.AsView()` iff `taglen_==LARGE_STR_TAG && encoding_==NONE_ENC && !IsExternal()`. Nullopt otherwise.
- New `BorrowedString { string_view view; }` strong-typed variant
  alternative in `StringResult`.
- New `ReadStringBorrow` helper: tries `TryGetRawView`, falls back to
  `ReadString` (owned `std::string`) otherwise.
- `CmdGet` uses `ReadStringBorrow`. Other readers (`GETDEL`, `GETEX`,
  `GETSET`) keep `ReadString` because they mutate or remove the key.
- New `SendBulkStringBorrowed(view)` on the reply builder — same as
  `SendBulkString` for the default sink, but `CapturingReplyBuilder`
  (used by the squashing / `MULTI`-`EXEC` path) overrides it to preserve
  the borrowed view instead of materializing a `std::string` copy. This
  is critical for pipelined / `EXEC` workloads — without it the captured
  payload would defeat zero-copy.

### Why it's read-only

Between `SendBulkStringBorrowed(view)` and the socket-write completion,
the borrowed pointer must remain valid. The shard's fiber may yield
during `Send`'s `writev`, allowing other fibers (different connection's
commands on the same shard) to run. Under read-only traffic none of
them frees the buffer; with mutations any of:

- `SET` overwrite — `LargeString::SetString` deallocates the prior buffer
  inline.
- `APPEND` / `SETRANGE` — `ExtendExisting` reads + `SetString`.
- `DEL` / TTL expiry / eviction — `PerformDeletionAtomic`.
- Defrag — `LargeString::DefragIfNeeded` reallocates in place.

…would invalidate the pointer.

The MVP ships behind a startup flag `--get_zero_copy` (cached
thread-locally in `ReadStringBorrow`, no per-GET overhead). Two INFO
counters expose engagement: `borrowed_string_views_total` (shard-side
decision) and `borrowed_strings_sent_total` (reply-side, including
capture/replay through squashing).

## Stage 2 — Copy-on-Write

CoW lifts the read-only restriction. Three ingredients:

1. **A `read_pending` bit on `LargeString`.** Set when at least one reader
   is borrowing `ptr`.
2. **A per-shard pin registry.** Maps active buffer pointers to a
   refcount-bearing `PendingRead` entry.
3. **Mutation interception** in `LargeString::SetString` /
   `LargeString::Free`. When `read_pending=1`, the writer *orphans* the
   buffer (transfers ownership to the registry) instead of deallocating
   it inline. The `CompactObj` installs a fresh allocation in place.

When the last reader unpins, the orphaned buffer is reclaimed on the
owning shard's heap (mimalloc requires cross-thread free to run on the
owning heap).

### `PendingRead`

```cpp
struct PendingRead {
  void* ptr;                            // buffer being tracked
  std::atomic<uint32_t> refcnt;         // active reader count
  bool orphaned;                        // writer detached it from CompactObj
  EngineShard* owner_shard;             // for cross-thread routing
  std::atomic<PendingRead*> mpsc_next;  // intrusive next for MPSC free list
};
```

`PendingRead` is allocated by `EngineShard::PinRead` and lives until the
shard drains it (refcnt==0 + processed). It's *not* allocated from the
shard's `MiMemoryResource` — `new`/`delete` is fine since it's a small
fixed-size struct and we want cross-thread visibility of the metadata.

### Per-shard registry

```cpp
class EngineShard {
  // Active pins. Only touched on this shard's thread.
  absl::flat_hash_map<void*, PendingRead*> pending_read_map_;

  // PendingRead entries whose refcnt reached zero on a remote thread,
  // waiting for cleanup on this shard.
  base::MPSCIntrusiveQueue<PendingRead> pending_read_free_list_;
};
```

`pending_read_map_` is single-threaded (shard-only). Producers of
`pending_read_free_list_` are any IO threads via `UnpinRead`; consumer
is this shard via `DrainPendingReads`.

### API

```cpp
// On the shard thread, at borrow time.
PendingRead* shard->PinRead(void* ptr);

// Sets the bit on the LargeString — must be paired with PinRead.
pv.MarkReadPending();

// Any thread, after the socket write completes.
static void EngineShard::UnpinRead(PendingRead* pin);

// Shard thread, from Heartbeat() and Shutdown().
void shard->DrainPendingReads();

// Shard thread, called by LargeString::SetString / Free via callback.
bool shard->OrphanLargeStringPtr(void* ptr);
```

### LargeString hook

`compact_object.cc` declares a thread-local function pointer:

```cpp
thread_local LargeStringOrphanFn on_large_str_orphan;
```

`EngineShard::InitThreadLocal` installs a callback that forwards to
`OrphanLargeStringPtr` on the current shard. A common helper
`ReleasePtr` is invoked by `LargeString::SetString` and `LargeString::Free`:

```cpp
static void ReleasePtr(LargeString* ls, MemoryResource* mr) {
  if (ls->ptr == nullptr) {
    ls->SetReadPending(false);
    return;
  }
  bool orphaned = false;
  if (ls->IsReadPending() && on_large_str_orphan != nullptr)
    orphaned = on_large_str_orphan(ls->ptr);
  if (!orphaned)
    mr->deallocate(ls->ptr, 0, kAlignSize);
  ls->ptr = nullptr;
  ls->SetReadPending(false);
}
```

`SetString` triggers `ReleasePtr` when either the new value doesn't fit in
the existing buffer **or** `read_pending=1` (in-place overwrite of a buffer
visible to a reader would corrupt their bytes).

### Reply-builder integration

`SinkReplyBuilder` grows two members:

```cpp
absl::InlinedVector<void*, 4> post_send_pins_;
static void SetPostSendUnpinFn(void (*fn)(void*));  // wired at server startup
void AddPostSendPin(void* pin);
```

After `Send()`'s `writev` returns, the builder drains
`post_send_pins_` through the installed function (forwards to
`EngineShard::UnpinRead`).

The reply builder is intentionally typed `void*` and pulls no
`server/engine_shard.h` dependency — the function pointer is the only
seam.

### CmdGet flow

```cpp
// shard thread, inside SingleHopT callback:
if (auto view = pv.TryGetRawView()) {
  PendingRead* pin = es->PinRead(view->data());
  pv.MarkReadPending();
  return BorrowedString{*view, pin};
}

// proactor thread, building the reply:
rb->SendBulkStringBorrowed(bs.view);   // pushes iovec ref
if (bs.pin)
  rb->AddPostSendPin(bs.pin);           // released after Send()'s writev
```

Order matters: `AddPostSendPin` runs **after** `SendBulkStringBorrowed`. The
latter may internally `Flush` if it hits `IOV_MAX`; that internal `Send`
drains `post_send_pins_`, so the new pin must be queued afterward to avoid
early release.

## End-to-end timeline

```
                    Shard A thread                           IO thread (conn X)
                    --------------                           ------------------
T0  GET k (cb)
      pv = FindReadOnly(k)
      view = pv.TryGetRawView() // points into u_.large_str.ptr
      pin = PinRead(view.data())
      pv.MarkReadPending()      // read_pending = 1
      return BorrowedString{view, pin}
                                  ─── SingleHopT result ──>
T1                                                          rb->SendBulkStringBorrowed(view)
                                                            rb->AddPostSendPin(pin)
T2  (other commands run on A while X's reply is queued)     rb->Flush() → Send() → sink->Write(vecs)
      e.g. SET k newval2:
        LargeString::SetString(newval2)
          read_pending == 1
            on_large_str_orphan(ptr) → OrphanLargeStringPtr:
              entry = pending_read_map_[ptr]
              entry->orphaned = true
              pending_read_map_.erase(ptr)
              return true
          allocate new buffer, install in CompactObj
          read_pending = 0
T3                                                          (Send returns; bytes in kernel)
                                                            for p in post_send_pins_: unpin(p)
                                                              UnpinRead(pin):
                                                                fetch_sub(1) == 1
                                                                push to A's pending_read_free_list_
T4  Heartbeat:
      DrainPendingReads()
        pop entry, refcnt==0, orphaned=true
        mi_resource_.deallocate(entry->ptr) // freed on A's heap
        delete entry
```

If no mutation happens between T0 and T3 (the read-only case the MVP
targeted), at T4 the entry is `orphaned=false`: drain simply erases it
from the map; the `CompactObj` continues to own the buffer.

## Race conditions and safety

### Drain vs. re-pin

After `UnpinRead` pushes `entry` to `free_list_`, but before `DrainPendingReads`
pops it, another reader on the same shard can call `PinRead(ptr)` and find
the entry in the map with refcnt 0 → 1. When drain eventually pops, it
re-loads refcnt with `acquire` ordering and skips entries with refcnt > 0.
The new readers will themselves push to `free_list_` when they unpin.

### Post-drain mutation

The `LargeString::read_pending` bit can linger after the last reader has
unpinned and `DrainPendingReads` has erased the map entry (for the
non-orphaned case). If a writer mutates *after* that point, the orphan
callback looks up `ptr` and doesn't find it. The callback returns `false`;
`LargeString` falls through to a normal `mr->deallocate`. Correct, because
no active reader has the pointer.

### Defrag

`LargeString::DefragIfNeeded` would reallocate the buffer and break any
outstanding borrowed view. When `read_pending=1`, it returns `false` early
without doing anything. The next defrag pass after the pin clears will pick
up the same buffer.

### AppendString

`LargeString::AppendString` mutates the buffer in place — that would
corrupt pinned readers. It DCHECKs `!IsReadPending()`. The only caller is
`rdb_load`, which never produces values that have been pinned.

### Cross-thread free

Pin allocated on shard A but unpinned on a different IO thread X: when
refcnt → 0 on X, the entry is pushed onto **A's** MPSC queue. The actual
`mr->deallocate` runs in A's `DrainPendingReads`, on A's mimalloc heap.
mimalloc supports cross-thread free, but routing it back to the owning
heap avoids the slow path and the cross-thread accounting overhead.

### Pipelining / `EXEC`

`MultiCommandSquasher` uses `CapturingReplyBuilder` which records replies
into intermediate payloads before flushing to the real sink. The
`SendBulkStringBorrowed` override stores the borrowed view directly in the
capture (no copy). The lifetime contract holds: pins added via
`AddPostSendPin` are not released until the final `Send` writes the
captured payload.

## What's out of scope

- **MGET.** Today `CollectKeys` allocates a per-shard storage buffer
  (`make_unique<char[]>`) and packs values into it. Zero-copy MGET would
  require restructuring this path to carry borrowed views per result.
  Future work.
- **Encoded large strings.** ASCII1/ASCII2/HUFFMAN-encoded values must
  be decoded before reaching the wire. `TryGetRawView` returns `nullopt`
  for them; they keep the existing `pv.ToString` path.
- **Tiered (`EXTERNAL_TAG`) values.** Require an asynchronous disk
  fetch and live materialization; outside the zero-copy story.
- **`SMALL_TAG` (<256 B) values.** Already cheap to copy; the
  `LARGE_STR_TAG` boundary is the practical cutoff.

## Tag / flag / counter reference

| Symbol | Where | Meaning |
|---|---|---|
| `LARGE_STR_TAG` | `CompactObj::TagEnum` | Heap-allocated raw large string |
| `NONE_ENC` | `CompactObj::EncodingEnum` | Stored bytes are the raw value |
| `read_pending` | `detail::LargeString` | One or more readers may be borrowing `ptr` |
| `--get_zero_copy` | `string_family.cc` flag | Master toggle for the borrow path |
| `borrowed_string_views_total` | `EngineShard::Stats` | Shard-side: borrow decisions taken |
| `borrowed_strings_sent_total` | `ServerState::Stats` | Reply-side: borrowed bulk strings sent (incl. via capture) |

## File map

| Concern | File |
|---|---|
| Bit, view accessor, orphan hook | `src/core/compact_object.{h,cc}` |
| Per-shard pin registry + MPSC drain | `src/server/engine_shard.{h,cc}` |
| Reply builder pin release | `src/facade/reply_builder.{h,cc}` |
| Capture/replay override | `src/facade/reply_capture.{h,cc}` |
| GET fast path | `src/server/string_family.cc` |
| Tests | `src/server/string_family_test.cc` |
