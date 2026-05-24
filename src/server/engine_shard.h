// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <atomic>
#include <cstdint>

#include "base/mpsc_intrusive_queue.h"
#include "core/intent_lock.h"
#include "core/mi_memory_resource.h"
#include "core/page_usage/page_usage_stats.h"
#include "core/task_queue.h"
#include "core/tx_queue.h"
#include "server/common_types.h"
#include "util/sliding_counter.h"

typedef char* sds;

namespace dfly {

class EngineShard;
class EngineShardSet;
class TieredStorage;
class ShardDocIndices;

// Tracks the in-flight read pins on a single LargeString buffer (`ptr`).
// Lives in the owning shard's pending_read_map_ until all readers unpin and
// the entry is drained. If the writer mutates the value while readers are
// pinning, the entry is marked `orphaned` and removed from the map; when the
// last reader unpins, the buffer is freed on the owning shard's heap.
//
// Readers hold a refcnt that the IO thread decrements after the socket
// write completes. When refcnt transitions to 0, the entry is pushed to the
// owning shard's free_list_ (an MPSC queue) for cleanup on the owning
// shard's thread (mimalloc requires cross-thread free to run on the owning
// heap).
struct PendingRead {
  void* ptr = nullptr;
  std::atomic<uint32_t> refcnt{0};
  bool orphaned = false;
  EngineShard* owner_shard = nullptr;

  // Intrusive next pointer for base::MPSCIntrusiveQueue.
  std::atomic<PendingRead*> mpsc_next{nullptr};
};

inline PendingRead* MPSC_intrusive_load_next(const PendingRead& n) {
  return n.mpsc_next.load(std::memory_order_acquire);
}

inline void MPSC_intrusive_store_next(PendingRead* dest, PendingRead* next) {
  dest->mpsc_next.store(next, std::memory_order_release);
}

class EngineShard {
  friend class EngineShardSet;

 public:
  struct Stats {
    uint64_t defrag_attempt_total = 0;
    uint64_t defrag_realloc_total = 0;
    uint64_t defrag_task_invocation_total = 0;
    uint64_t defrag_skipped_mem_under_threshold = 0;
    uint64_t defrag_skipped_within_check_interval = 0;
    uint64_t defrag_skipped_not_enough_fragmentation = 0;
    uint64_t poll_execution_total = 0;

    // number of optimistic executions - that were run as part of the scheduling.
    uint64_t tx_optimistic_total = 0;
    uint64_t tx_ooo_total = 0;

    // Number of ScheduleBatchInShard calls.
    uint64_t tx_batch_schedule_calls_total = 0;

    // Number of transactions scheduled via ScheduleBatchInShard.
    uint64_t tx_batch_scheduled_items_total = 0;

    uint64_t total_heartbeat_expired_keys = 0;
    uint64_t total_heartbeat_expired_bytes = 0;
    uint64_t total_heartbeat_expired_calls = 0;

    // cluster stats
    uint64_t total_migrated_keys = 0;

    // how many huffman tables were built successfully in the background
    uint32_t huffman_tables_built = 0;

    // Stream access pattern metrics (per-command, not per-entry).
    uint64_t stream_sequential_accesses = 0;  // head/tail: XADD, XREAD recent, XTRIM, etc.
    uint64_t stream_random_accesses = 0;      // arbitrary-ID lookups: XRANGE partial, XDEL, XCLAIM
    uint64_t stream_fetch_all_accesses = 0;   // full stream scan from beginning
    uint64_t borrowed_string_views_total = 0;

    Stats& operator+=(const Stats&);
  };

  // Sets up a new EngineShard in the thread.
  // If update_db_time is true, initializes periodic time update for its db_slice.
  static void InitThreadLocal(util::ProactorBase* pb);

  // Must be called after all InitThreadLocal() have finished
  void InitTieredStorage(util::ProactorBase* pb, size_t max_file_size);

  static void DestroyThreadLocal();

  static EngineShard* tlocal() {
    return shard_;
  }

  bool IsMyThread() const {
    return this == shard_;
  }

  ShardId shard_id() const {
    return shard_id_;
  }

  PMR_NS::memory_resource* memory_resource() {
    return &mi_resource_;
  }

  TaskQueue* GetFiberQueue() {
    return &queue_;
  }

  TaskQueue* GetSecondaryQueue() {
    return &queue2_;
  }

  // Processes TxQueue, blocked transactions or any other execution state related to that
  // shard. Tries executing the passed transaction if possible (does not guarantee though).
  void PollExecution(const char* context, Transaction* trans);

  // Returns transaction queue.
  TxQueue* txq() {
    return &txq_;
  }

  const TxQueue* txq() const {
    return &txq_;
  }

  TxId committed_txid() const {
    return committed_txid_;
  }

  // Signals whether shard-wide lock is active.
  // Transactions that conflict with shard locks must subscribe into pending queue.
  IntentLock* shard_lock() {
    return &shard_lock_;
  }

  // Remove current continuation trans if its equal to tx.
  void RemoveContTx(Transaction* tx);

  const Stats& stats() const {
    return stats_;
  }

  Stats& stats() {
    return stats_;
  }

  // Calculate memory used by shard by summing multiple sources
  size_t UsedMemory() const;

  TieredStorage* tiered_storage() {
    return tiered_storage_.get();
  }

  // Zero-copy GET read-pin registry.
  //
  // PinRead(ptr): called on this shard's thread before exposing a borrowed
  // view of `ptr` to a reader. If `ptr` already has an active pin entry,
  // the entry's refcnt is incremented; otherwise a fresh entry is created
  // and inserted into pending_read_map_. The caller (e.g. CmdGet) is
  // responsible for setting the LargeString's read_pending bit and
  // forwarding the returned PendingRead* to the reply builder so the pin
  // is released after the socket write completes.
  PendingRead* PinRead(void* ptr);

  // Drop one read pin. Safe to call from any thread (typically the IO
  // thread that owns the connection). When refcnt transitions to 0, the
  // entry is pushed onto its owner shard's free_list_ for cleanup on the
  // owning shard's heap.
  static void UnpinRead(PendingRead* pin);

  // Called by LargeString when it's about to release a pointer that has
  // read_pending set. Marks the active map entry as orphaned and removes
  // it from pending_read_map_. The PendingRead takes ownership of the
  // buffer; it will be freed once the last reader unpins. Returns true if
  // the buffer was orphaned, false if no entry was found in the map (e.g.
  // because a previous drain already cleaned up — caller must free
  // normally).
  // Must be called on the shard that owns `ptr`.
  bool OrphanLargeStringPtr(void* ptr);

  // Drains the free_list_ MPSC queue. For each popped entry whose refcnt
  // is observed as 0 (i.e. not re-pinned since enqueue), either frees the
  // orphaned buffer (if orphaned=true) or removes the entry from the map
  // (if still active). Called periodically from Heartbeat().
  void DrainPendingReads();

  ShardDocIndices* search_indices() const {
    return shard_search_indices_.get();
  }

  // Moving average counters.
  enum MovingCnt : uint8_t { TTL_TRAVERSE, TTL_DELETE, COUNTER_TOTAL };

  // Returns moving sum over the last 6 seconds.
  uint32_t GetMovingSum6(MovingCnt type) const {
    return counter_[unsigned(type)].SumTail();
  }

  bool journal() const {
    return journal_;
  }

  void set_journal(bool enable) {
    journal_ = enable;
  }

  void SetReplica(bool replica) {
    is_replica_ = replica;
  }

  bool IsReplica() const {
    return is_replica_;
  }

  const Transaction* GetContTx() const {
    return continuation_trans_;
  }

  void StopPeriodicFiber();

  struct TxQueueItem {
    std::string debug_id_info;
  };

  struct TxQueueInfo {
    // Armed - those that the coordinator has armed with callbacks and wants them to run.
    // Runnable - those that could run (they own the locks) but probably can not run due
    // to head of line blocking in the transaction queue i.e. there is a transaction that
    // either is not armed or not runnable that is blocking the runnable transactions.
    // tx_total is the size of the transaction queue.
    unsigned tx_armed = 0, tx_total = 0, tx_runnable = 0, tx_global = 0;

    // total_locks - total number of the transaction locks in the shard.
    unsigned total_locks = 0;

    // contended_locks - number of locks that are contended by more than one transaction.
    unsigned contended_locks = 0;

    // The score of the lock with maximum contention (see IntentLock::ContetionScore for details).
    unsigned max_contention_score = 0;

    // the lock fingerprint with maximum contention score.
    uint64_t max_contention_lock;

    // We can use a vector to hold debug info for all items in the txqueue
    TxQueueItem head;

    std::string Format() const;
  };

  TxQueueInfo AnalyzeTxQueue() const;

  // Returns true if revelant write operations should throttle to wait for tiering to catch up.
  // The estimate is based on memory usage crossing tiering redline and the write depth being at
  // least 50% of allowed max, providing at least some guarantee of progress.
  bool ShouldThrottleForTiering() const;

  void FinalizeMulti(Transaction* tx);

  // Scan the shard with the cursor and apply defragmentation for database entries.
  // Returns collected page stats if defragmentation was performed.
  std::optional<CollectedPageStats> DoDefrag(PageUsage* page_usage);

  uint64_t GetDefragCursor() const {
    return defrag_state_.cursor;
  }

  // Return total segments merged.
  size_t CompactTable(double threshold, DbIndex db_idx);

 private:
  struct DefragTaskState {
    size_t dbid = 0u;
    uint64_t cursor = 0u;
    time_t last_check_time = 0;
    float page_utilization_threshold = 0.8;

    enum class SkipReason : uint8_t {
      MemoryTooLow,
      MemoryBelowThreshold,
      CheckWithinInterval,
      NotEnoughFragmentation,
      CheckInProgress,
      NotSkipped,
    };

    // check the current threshold and return a reason if we skip the defragmentation
    SkipReason CheckRequired();

    void UpdateScanState(uint64_t cursor_val);

    void ResetScanState();
  };

  struct EvictionTaskState {
    void Reset(bool rss_eviction_enabled_flag) {
      rss_eviction_enabled = rss_eviction_enabled_flag;
      shard_used_memory_at_prev_eviction = global_rss_memory_at_prev_eviction =
          acc_deleted_bytes_during_eviction = deleted_bytes_at_prev_eviction = 0;
    }
    void AdjustDeletedBytes(size_t shard_used_memory);
    void LimitAccumulatedDeletedBytes(size_t shard_rss_over_memory_budget);
    void AdjustAccumulatedDeletedBytes(size_t global_used_rss_memory);
    bool rss_eviction_enabled = true;
    bool track_deleted_bytes = false;
    size_t acc_deleted_bytes_during_eviction = 0;  // Accumulated deleted bytes during eviction
    size_t deleted_bytes_at_prev_eviction = 0;     // Bytes deleted in previous eviction
    size_t shard_used_memory_at_prev_eviction = 0;
    size_t global_rss_memory_at_prev_eviction = 0;
  };

  EngineShard(util::ProactorBase* pb, mi_heap_t* heap);

  // blocks the calling fiber.
  void Shutdown();  // called before destructing EngineShard.

  void StartPeriodicHeartbeatFiber(util::ProactorBase* pb);
  void StartPeriodicShardHandlerFiber(util::ProactorBase* pb, std::function<void()> shard_handler);

  void Heartbeat();
  void RetireExpiredAndEvict();

  /* Calculates the number of bytes to evict based on memory and rss memory usage. */
  size_t CalculateEvictionBytes();

  void CacheStats();

  // We are running a task that checks whether we need to
  // do memory de-fragmentation here, this task only run
  // when there are available CPU time.
  // --------------------------------------------------------------------------
  // NOTE: This task is running with exclusive access to the shard.
  // i.e. - Since we are using shared noting access here, and all access
  // are done using fibers, This fiber is run only when no other fiber in the
  // context of the controlling thread will access this shard!
  // --------------------------------------------------------------------------
  uint32_t DefragTask();

  TxQueue txq_;
  TaskQueue queue_, queue2_;

  ShardId shard_id_;
  Stats stats_;

  // Become passive if replica: don't automatially evict expired items.
  bool is_replica_ = false;
  bool journal_ = false;

  // Precise tracking of used memory by persistent shard local values and structures
  MiMemoryResource mi_resource_;

  struct {
    size_t used_mem = 0;
  } last_mem_params_;

  // Logical ts used to order distributed transactions.
  TxId committed_txid_ = 0;
  Transaction* continuation_trans_ = nullptr;
  std::string continuation_debug_id_;
  unsigned poll_concurrent_factor_ = 0;

  IntentLock shard_lock_;

  uint32_t defrag_task_id_ = UINT32_MAX, huffman_check_task_id_ = UINT32_MAX;
  EvictionTaskState eviction_state_;  // Used on eviction fiber
  util::fb2::Fiber fiber_heartbeat_periodic_;
  util::fb2::Done fiber_heartbeat_periodic_done_;

  util::fb2::Fiber fiber_shard_handler_periodic_;
  util::fb2::Done fiber_shard_handler_periodic_done_;

  DefragTaskState defrag_state_;
  std::unique_ptr<TieredStorage> tiered_storage_;

  // Active read pins (per-shard). Keyed by the LargeString buffer pointer.
  // Only touched on this shard's thread.
  absl::flat_hash_map<void*, PendingRead*> pending_read_map_;

  // MPSC queue of PendingRead* entries whose refcnt has dropped to zero
  // and need cleanup. Producers: any IO thread (via UnpinRead). Consumer:
  // this shard's thread (DrainPendingReads from Heartbeat).
  base::MPSCIntrusiveQueue<PendingRead> pending_read_free_list_;
  // TODO: Move indices to Namespace
  std::unique_ptr<ShardDocIndices> shard_search_indices_;
  uint64_t stalled_start_ns_ = 0;
  using Counter = util::SlidingCounter<7>;

  Counter counter_[COUNTER_TOTAL];

  static __thread EngineShard* shard_;
};

}  // namespace dfly
