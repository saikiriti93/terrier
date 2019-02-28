#include "transaction/transaction_manager.h"
#include <algorithm>
#include <utility>

namespace terrier::transaction {
TransactionContext *TransactionManager::BeginTransaction(TransactionThreadContext *thread_context) {
  // This latch has to also protect addition of this transaction to the running transaction table. Otherwise,
  // the thread might get scheduled out while other transactions commit, and the GC will deallocate their version
  // chain which may be needed for this transaction, assuming that this transaction does not exist.
  common::SharedLatch::ScopedSharedLatch guard(&commit_latch_);
  timestamp_t start_time = time_++;

  // If no valid TransactionThreadContext is passed, use the default_worker to process this.
  if (thread_context == nullptr) {
    thread_context = default_worker_;
  }
  // TODO(Tianyu):
  // Maybe embed this into the data structure, or use an object pool?
  // Doing this with std::map or other data structure is risky though, as they may not
  // guarantee that the iterator or underlying pointer is stable across operations.
  // (That is, they may change as concurrent inserts and deletes happen)
  auto *const result =
      new TransactionContext(start_time, start_time + INT64_MIN, buffer_pool_, log_manager_, thread_context);

  // Get the latch on this threadContext's running transaction set.
  common::SpinLatch::ScopedSpinLatch running_guard(&thread_context->curr_running_txns_latch);

  const auto ret UNUSED_ATTRIBUTE = thread_context->curr_running_txns.emplace(result->StartTime());
  TERRIER_ASSERT(ret.second, "commit start time should be globally unique");
  return result;
}

TransactionThreadContext *TransactionManager::RegisterWorker(worker_id_t worker_id) {
  auto transaction_thread_context = new TransactionThreadContext(worker_id);
  // Add it to the list of TransactionThreadContexts
  {
    common::SpinLatch::ScopedSpinLatch workers_guard(&thread_contexts_latch_);
    txn_thread_contexts_.push_front(transaction_thread_context);
  }
  return transaction_thread_context;
}

void TransactionManager::UnregisterWorker(TransactionThreadContext *thread) {
  {
    common::SpinLatch::ScopedSpinLatch workers_guard(&thread_contexts_latch_);
    txn_thread_contexts_.remove(thread);
  }

  {
    // To prevent commit or abort adding something to the thread->completed_txns.
    common::SpinLatch::ScopedSpinLatch running_guard(&thread->completed_txns_latch);
    // To protect the global completed_txns_ access
    common::SpinLatch::ScopedSpinLatch completed_guard(&completed_txns_latch_);
    // Add the completed_txns in this thread to the global completed_txns.
    if (!thread->completed_txns.empty()) {
      completed_txns_.splice_after(completed_txns_.cbefore_begin(), thread->completed_txns);
      TERRIER_ASSERT(thread->completed_txns.empty(), "Completed transactions not emptied in unregister worker");
    }
  }
  delete (thread);
}

void TransactionManager::LogCommit(TransactionContext *const txn, const timestamp_t commit_time,
                                   const callback_fn callback, void *const callback_arg) {
  txn->TxnId().store(commit_time);
  if (log_manager_ != LOGGING_DISABLED) {
    // At this point the commit has already happened for the rest of the system.
    // Here we will manually add a commit record and flush the buffer to ensure the logger
    // sees this record.
    byte *const commit_record = txn->redo_buffer_.NewEntry(storage::CommitRecord::Size());
    const bool is_read_only = txn->undo_buffer_.Empty();
    storage::CommitRecord::Initialize(commit_record, txn->StartTime(), commit_time, callback, callback_arg,
                                      is_read_only, txn);
    // Signal to the log manager that we are ready to be logged out
  } else {
    // Otherwise, logging is disabled. We should pretend to have flushed the record so the rest of the system proceeds
    // correctly
    txn->log_processed_ = true;
    callback(callback_arg);
  }
  txn->redo_buffer_.Finalize(true);
}

timestamp_t TransactionManager::ReadOnlyCommitCriticalSection(TransactionContext *const txn, const callback_fn callback,
                                                              void *const callback_arg) {
  // No records to update. No commit will ever depend on us. We can do all the work outside of the critical section
  const timestamp_t commit_time = time_++;
  // TODO(Tianyu): Notice here that for a read-only transaction, it is necessary to communicate the commit with the
  // LogManager, so speculative reads are handled properly,  but there is no need to actually write out the read-only
  // transaction's commit record to disk.
  LogCommit(txn, commit_time, callback, callback_arg);
  return commit_time;
}

timestamp_t TransactionManager::UpdatingCommitCriticalSection(TransactionContext *const txn, const callback_fn callback,
                                                              void *const callback_arg) {
  common::SharedLatch::ScopedExclusiveLatch guard(&commit_latch_);
  const timestamp_t commit_time = time_++;
  // TODO(Tianyu):
  // WARNING: This operation has to happen in the critical section to make sure that commits appear in serial order
  // to the log manager. Otherwise there are rare races where:
  // transaction 1        transaction 2
  //   begin
  //   write a
  //   commit
  //                          begin
  //                          read a
  //                          ...
  //                          commit
  //                          add to log manager queue
  //  add to queue
  //
  //  Where transaction 2's commit can be logged out before transaction 1. If the system crashes between txn 2's
  //  commit is written out and txn 1's commit is written out, we are toast.
  //  Make sure you solve this problem before you remove this latch for whatever reason.
  LogCommit(txn, commit_time, callback, callback_arg);
  // flip all timestamps to be committed
  for (auto &it : txn->undo_buffer_) it.Timestamp().store(commit_time);

  return commit_time;
}

timestamp_t TransactionManager::Commit(TransactionContext *const txn, transaction::callback_fn callback,
                                       void *callback_arg) {
  const timestamp_t result = txn->undo_buffer_.Empty() ? ReadOnlyCommitCriticalSection(txn, callback, callback_arg)
                                                       : UpdatingCommitCriticalSection(txn, callback, callback_arg);
  auto worker_thread = txn->worker_thread_;
  const timestamp_t start_time = txn->StartTime();
  {
    // Get the TransactionThreadContext object associated with the txn
    common::SpinLatch::ScopedSpinLatch running_guard(&worker_thread->curr_running_txns_latch);

    // In a critical section, remove this transaction from the table of running transactions
    const size_t ret UNUSED_ATTRIBUTE = worker_thread->curr_running_txns.erase(start_time);
    TERRIER_ASSERT(ret == 1, "Committed transaction did not exist in global transactions table");
    // It is not necessary to have to GC process read-only transactions, but it's probably faster to call free off
    // the critical path there anyway
    // Also note here that GC will figure out what varlen entries to GC, as opposed to in the abort case.
  }
  {
    // We are modifying only completed_txns of the worker. So no lock required over the curr_running_txns
    common::SpinLatch::ScopedSpinLatch completed_guard(&worker_thread->completed_txns_latch);
    if (gc_enabled_) worker_thread->completed_txns.push_front(txn);
  }
  return result;
}

void TransactionManager::Abort(TransactionContext *const txn) {
  // no commit latch required here since all operations are transaction-local
  for (auto &it : txn->undo_buffer_) Rollback(txn, it);
  // The last update might not have been installed, and thus Rollback would miss it if it contains a
  // varlen entry whose memory content needs to be freed. We have to check for this case manually.
  GCLastUpdateOnAbort(txn);
  // Discard the redo buffer that is not yet logged out
  txn->redo_buffer_.Finalize(false);
  txn->log_processed_ = true;
  auto worker_thread = txn->worker_thread_;
  const timestamp_t start_time = txn->StartTime();
  {
    // In a critical section, remove this transaction from the table of running transactions
    common::SpinLatch::ScopedSpinLatch running_guard(&worker_thread->curr_running_txns_latch);
    const size_t ret UNUSED_ATTRIBUTE = worker_thread->curr_running_txns.erase(start_time);
    TERRIER_ASSERT(ret == 1, "Aborted transaction did not exist in thread transactions table");
    // Add the transaction to the list of completed transactions of the corresponding thread.
  }
  {
    // We are modifying only completed_txns of the worker. So no lock required over the curr_running_txns
    common::SpinLatch::ScopedSpinLatch completed_guard(&worker_thread->completed_txns_latch);
    if (gc_enabled_) worker_thread->completed_txns.push_front(txn);
  }
}

void TransactionManager::GCLastUpdateOnAbort(TransactionContext *const txn) {
  auto *last_log_record = reinterpret_cast<storage::LogRecord *>(txn->redo_buffer_.LastRecord());
  auto *last_undo_record = reinterpret_cast<storage::UndoRecord *>(txn->undo_buffer_.LastRecord());
  // It is possible that there is nothing to do here, because we aborted for reasons other than a
  // write-write conflict (client calling abort, validation phase failure, etc.). We can
  // tell whether a write-write conflict happened by checking the last entry of the undo to see
  // if the update was indeed installed.
  // TODO(Tianyu): This way of gcing varlen implies that we abort right away on a conflict
  // and not perform any further updates. Shouldn't be a stretch.
  if (last_log_record == nullptr) return;                                     // there are no updates
  if (last_log_record->RecordType() != storage::LogRecordType::REDO) return;  // Only redos need to be gc-ed.

  // Last update can potentially contain a varlen that needs to be gc-ed. We now need to check if it
  // was installed or not.
  auto *redo = last_log_record->GetUnderlyingRecordBodyAs<storage::RedoRecord>();
  TERRIER_ASSERT(redo->GetTupleSlot() == last_undo_record->Slot(),
                 "Last undo record and redo record must correspond to each other");
  if (last_undo_record->Table() != nullptr) return;  // the update was installed and will be handled by the GC

  // We need to free any varlen memory in the last update if the code reaches here
  const storage::BlockLayout &layout = redo->GetDataTable()->accessor_.GetBlockLayout();
  for (uint16_t i = 0; i < redo->Delta()->NumColumns(); i++) {
    // Need to deallocate any possible varlen, as updates may have already been logged out and lost.
    storage::col_id_t col_id = redo->Delta()->ColumnIds()[i];
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessWithNullCheck(i));
      if (varlen != nullptr) {
        TERRIER_ASSERT(!varlen->IsGathered(), "Fresh updates cannot be gathered already");
        txn->loose_ptrs_.push_back(varlen->Content());
      }
    }
  }
}

timestamp_t TransactionManager::OldestTransactionStartTime() const {
  bool all_empty = true;
  timestamp_t min_txn = time_.load();
  // To protect access to txn_thread_contexts_
  common::SpinLatch::ScopedSpinLatch workers_guard(&thread_contexts_latch_);
  // Find the min_element in all the curr_running_txns. If there is nothing, just return time_.load()
  for (auto const &worker_thread : txn_thread_contexts_) {
    common::SpinLatch::ScopedSpinLatch running_guard(&worker_thread->curr_running_txns_latch);

    // Get the minimum timestamp in this thread
    auto worker_min_txn =
        std::min_element(worker_thread->curr_running_txns.cbegin(), worker_thread->curr_running_txns.cend());
    if (worker_min_txn != worker_thread->curr_running_txns.end()) {
      all_empty = false;
      if (*worker_min_txn < min_txn) {
        min_txn = *worker_min_txn;
      }
    }
  }

  const timestamp_t result = (all_empty) ? time_.load() : min_txn;
  return result;
}

TransactionQueue TransactionManager::CompletedTransactionsForGC() {
  // Collect all the completed transactions
  // Lock it to ensure that commit and abort are not adding to this list.
  TransactionQueue temp_completed_txns;
  {
    common::SpinLatch::ScopedSpinLatch workers_guard(&thread_contexts_latch_);
    for (auto const &worker_thread : txn_thread_contexts_) {
      common::SpinLatch::ScopedSpinLatch completed_guard(&worker_thread->completed_txns_latch);
      // completed_txns_.splice_after(completed_txns_.cbefore_begin(), worker_thread->completed_txns);
      temp_completed_txns.splice_after(temp_completed_txns.cbefore_begin(), worker_thread->completed_txns);
      TERRIER_ASSERT(worker_thread->completed_txns.empty(), "Splicing not working for removing elements");
    }
  }

  // Handover the completed transactions to GC.
  common::SpinLatch::ScopedSpinLatch global_completed_guard(&completed_txns_latch_);
  if (!temp_completed_txns.empty()) {
    completed_txns_.splice_after(completed_txns_.cbefore_begin(), temp_completed_txns);
  }
  TransactionQueue hand_to_gc(std::move(completed_txns_));
  TERRIER_ASSERT(completed_txns_.empty(), "Handing over to GC not emptying completed transactions");
  return hand_to_gc;
}

void TransactionManager::Rollback(TransactionContext *txn, const storage::UndoRecord &record) const {
  // No latch required for transaction-local operation
  storage::DataTable *const table = record.Table();
  if (table == nullptr) {
    // This UndoRecord was never installed in the version chain, so we can skip it
    return;
  }
  const storage::TupleSlot slot = record.Slot();
  const storage::TupleAccessStrategy &accessor = table->accessor_;
  // This is slightly weird because we don't necessarily undo the record given, but a record by this txn at the
  // given slot. It ends up being correct because we call the correct number of rollbacks.
  storage::UndoRecord *const version_ptr = table->AtomicallyReadVersionPtr(slot, accessor);
  TERRIER_ASSERT(version_ptr != nullptr && version_ptr->Timestamp().load() == txn->txn_id_.load(),
                 "Attempting to rollback on a TupleSlot where this txn does not hold the write lock!");

  switch (version_ptr->Type()) {
    case storage::DeltaRecordType::UPDATE:
      // Re-apply the before image
      for (uint16_t i = 0; i < version_ptr->Delta()->NumColumns(); i++) {
        // Need to deallocate any possible varlen.
        DeallocateColumnUpdateIfVarlen(txn, version_ptr, i, accessor);
        storage::StorageUtil::CopyAttrFromProjection(accessor, slot, *(version_ptr->Delta()), i);
      }
      break;
    case storage::DeltaRecordType::INSERT:
      // Same as update, need to deallocate possible varlens.
      DeallocateInsertedTupleIfVarlen(txn, version_ptr, accessor);
      accessor.SetNull(slot, VERSION_POINTER_COLUMN_ID);
      accessor.Deallocate(slot);
      break;
    case storage::DeltaRecordType::DELETE:
      accessor.SetNotNull(slot, VERSION_POINTER_COLUMN_ID);
      break;
    default:
      throw std::runtime_error("unexpected delta record type");
  }
  // Remove this delta record from the version chain, effectively releasing the lock. At this point, the tuple
  // has been restored to its original form. No CAS needed since we still hold the write lock at time of the atomic
  // write.
  table->AtomicallyWriteVersionPtr(slot, accessor, version_ptr->Next());
}

void TransactionManager::DeallocateColumnUpdateIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                                        uint16_t projection_list_index,
                                                        const storage::TupleAccessStrategy &accessor) const {
  const storage::BlockLayout &layout = accessor.GetBlockLayout();
  storage::col_id_t col_id = undo->Delta()->ColumnIds()[projection_list_index];
  if (layout.IsVarlen(col_id)) {
    auto *varlen = reinterpret_cast<storage::VarlenEntry *>(accessor.AccessWithNullCheck(undo->Slot(), col_id));
    if (varlen != nullptr) {
      TERRIER_ASSERT(!varlen->IsGathered(), "Fresh updates cannot be gathered already");
      txn->loose_ptrs_.push_back(varlen->Content());
    }
  }
}

void TransactionManager::DeallocateInsertedTupleIfVarlen(TransactionContext *txn, storage::UndoRecord *undo,
                                                         const storage::TupleAccessStrategy &accessor) const {
  const storage::BlockLayout &layout = accessor.GetBlockLayout();
  for (uint16_t i = NUM_RESERVED_COLUMNS; i < layout.NumColumns(); i++) {
    storage::col_id_t col_id(i);
    if (layout.IsVarlen(col_id)) {
      auto *varlen = reinterpret_cast<storage::VarlenEntry *>(accessor.AccessWithNullCheck(undo->Slot(), col_id));
      if (varlen != nullptr) {
        TERRIER_ASSERT(!varlen->IsGathered(), "Fresh updates cannot be gathered already");
        txn->loose_ptrs_.push_back(varlen->Content());
      }
    }
  }
}
}  // namespace terrier::transaction
