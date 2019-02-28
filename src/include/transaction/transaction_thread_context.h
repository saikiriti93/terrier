#pragma once
#include <unordered_set>
#include "common/shared_latch.h"
#include "common/spin_latch.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * A TransactionThreadContext encapsulates information about the thread on which the transaction is started
 * (and presumably will finish). While this is not essential to our concurrency control algorithm, having
 * this information tagged with each transaction helps with various performance optimizations.
 */
class TransactionThreadContext {
 public:
  /**
   * Constructs a new TransactionThreadContext with the given worker_id
   * @param worker_id the worker_id of the thread
   */
  explicit TransactionThreadContext(worker_id_t worker_id) : worker_id_(worker_id) {}

  /**
   * @return worker id of the thread
   */
  worker_id_t getWorkerId() const { return worker_id_; }

  // Thread-specific set of currently running transactions.
  std::unordered_set<timestamp_t> curr_running_txns;
  // Latch for the current running transactions.
  mutable common::SpinLatch curr_running_txns_latch;

  TransactionQueue completed_txns;
  mutable common::SpinLatch completed_txns_latch;

 private:
  // id of the worker thread on which the transaction start and finish.
  worker_id_t worker_id_;
};
}  // namespace terrier::transaction
