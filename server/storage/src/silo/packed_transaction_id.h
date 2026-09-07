#ifndef HELIOS_STORAGE_SRC_SILO_PACKED_TRANSACTION_ID_H
#define HELIOS_STORAGE_SRC_SILO_PACKED_TRANSACTION_ID_H

#include <cstdint>

#include "types/definitions.h"
#include "types/transaction_id.h"

namespace helios::storage {
namespace silo {

/**
 * @brief Pack a {epoch, tid} pair into one uint64_t so it can travel over
 * the RPC as an opaque version token.
 */
inline uint64_t PackTransactionId(const TransactionId &tid) {
  return (static_cast<uint64_t>(tid.epoch) << 32) |
         static_cast<uint64_t>(tid.tid);
}

/**
 * @brief Inverse of PackTransactionId.
 *
 * The stateless API hands the packed value back through Commit,
 * which unpacks it to compare with the live TransactionId on the DataItem.
 */
inline TransactionId UnpackTransactionId(uint64_t packed) {
  return {static_cast<EpochNumber>(packed >> 32),
          static_cast<uint32_t>(packed & 0xffffffffu)};
}

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_PACKED_TRANSACTION_ID_H
