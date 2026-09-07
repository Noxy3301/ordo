// durability.cc
// The durable frontier and the epoch it is compared against.

#include "database_impl.h"

namespace helios::storage {

EpochNumber Database::Impl::GetDurableEpoch() const {
  return logger_.GetDurableEpoch();
}

EpochNumber Database::Impl::GetGlobalEpoch() const {
  return epoch_framework_.GetGlobalEpoch();
}

}  // namespace helios::storage
