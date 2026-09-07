/**
 * @file server/storage/src/util/epoch.h
 * The epoch counter every subsystem stamps its work with.
 */

#ifndef HELIOS_STORAGE_SRC_UTIL_EPOCH_H
#define HELIOS_STORAGE_SRC_UTIL_EPOCH_H

#include <cstdint>

namespace helios::storage {

using EpochNumber = uint32_t;

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_UTIL_EPOCH_H
