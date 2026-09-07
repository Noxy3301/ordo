/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef HELIOS_CONFIG_H
#define HELIOS_CONFIG_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace helios::storage {

/**
 * @brief
 * Configuration and options for LineairDB instances.
 */
struct Config {
  /**
   * @brief
   * The size of epoch duration (milliseconds). See [Tu13, Chandramouli18] to
   * get more details of epoch-based group commit. Briefly, LineairDB
   * concurrently processes transactions included in the same epoch duration. As
   * you set the larger duration to this paramter, you may get a higher
   * throughput. However, this setting results in the increase of average
   * response time.
   *
   * Default: 40ms.
   * @see [Tu13] https://dl.acm.org/doi/10.1145/2517349.2522713
   * @see [Chandramouli18]
   * https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
   */
  size_t epoch_duration_ms = 40;

  /**
   * @brief
   * If true, tables may install PAX storage metadata and route newly-created
   * row payloads through PaxStore.
   *
   * Default: false.
   */
  bool enable_pax_storage = false;

  /**
   * @brief
   * If true, LineairDB processes recovery at the instantiation.
   *
   * Default: true
   */
  bool enable_recovery = true;

  /**
   * @brief
   * How much of the write-ahead log is made writable in place at a time.
   *
   * @details
   * The log file is written out with zeroes to this size before any record
   * lands in it, and records are then written in place, so a commit's
   * fdatasync persists data and not the size, allocation, or extent-state
   * metadata a growing file drags in (the benefit is filesystem- and
   * device-specific). A log that grows past it is extended by the same
   * amount again: a granularity rather than a limit. Larger means fewer
   * synchronous extensions but a longer startup scan of the reserved
   * region. Zero disables reservation and lets the file grow as written.
   *
   * @note The storage stack must honour fsync/fdatasync; the supported
   * ext4 setup uses its default data ordering and barriers. A log from a
   * build that did not reserve is readable here; the reverse does not
   * hold (such a build reads the reserved zeroes as a broken frame and
   * refuses to start).
   *
   * Default: 64 MiB
   */
  uint64_t wal_initial_capacity_bytes = 64ull * 1024ull * 1024ull;

  /**
   * @brief
   * How often (milliseconds) an image of the live rows is written, or zero to
   * write none.
   *
   * The image is scanned while transactions keep running and is merged with
   * the log at recovery, which is what bounds the part of the log that has to
   * be replayed. It does not bound the log on disk: nothing is truncated.
   *
   * Requires a durability contract that writes a log, since the image alone is
   * not a recoverable state.
   *
   * Default: 0 (no image)
   */
  size_t checkpoint_interval_ms = 0;

  /**
   * @brief
   * One image written this many milliseconds after startup, or zero for none.
   * Independent of checkpoint_interval_ms, which keeps its own cadence
   * afterwards when both are set.
   *
   * Default: 0 (no image)
   */
  size_t checkpoint_once_after_ms = 0;

  /**
   * @brief
   * The directory path that lineardb use as working directory.
   * All of data, logs and related files are stored in the directory.
   *
   * Default: "helios_wal"
   */
  std::string work_dir = "./helios_wal";
};

// Secondary index options (moved from secondary_index_option.h)
struct SecondaryIndexOption {
  enum class Constraint : unsigned {
    NONE = 0u,
    UNIQUE = 1u << 0,
    NOT_NULL = 1u << 1,
  };
};

// bitwise operators for enum class Constraint
constexpr SecondaryIndexOption::Constraint operator|(
    SecondaryIndexOption::Constraint a, SecondaryIndexOption::Constraint b) {
  using U = std::underlying_type_t<SecondaryIndexOption::Constraint>;
  return static_cast<SecondaryIndexOption::Constraint>(static_cast<U>(a) |
                                                       static_cast<U>(b));
}

constexpr SecondaryIndexOption::Constraint operator&(
    SecondaryIndexOption::Constraint a, SecondaryIndexOption::Constraint b) {
  using U = std::underlying_type_t<SecondaryIndexOption::Constraint>;
  return static_cast<SecondaryIndexOption::Constraint>(static_cast<U>(a) &
                                                       static_cast<U>(b));
}

constexpr bool HasFlag(SecondaryIndexOption::Constraint set,
                       SecondaryIndexOption::Constraint flag) {
  using U = std::underlying_type_t<SecondaryIndexOption::Constraint>;
  return (static_cast<U>(set) & static_cast<U>(flag)) != 0;
}
}  // namespace helios::storage

#endif
