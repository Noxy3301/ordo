/**
 * @file server/storage/src/index/index_constraint.h
 * The uniqueness a secondary index is declared with.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_INDEX_CONSTRAINT_H
#define HELIOS_STORAGE_SRC_INDEX_INDEX_CONSTRAINT_H

#include <cstdint>

namespace helios::storage::index {

/**
 * @brief What a secondary index promises about its keys.
 *
 * @details kNone and kUnique are the only accepted values and
 * CreateSecondaryIndex refuses anything else. The values cross the wire, so
 * they are fixed.
 */
class IndexConstraint {
 public:
  using RawType = uint32_t;

  static constexpr RawType kNone = 0;
  static constexpr RawType kUnique = 1;

  constexpr IndexConstraint() : raw_(kNone) {}
  constexpr explicit IndexConstraint(RawType raw) : raw_(raw) {}

  static constexpr IndexConstraint FromRaw(RawType raw) {
    return IndexConstraint(raw);
  }

  constexpr RawType Raw() const { return raw_; }
  constexpr bool IsUnique() const { return raw_ == kUnique; }

 private:
  RawType raw_;
};

}  // namespace helios::storage::index

#endif  // HELIOS_STORAGE_SRC_INDEX_INDEX_CONSTRAINT_H
