#pragma once

#include <cstdint>

namespace helios::storage::index {

// The declared kind of one secondary index. kNone and kUnique are the only
// accepted values; CreateSecondaryIndex refuses anything else.
class SecondaryIndexType {
 public:
  using RawType = uint32_t;

  static constexpr RawType kNone = 0;
  static constexpr RawType kUnique = 1;

  constexpr SecondaryIndexType() : raw_(kNone) {}
  constexpr explicit SecondaryIndexType(RawType raw) : raw_(raw) {}

  static constexpr SecondaryIndexType FromRaw(RawType raw) {
    return SecondaryIndexType(raw);
  }

  constexpr RawType Raw() const { return raw_; }
  constexpr bool IsUnique() const { return raw_ == kUnique; }

 private:
  RawType raw_;
};

}  // namespace helios::storage::index
