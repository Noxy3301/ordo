#include "index/secondary_index_type.h"

#include "gtest/gtest.h"

namespace {

using helios::storage::index::SecondaryIndexType;

TEST(SecondaryIndexTypeTest, DefaultIsNotUnique) {
  EXPECT_FALSE(SecondaryIndexType().IsUnique());
  EXPECT_EQ(SecondaryIndexType().Raw(), SecondaryIndexType::kNone);
}

TEST(SecondaryIndexTypeTest, UniqueRoundTrips) {
  const auto unique = SecondaryIndexType::FromRaw(SecondaryIndexType::kUnique);
  EXPECT_TRUE(unique.IsUnique());
  EXPECT_EQ(unique.Raw(), SecondaryIndexType::kUnique);
}

}  // namespace
