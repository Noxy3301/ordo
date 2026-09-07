#include "index/index_constraint.h"

#include "gtest/gtest.h"

namespace {

using helios::storage::index::IndexConstraint;

TEST(IndexConstraintTest, DefaultIsNotUnique) {
  EXPECT_FALSE(IndexConstraint().IsUnique());
  EXPECT_EQ(IndexConstraint().Raw(), IndexConstraint::kNone);
}

TEST(IndexConstraintTest, UniqueRoundTrips) {
  const auto unique = IndexConstraint::FromRaw(IndexConstraint::kUnique);
  EXPECT_TRUE(unique.IsUnique());
  EXPECT_EQ(unique.Raw(), IndexConstraint::kUnique);
}

}  // namespace
