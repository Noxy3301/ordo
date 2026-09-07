#include <storage/config.h>
#include <storage/database.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "../stateless_helper.hpp"
#include "gtest/gtest.h"

namespace {
constexpr const char *kTable = "users";
}  // namespace

class ScanDeleteVisibilityTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.work_dir = "./helios_scan_delete_visibility_test_logs";
    std::filesystem::remove_all(config_.work_dir);
    db_ = std::make_unique<helios::storage::Database>(config_);
    ASSERT_TRUE(db_->CreateTable(kTable));
  }
};

TEST_F(ScanDeleteVisibilityTest, ScanShouldExcludeDeletedKeys) {
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{kTable, "alice", TestHelper::Encode<int>(1), false, false},
             {kTable, "bob", TestHelper::Encode<int>(2), false, false},
             {kTable, "carol", TestHelper::Encode<int>(3), false, false}}));

  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, "bob"));

  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "carol");
  // bob is deleted and carol is the exclusive upper bound.
  ASSERT_EQ(rows.size(), size_t(1));
  EXPECT_EQ(rows[0].first, "alice");
  EXPECT_EQ(TestHelper::Decode<int>(rows[0].second), 1);
}
