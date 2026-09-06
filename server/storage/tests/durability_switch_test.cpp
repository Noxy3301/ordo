#include <lineairdb/config.h>
#include <lineairdb/database.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "stateless_helper.hpp"

namespace {

constexpr auto kBarrierTimeout = std::chrono::seconds(10);
constexpr int kValue = 42;
const char *const kTable = "users";
const char *const kWorkDir = "lineairdb_durability_switch_logs";

}  // namespace

// Database::SetCommitDurability, which a load can use to run under Async and
// then adopt Sync before the first measured transaction.
class DurabilitySwitchTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;

  void SetUp() override {
    std::filesystem::remove_all(kWorkDir);
    config_.work_dir = kWorkDir;
    // The combination the server runs; the defaults need build options this
    // tree does not set.
    config_.commit_durability = LineairDB::Config::CommitDurability::Async;
    config_.enable_recovery = true;
  }

  void TearDown() override {
    db_.reset(nullptr);
    std::filesystem::remove_all(kWorkDir);
  }
};

TEST_F(DurabilitySwitchTest, AsyncCommitsSurviveTheSwitchToSync) {
  db_ = std::make_unique<LineairDB::Database>(config_);
  ASSERT_TRUE(db_->CreateTable(kTable));
  ASSERT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Async);

  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "alice", kValue));

  ASSERT_TRUE(db_->SetCommitDurability(
      LineairDB::Config::CommitDurability::Sync, kBarrierTimeout));
  EXPECT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  // The barrier returned, so the record is on the device and a replay finds
  // it. The reopen is clean rather than a crash; what it checks is that the
  // switch left a log a recovery can read the acknowledged value out of.
  const LineairDB::Config reopen = db_->GetConfig();
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(reopen);
  const auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
  ASSERT_TRUE(alice.has_value());
  ASSERT_EQ(kValue, alice.value());
}

TEST_F(DurabilitySwitchTest, VolatileDatabaseRefusesTheSwitch) {
  config_.commit_durability = LineairDB::Config::CommitDurability::Volatile;
  config_.enable_recovery = false;
  db_ = std::make_unique<LineairDB::Database>(config_);
  ASSERT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Volatile);

  EXPECT_FALSE(db_->SetCommitDurability(
      LineairDB::Config::CommitDurability::Sync, kBarrierTimeout));
  EXPECT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Volatile);
}

TEST_F(DurabilitySwitchTest, VolatileTargetIsRefused) {
  db_ = std::make_unique<LineairDB::Database>(config_);

  EXPECT_FALSE(db_->SetCommitDurability(
      LineairDB::Config::CommitDurability::Volatile, kBarrierTimeout));
  EXPECT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Async);
}

TEST_F(DurabilitySwitchTest, SyncToAsyncNeedsNoBarrier) {
  config_.commit_durability = LineairDB::Config::CommitDurability::Sync;
  db_ = std::make_unique<LineairDB::Database>(config_);
  ASSERT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Sync);

  const auto started = std::chrono::steady_clock::now();
  EXPECT_TRUE(db_->SetCommitDurability(
      LineairDB::Config::CommitDurability::Async, kBarrierTimeout));
  EXPECT_LT(std::chrono::steady_clock::now() - started,
            std::chrono::seconds(1));
  EXPECT_EQ(db_->GetCommitDurability(),
            LineairDB::Config::CommitDurability::Async);
}
