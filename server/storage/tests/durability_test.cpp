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

#include <lineairdb/config.h>
#include <lineairdb/database.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "stateless_helper.hpp"
#include "util/logger.hpp"

namespace {
constexpr const char *kTable = "users";
}  // namespace

class DurabilityTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all("lineairdb_logs");
    config_.durability = LineairDB::Config::Durability::Logged;
    config_.enable_recovery = true;
    db_ = std::make_unique<LineairDB::Database>(config_);
    db_->CreateTable(kTable);
  }
};

TEST_F(DurabilityTest, Recovery) {
  // We expect LineairDB enables recovery logging by default.
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_EQ(config.durability, LineairDB::Config::Durability::Logged);

  int initial_value = 1;
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "alice", initial_value));
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "bob", initial_value));

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config);

    auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
    ASSERT_TRUE(alice.has_value());
    ASSERT_EQ(initial_value, alice.value());
    auto bob = TestHelper::Read<int>(*db_, kTable, "bob");
    ASSERT_TRUE(bob.has_value());
    ASSERT_EQ(initial_value, bob.value());
  }
}

TEST_F(DurabilityTest, RecoveryKeepsDeletedKeysAbsent) {
  // We expect LineairDB enables recovery logging by default.
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_EQ(config.durability, LineairDB::Config::Durability::Logged);

  int initial_value = 1;
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "alice", initial_value));
  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, "alice"));

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config);

    auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
    ASSERT_FALSE(alice.has_value());
  }
}

TEST_F(DurabilityTest, RecoveryLargeObject) {
  std::string initial_value(4096, 'a');
  ASSERT_TRUE(TestHelper::Write(*db_, kTable, "alice", initial_value));

  for (size_t i = 0; i < 3; i++) {
    auto alice = TestHelper::Read(*db_, kTable, "alice");
    ASSERT_TRUE(alice.has_value());
    ASSERT_EQ(initial_value, alice.value());
  }
}

TEST_F(DurabilityTest, RecoveryInContendedWorkload) {
  // We expect LineairDB enables recovery logging by default.
  const LineairDB::Config config = db_->GetConfig();
  ASSERT_EQ(config.durability, LineairDB::Config::Durability::Logged);

  const int value = 0xBEEF;
  std::vector<std::thread> writers;
  for (size_t i = 0; i < 3; i++) {
    writers.emplace_back([&] {
      while (!TestHelper::Write<int>(*db_, kTable, "alice", value)) {
      }
    });
  }
  for (auto &writer : writers) writer.join();

  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
  ASSERT_TRUE(alice.has_value());
  ASSERT_EQ(value, alice.value());
}

TEST_F(DurabilityTest, RecoveryWithNamedTable) {
  const LineairDB::Config config = db_->GetConfig();
  const std::string table_name = "accounts";
  const std::string key = "user1";
  const int value = 12345;

  // 1. Create a table and write to it
  ASSERT_TRUE(db_->CreateTable(table_name));
  ASSERT_TRUE(TestHelper::Write<int>(*db_, table_name, key, value));

  // 2. Restart DB to trigger recovery
  db_.reset(nullptr);
  db_ = std::make_unique<LineairDB::Database>(config);

  // 3. Verify data is recovered in the correct table
  auto data = TestHelper::Read<int>(*db_, table_name, key);
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), value);
}

// A commit that asked for Async is acknowledged at precommit, so it returns
// without the epoch it committed in having reached the device. The epoch
// window is the clock here: an epoch this long cannot close, let alone be
// flushed, inside the time an Async commit is allowed to take, so a return
// that fast is proof it did not wait. The Sync commit beside it does wait,
// which is what makes the comparison a contract and not a stopwatch reading.
TEST(CommitPolicyTest, AsyncDoesNotWaitForTheDevice) {
  constexpr size_t kEpochMs = 1000;
  LineairDB::Config config;
  config.work_dir = "./lineairdb_commit_policy_test_logs";
  std::filesystem::remove_all(config.work_dir);
  config.durability = LineairDB::Config::Durability::Logged;
  config.enable_recovery = false;
  config.epoch_duration_ms = kEpochMs;

  LineairDB::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  const auto commit = [&db](const std::string &key,
                            LineairDB::CommitPolicy policy) {
    const auto started = std::chrono::steady_clock::now();
    const bool committed = db.ValidateAndCommit({}, {{kTable, key, "v", false}},
                                                {}, {}, policy, nullptr);
    db.ReleaseMasstreeThreadEpoch();
    EXPECT_TRUE(committed);
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - started)
        .count();
  };

  const auto async_ms = commit("async_key", LineairDB::CommitPolicy::Async);
  EXPECT_LT(async_ms, static_cast<long>(kEpochMs))
      << "an Async commit waited for its epoch to become durable";

  const auto sync_ms = commit("sync_key", LineairDB::CommitPolicy::Sync);
  EXPECT_GE(sync_ms, 1)
      << "a Sync commit returned before any epoch could close";
  EXPECT_LT(async_ms, sync_ms)
      << "Async did not return sooner than Sync (async " << async_ms
      << " ms, sync " << sync_ms << " ms)";

  std::filesystem::remove_all(config.work_dir);
}
