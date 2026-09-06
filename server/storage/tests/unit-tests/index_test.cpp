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

#include <memory>

#include "../stateless_helper.hpp"
#include "gtest/gtest.h"

namespace {
constexpr const char* kTable = "users";
}  // namespace

class IndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.commit_durability = LineairDB::Config::CommitDurability::Volatile;
    db_.reset(nullptr);
    db_ = std::make_unique<LineairDB::Database>(config_);
    ASSERT_TRUE(db_->CreateTable(kTable));

    ASSERT_TRUE(TestHelper::CommitWrites(
        *db_, {{kTable, "alice", TestHelper::Encode<int>(1), false, false},
               {kTable, "bob", TestHelper::Encode<int>(2), false, false},
               {kTable, "carol", TestHelper::Encode<int>(3), false, false}}));
  }
};

TEST_F(IndexTest, Scan) {
  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "bob");
  ASSERT_EQ(size_t(1), rows.size());  // half-open: bob is excluded
  EXPECT_EQ(rows[0].first, "alice");
}

TEST_F(IndexTest, AlphabeticalOrdering) {
  // An inverted range holds nothing.
  EXPECT_TRUE(TestHelper::Scan(*db_, kTable, "carol", "alice").empty());

  const auto rows = TestHelper::Scan(*db_, kTable, "carol", "zzz");
  ASSERT_EQ(size_t(1), rows.size());
  EXPECT_EQ(rows[0].first, "carol");
}

TEST_F(IndexTest, StopScanning) {
  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "carol", 1);
  ASSERT_EQ(size_t(1), rows.size());
  EXPECT_EQ(rows[0].first, "alice");
}

TEST_F(IndexTest, ScanWithoutEnd) {
  const auto rows =
      TestHelper::Scan(*db_, kTable, "alice", TestHelper::kMaxKey);
  ASSERT_EQ(size_t(3), rows.size());
  EXPECT_EQ(rows[0].first, "alice");
  EXPECT_EQ(rows[1].first, "bob");
  EXPECT_EQ(rows[2].first, "carol");
}

TEST_F(IndexTest, Delete) {
  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, "bob"));

  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "carol");
  ASSERT_EQ(size_t(1), rows.size());
  EXPECT_EQ(rows[0].first, "alice");
}
