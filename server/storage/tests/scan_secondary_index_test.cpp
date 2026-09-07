#include <lineairdb/config.h>
#include <lineairdb/database.h>

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "stateless_helper.hpp"

namespace {
using Entries = std::vector<std::pair<std::string, std::string>>;
}  // namespace

class ScanSecondaryIndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;
  std::unique_ptr<LineairDB::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.work_dir = "./lineairdb_scan_secondary_index_test_logs";
    std::filesystem::remove_all(config_.work_dir);
    db_ = std::make_unique<LineairDB::Database>(config_);
  }
};

TEST_F(ScanSecondaryIndexTest, Delete) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "age_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", "user1", "Alice", false, false}},
      {{"users", "age_index", "25", "user1", false}}));

  const auto entries =
      TestHelper::ReadSecondaryIndex(*db_, "users", "age_index", "25");
  ASSERT_EQ(entries.size(), 1u);
  EXPECT_EQ(entries[0], "user1");

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "age_index", "25", "user1", true}}));

  EXPECT_TRUE(
      TestHelper::ReadSecondaryIndex(*db_, "users", "age_index", "25").empty());
}

TEST_F(ScanSecondaryIndexTest, DeleteAndScan) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "alpha_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice", false, false},
       {"users", "user2", "Bob", false, false},
       {"users", "user3", "Carol", false, false}},
      {{"users", "alpha_index", "a", "user1", false},
       {"users", "alpha_index", "b", "user2", false},
       {"users", "alpha_index", "c", "user3", false}}));

  // The scan range is half-open: c is the exclusive upper bound.
  EXPECT_EQ(
      TestHelper::ScanSecondaryIndex(*db_, "users", "alpha_index", "a", "c"),
      (Entries{{"a", "user1"}, {"b", "user2"}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "alpha_index", "b", "user2", true}}));
  EXPECT_TRUE(TestHelper::ReadSecondaryIndex(*db_, "users", "alpha_index", "b")
                  .empty());

  EXPECT_EQ(
      TestHelper::ScanSecondaryIndex(*db_, "users", "alpha_index", "a", "c"),
      (Entries{{"a", "user1"}}));
}

TEST_F(ScanSecondaryIndexTest, ScanShouldIncludeInsertedKeys) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", "user1", "Alice", false, false}},
      {{"users", "name_index", "alice", "user1", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user2", "Bob", false, false},
       {"users", "user3", "Carol", false, false},
       {"users", "user4", "Erin", false, false}},
      {{"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false},
       {"users", "name_index", "erin", "user4", false}}));

  // erin is the exclusive upper bound.
  EXPECT_EQ(
      TestHelper::ScanSecondaryIndex(*db_, "users", "name_index", "alice",
                                     "erin"),
      (Entries{{"alice", "user1"}, {"bob", "user2"}, {"carol", "user3"}}));
}

TEST_F(ScanSecondaryIndexTest, ScanShouldReturnKeysInOrder) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice", false, false},
       {"users", "user4", "Diana", false, false}},
      {{"users", "name_index", "alice", "user1", false},
       {"users", "name_index", "diana", "user4", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user2", "Bob", false, false},
       {"users", "user3", "Carol", false, false},
       {"users", "user5", "Erin", false, false}},
      {{"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false},
       {"users", "name_index", "erin", "user5", false}}));

  EXPECT_EQ(TestHelper::ScanSecondaryIndex(*db_, "users", "name_index", "alice",
                                           "erin"),
            (Entries{{"alice", "user1"},
                     {"bob", "user2"},
                     {"carol", "user3"},
                     {"diana", "user4"}}));
}

TEST_F(ScanSecondaryIndexTest,
       ScanReverseShouldReturnSecondaryKeysInReverseOrder) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "group_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice", false, false},
       {"users", "user2", "Bob", false, false},
       {"users", "user3", "Carol", false, false}},
      {{"users", "group_index", "g1", "user2", false},
       {"users", "group_index", "g1", "user1", false},
       {"users", "group_index", "g2", "user3", false}}));

  // Reverse walks the secondary keys backwards; the primary keys under one
  // secondary key keep their stored ascending order.
  EXPECT_EQ(TestHelper::ScanSecondaryIndex(*db_, "users", "group_index", "g1",
                                           "g3", 0, true),
            (Entries{{"g2", "user3"}, {"g1", "user1"}, {"g1", "user2"}}));
}

TEST_F(ScanSecondaryIndexTest, ScanShouldStopAtCorrectPosition) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice", false, false},
       {"users", "user4", "Diana", false, false},
       {"users", "user6", "Frank", false, false}},
      {{"users", "name_index", "alice", "user1", false},
       {"users", "name_index", "diana", "user4", false},
       {"users", "name_index", "frank", "user6", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user2", "Bob", false, false},
       {"users", "user3", "Carol", false, false},
       {"users", "user5", "Erin", false, false}},
      {{"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false},
       {"users", "name_index", "erin", "user5", false}}));

  // The row limit caps the scan at carol, well inside the range.
  EXPECT_EQ(
      TestHelper::ScanSecondaryIndex(*db_, "users", "name_index", "alice",
                                     "zzz", 3),
      (Entries{{"alice", "user1"}, {"bob", "user2"}, {"carol", "user3"}}));
}

TEST_F(ScanSecondaryIndexTest, ScanShouldExcludeDeletedKeys) {
  db_->CreateTable("users");
  db_->CreateSecondaryIndex("users", "name_index", 0);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice", false, false},
       {"users", "user2", "Bob", false, false},
       {"users", "user3", "Carol", false, false}},
      {{"users", "name_index", "alice", "user1", false},
       {"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "name_index", "bob", "user2", true}}));

  // bob is deleted, carol is the exclusive upper bound.
  EXPECT_EQ(TestHelper::ScanSecondaryIndex(*db_, "users", "name_index", "alice",
                                           "carol"),
            (Entries{{"alice", "user1"}}));
}
