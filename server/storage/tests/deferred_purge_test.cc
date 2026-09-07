#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "storage/config.h"
#include "storage/database.h"
#include "storage/read.h"

namespace {

constexpr const char *kTable = "purge_test";

helios::storage::Config MakeConfig(size_t epoch_duration_ms) {
  helios::storage::Config config;
  config.epoch_duration_ms = epoch_duration_ms;
  config.enable_recovery = false;
  config.work_dir = "./helios_deferred_purge_test_logs";
  std::filesystem::remove_all(config.work_dir);
  return config;
}

bool CommitWrite(helios::storage::Database &db, const std::string &key,
                 const std::string &value, std::string *reason = nullptr) {
  const bool committed = db.Commit({}, {{kTable, key, value, false}}, {}, {},
                                   helios::storage::CommitPolicy::Sync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

bool CommitInsert(helios::storage::Database &db, const std::string &key,
                  const std::string &value, std::string *reason = nullptr) {
  const bool committed =
      db.Commit({}, {{kTable, key, value, false, true}}, {}, {},
                helios::storage::CommitPolicy::Sync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

bool CommitDelete(helios::storage::Database &db, const std::string &key,
                  std::string *reason = nullptr) {
  const bool committed = db.Commit({}, {{kTable, key, "", true}}, {}, {},
                                   helios::storage::CommitPolicy::Sync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

helios::storage::ReadResult Read(helios::storage::Database &db,
                                 const std::string &key) {
  auto result = db.Read(kTable, key);
  db.ReleaseThreadEpoch();
  return result;
}

bool ValidateRead(helios::storage::Database &db,
                  const helios::storage::ReadResult &read,
                  const std::string &key, std::string *reason) {
  const bool committed =
      db.Commit({{kTable, key, read.tid, read.found}}, {}, {}, {},
                helios::storage::CommitPolicy::Sync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

bool StartsWith(const std::string &value, const std::string &prefix) {
  return value.rfind(prefix, 0) == 0;
}

void WaitForEpochReaper(helios::storage::Database &db,
                        std::chrono::milliseconds duration) {
  std::this_thread::sleep_for(duration);
  db.ReleaseThreadEpoch();
  std::this_thread::sleep_for(duration);
}

}  // namespace

TEST(DeferredPurgeTest, SameEpochDeleteReinsertInvalidatesStaleRead) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  const auto stale = Read(db, "k");
  ASSERT_TRUE(stale.found);
  ASSERT_EQ(stale.value, "v1");

  ASSERT_TRUE(CommitDelete(db, "k"));
  ASSERT_TRUE(CommitWrite(db, "k", "v2"));

  std::string reason;
  EXPECT_FALSE(ValidateRead(db, stale, "k", &reason));
}

TEST(DeferredPurgeTest, FoundReadAbortsAfterDeferredPurgeRemovesSlot) {
  auto config = MakeConfig(5);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  const auto stale = Read(db, "k");
  ASSERT_TRUE(stale.found);

  ASSERT_TRUE(CommitDelete(db, "k"));

  std::string reason;
  for (int i = 0; i < 200; ++i) {
    ASSERT_FALSE(ValidateRead(db, stale, "k", &reason));
    if (StartsWith(reason, "exact_read_disappeared")) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(StartsWith(reason, "exact_read_disappeared")) << reason;
}

TEST(DeferredPurgeTest, ReinsertBeforeReaperKeepsLiveRow) {
  auto config = MakeConfig(200);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  ASSERT_TRUE(CommitDelete(db, "k"));
  ASSERT_TRUE(CommitWrite(db, "k", "v2"));

  WaitForEpochReaper(db, std::chrono::milliseconds(300));

  const auto live = Read(db, "k");
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, AbsentReadStillAbortsWhenRowAppears) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  const auto absent = Read(db, "k");
  ASSERT_FALSE(absent.found);

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));

  std::string reason;
  EXPECT_FALSE(ValidateRead(db, absent, "k", &reason));
  EXPECT_TRUE(StartsWith(reason, "exact_read_appeared")) << reason;
}

TEST(DeferredPurgeTest, InsertAfterThePurgeClaimsAFreshSlot) {
  auto config = MakeConfig(5);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  const std::string key = "purged_then_reinserted_key";
  ASSERT_TRUE(CommitInsert(db, key, "v1"));
  ASSERT_TRUE(CommitDelete(db, key));

  // Several epochs, so the reaper retires the tombstone's slot before the
  // insert below claims the key again.
  WaitForEpochReaper(db, std::chrono::milliseconds(100));

  ASSERT_TRUE(CommitInsert(db, key, "v2"));
  const auto live = Read(db, key);
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, TwoInsertsOfOneKeyInARequestAreRefused) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  const std::string key = "twice_inserted_key";
  std::string reason;
  EXPECT_FALSE(db.Commit(
      {}, {{kTable, key, "v1", false, true}, {kTable, key, "v2", false, true}},
      {}, {}, helios::storage::CommitPolicy::Sync, &reason));
  db.ReleaseThreadEpoch();
  EXPECT_EQ(reason, helios::storage::kDuplicateKeyAbortReason);
  EXPECT_FALSE(Read(db, key).found);

  // Deleted in between, the second insert is not a duplicate.
  reason.clear();
  EXPECT_TRUE(db.Commit({},
                        {{kTable, key, "v1", false, true},
                         {kTable, key, "", true, false},
                         {kTable, key, "v2", false, true}},
                        {}, {}, helios::storage::CommitPolicy::Sync, &reason));
  db.ReleaseThreadEpoch();
  const auto live = Read(db, key);
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, InsertOntoALiveKeyIsRefused) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(db.CreateTable(kTable));

  const std::string key = "live_key";
  ASSERT_TRUE(CommitInsert(db, key, "v1"));

  std::string reason;
  EXPECT_FALSE(CommitInsert(db, key, "v2", &reason));
  EXPECT_EQ(reason, helios::storage::kDuplicateKeyAbortReason);

  const auto live = Read(db, key);
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v1");
}
