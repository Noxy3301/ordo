/**
 * @file server/storage/tests/recovery_tid_test.cc
 * That the transaction id a recovered key carries admits the next write.
 */

#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "storage/config.h"
#include "storage/database.h"

namespace {

constexpr const char *kTable = "recovery_tid_test";

helios::storage::Config MakeConfig() {
  helios::storage::Config config;
  config.epoch_duration_ms = 10;
  config.enable_recovery = true;
  config.work_dir = "./helios_recovery_tid_test_logs";
  return config;
}

// A write captures its log snapshot while the row lock is held.
// A snapshot persisted with the locked TID poisons recovery: the recovered
// row looks locked by a transaction that no longer exists, and every later
// access to the key spins or aborts forever.
TEST(RecoveryTidTest, ARecoveredKeyAcceptsTheNextWrite) {
  const auto config = MakeConfig();
  std::filesystem::remove_all(config.work_dir);

  {
    helios::storage::Database db(config);
    db.CreateTable(kTable);
    std::string reason;
    const bool committed =
        db.Commit({}, {{kTable, "alice", "v1", false}}, {}, {},
                  helios::storage::CommitDurability::kSync, &reason);
    db.ReleaseThreadEpoch();
    ASSERT_TRUE(committed) << reason;
  }

  {
    helios::storage::Database db(config);
    db.CreateTable(kTable);

    auto read = db.Read(kTable, "alice");
    db.ReleaseThreadEpoch();
    ASSERT_TRUE(read.found);
    EXPECT_EQ(read.tid % 2, 0u)
        << "the recovered TID still carries the lock bit";

    std::string reason;
    const bool committed =
        db.Commit({}, {{kTable, "alice", "v2", false}}, {}, {},
                  helios::storage::CommitDurability::kSync, &reason);
    db.ReleaseThreadEpoch();
    EXPECT_TRUE(committed) << reason;
  }

  std::filesystem::remove_all(MakeConfig().work_dir);
}

}  // namespace
