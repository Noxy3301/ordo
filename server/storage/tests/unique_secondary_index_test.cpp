#include <lineairdb/config.h>
#include <lineairdb/database.h>

#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "stateless_helper.hpp"

namespace {

constexpr uint kUnique =
    static_cast<uint>(LineairDB::SecondaryIndexOption::Constraint::UNIQUE);

bool WriteSecondary(LineairDB::Database &db, const std::string &table_name,
                    const std::string &primary_key, const std::string &value,
                    const std::string &index_name,
                    const std::string &secondary_key) {
  return TestHelper::CommitWrites(
      db, {{table_name, primary_key, value, false, false}},
      {{table_name, index_name, secondary_key, primary_key, false}});
}

}  // namespace

class UniqueSecondaryIndexTest : public ::testing::Test {
 protected:
  LineairDB::Config config_;

  void SetUp() override {
    std::filesystem::remove_all("helios_wal");
    config_.epoch_duration_ms = 100;
  }

  void TearDown() override { std::filesystem::remove_all("helios_wal"); }
};

TEST_F(UniqueSecondaryIndexTest, DictUniqueFlagRejectsDuplicateSecondaryKey) {
  config_.work_dir = "./helios_unique_secondary_index_test_logs";
  std::filesystem::remove_all(config_.work_dir);
  config_.enable_recovery = false;

  LineairDB::Database db(config_);
  ASSERT_TRUE(db.CreateTable("users"));
  ASSERT_TRUE(db.CreateSecondaryIndex("users", "email_idx", kUnique));

  ASSERT_TRUE(WriteSecondary(db, "users", "user1", "Alice", "email_idx",
                             "alice@example.com"));
  EXPECT_FALSE(WriteSecondary(db, "users", "user2", "Bob", "email_idx",
                              "alice@example.com"));
}

TEST_F(UniqueSecondaryIndexTest,
       RecoveryRestoresUniqueSecondaryIndexTypeFromLogs) {
  config_.enable_recovery = true;

  {
    LineairDB::Database db(config_);
    ASSERT_TRUE(db.CreateTable("users"));
    ASSERT_TRUE(db.CreateSecondaryIndex("users", "email_idx", kUnique));

    ASSERT_TRUE(WriteSecondary(db, "users", "user1", "Alice", "email_idx",
                               "alice@example.com"));
  }

  LineairDB::Database recovered_db(config_);

  const auto recovered = TestHelper::ReadSecondaryIndex(
      recovered_db, "users", "email_idx", "alice@example.com");
  ASSERT_EQ(recovered.size(), 1u);
  EXPECT_EQ(recovered[0], "user1");

  EXPECT_FALSE(WriteSecondary(recovered_db, "users", "user2", "Bob",
                              "email_idx", "alice@example.com"));
}
