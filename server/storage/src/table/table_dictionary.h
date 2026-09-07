#ifndef HELIOS_STORAGE_SRC_TABLE_TABLE_DICTIONARY_H
#define HELIOS_STORAGE_SRC_TABLE_TABLE_DICTIONARY_H

#include <atomic>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "table/table.h"

namespace helios::storage {

/**
 * @brief The tables of one database: published by DDL, read by every request.
 *
 * @details Append-only, since a table is never removed while the database
 * lives. Creation serializes on a mutex and publishes the new node with a
 * release store; a lookup walks the chain with an acquire load and takes no
 * lock of its own. Tables are few and created only by DDL, so the walk is
 * cheaper than the index it replaces.
 */
class TableDictionary {
 public:
  TableDictionary() = default;
  TableDictionary(const TableDictionary &) = delete;
  TableDictionary &operator=(const TableDictionary &) = delete;

  ~TableDictionary() {
    Node *node = head_.load(std::memory_order_acquire);
    while (node != nullptr) {
      Node *next = node->next;
      delete node;
      node = next;
    }
  }

  bool CreateTable(std::string_view table_name,
                   epoch::EpochFramework &epoch_framework,
                   const Config &config) {
    std::lock_guard<std::mutex> lk(create_mtx_);
    if (Find(table_name) != nullptr) return false;
    auto *node = new Node(table_name, epoch_framework, config,
                          head_.load(std::memory_order_relaxed));
    head_.store(node, std::memory_order_release);
    return true;
  }

  std::optional<Table *> GetTable(const std::string_view table_name) {
    Node *node = Find(table_name);
    if (node == nullptr) return std::nullopt;
    return &node->table;
  }

  void ForEachTable(std::function<void(Table &)> f) {
    for (Node *node = head_.load(std::memory_order_acquire); node != nullptr;
         node = node->next) {
      f(node->table);
    }
  }

 private:
  struct Node {
    std::string name;
    Table table;
    Node *next;

    Node(std::string_view n, epoch::EpochFramework &epoch_framework,
         const Config &config, Node *next)
        : name(n), table(epoch_framework, config, n), next(next) {}
  };

  Node *Find(std::string_view table_name) const {
    for (Node *node = head_.load(std::memory_order_acquire); node != nullptr;
         node = node->next) {
      if (node->name == table_name) return node;
    }
    return nullptr;
  }

  std::atomic<Node *> head_{nullptr};
  std::mutex create_mtx_;
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_TABLE_TABLE_DICTIONARY_H
