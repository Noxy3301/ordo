#include <unordered_map>

#include "sql/handler.h" /* handler */
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "lineairdb_client.hh"

/**
 * @brief 
 * Wrapper of LineairDB::Transaction
 * Takes care of registering a transaction to MySQL core
 * 
 * Lifetime of this class equals the lifetime of the transaction.
 * The instance of this class is deleted in end_transaction.
 * Set the pointer to this class to nullptr after end_transaction
 * to indicate that LineairDBTransaction is terminated.
 */
class LineairDBTransaction
{
public:
  std::string get_selected_table_name();
  void choose_table(std::string db_table_name);
  bool table_is_not_chosen();

  const std::pair<const std::byte *const, const size_t> read(std::string key);
  std::vector<std::string> get_all_keys();
  std::vector<std::string> get_matching_keys(std::string key);
  bool write(std::string key, const std::string value);
  bool delete_value(std::string key);


  void begin_transaction();
  void set_status_to_abort();
  bool end_transaction();
  void fence() const;
  

  inline bool is_not_started() const {
    if (tx_id == -1) return true;
    return false;
  }

  inline int64_t get_tx_id() const {
    return tx_id;
  }

  inline bool is_aborted() const { 
    return is_aborted_;
  }

  inline void set_aborted(bool aborted) {
    is_aborted_ = aborted;
  }

  inline bool is_a_single_statement() const { return !isTransaction; }


  LineairDBTransaction(THD* thd, 
                       LineairDBClient* lineairdb_client, 
                       handlerton* lineairdb_hton,
                       bool isFence);
  ~LineairDBTransaction() = default;

private:
  int64_t tx_id;  // transaction id (instead of tx pointer), -1 means tx is not started
  LineairDBClient* lineairdb_client;
  std::string db_table_key;
  THD* thread;
  bool isTransaction;
  handlerton* hton;
  bool isFence;

  // stores RPC read results to maintain data pointer validity until transaction ends
  std::unordered_map<std::string, std::string> read_cache_;

  // transaction abort status (updated by RPC responses)
  bool is_aborted_;

  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};
