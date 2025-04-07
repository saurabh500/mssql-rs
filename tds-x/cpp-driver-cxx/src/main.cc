#include "cpp-driver-cxx/src/lib.rs.h"
#include "tds_connection.h"

#include <iostream>

void display_results_sync(
    rust::Box<::cxx_ffi::QueryResultTypeStream> &results) {
  while (results->next()) {
    auto current_result = results->current_result();
    if (current_result->get_type() == cxx_ffi::ResultType::Update) {
      auto update_result = current_result->take_update_result();
      std::cout << "Update Result: " << update_result << std::endl;
    } else {
      auto result_set = current_result->take_result_set();
      while (result_set->next()) {
        auto row_data = result_set->current_row();
        while (row_data->next()) {
          auto cell = row_data->current_cell();
          auto column_value = cell->take_column_value();
          column_value->print_column_value();
          std::cout << ", ";
        }
        std::cout << std::endl;
      }
    }
  }
}

void run_sync(std::string host, uint16_t port, std::string user,
              std::string password, std::string catalog) {
  std::cout << "Running in synchronous mode." << std::endl;
  auto connection = tds::TdsConnection::create_connection(host, port, user,
                                                          password, catalog);

  std::string input;
  do {
    std::cout << "Enter your SQL query (or leave blank to exit): ";
    getline(std::cin, input);
    if (!input.empty()) {
      auto results = connection->execute(input);
      display_results_sync(results);
    }
  } while (!input.empty());
}

void display_results_async(
    rust::Box<::cxx_ffi::QueryResultTypeFuture> &result_future) {
  auto results = result_future->await_query_result_type();
  while (results->next_async()->await_bool()) {
    auto current_result = results->current_result();
    if (current_result->get_type() == cxx_ffi::ResultType::Update) {
      auto update_result = current_result->take_update_result();
      std::cout << "Update Result: " << update_result << std::endl;
    } else {
      auto result_set = current_result->take_result_set();
      while (result_set->next_async()->await_bool()) {
        auto row_data = result_set->current_row();
        while (row_data->next_async()->await_bool()) {
          auto cell = row_data->current_cell();
          auto column_value = cell->take_column_value();
          column_value->print_column_value();
          std::cout << ", ";
        }
        std::cout << std::endl;
      }
    }
  }
}

void run_async(std::string host, uint16_t port, std::string user,
               std::string password, std::string catalog) {
  // Note: This differs from run_sync in that it uses methods generated from cxx
  // directly. In practice, a public API should wrap them to avoid exposing the
  // underlying code gen method.
  std::cout << "Running in async mode." << std::endl;
  auto context =
      cxx_ffi::create_client_context(host, port, user, password, catalog);
  auto connection_future = cxx_ffi::create_connection_async(*context);

  auto connection = connection_future->await_connection();
  std::string input;
  do {
    std::cout << "Enter your SQL query (or leave blank to exit): ";
    getline(std::cin, input);
    if (!input.empty()) {
      auto results_future = connection->execute_async(input);
      display_results_async(results_future);
    }
  } while (!input.empty());
}

int main(int argc, char **argv) {
  std::string host = "saurabhsingh.database.windows.net";
  uint16_t port = 1433;
  std::string user = "saurabh";
  std::string password = "password";
  std::string catalog = "drivers";

  std::string input;
  std::cout << "Enter sync to use synchronous mode or async to use async mode "
               "(default: async): ";
  getline(std::cin, input);
  if (input == "sync") {
    run_sync(host, port, user, password, catalog);
  } else {
    run_async(host, port, user, password, catalog);
  }

  return 0;
}
