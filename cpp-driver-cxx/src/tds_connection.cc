#include "tds_connection.h"

#include "cpp-driver-cxx/src/lib.rs.h"
#include "rust/cxx.h"

#include <iostream>
#include <memory>
#include <string>

namespace tds {
std::unique_ptr<TdsConnection>
TdsConnection::create_connection(std::string host, uint16_t port,
                                 std::string user, std::string password,
                                 std::string catalog) {
  auto context =
      cxx_ffi::create_client_context(host, port, user, password, catalog);
  auto inner = cxx_ffi::create_connection(*context);
  return std::unique_ptr<TdsConnection>(
      new TdsConnection(std::move(inner), std::move(context)));
}

::rust::Box<::cxx_ffi::QueryResultTypeStream>
TdsConnection::execute(const std::string &query_batch) {
  return _inner->execute(query_batch);
}

TdsConnection::TdsConnection(rust::Box<cxx_ffi::TdsConnection> &&inner,
                             rust::Box<cxx_ffi::ClientContext> &&context)
    : _inner(std::move(inner)), _context(std::move(context)) {}
} // namespace tds
