#pragma once

#include "cpp-driver-cxx/src/lib.rs.h"
#include "rust/cxx.h"
#include <future>
#include <memory>
#include <string>

namespace tds {
class TdsConnection {
public:
  static std::unique_ptr<TdsConnection>
  create_connection(std::string host, uint16_t port, std::string user,
                    std::string password, std::string catalog);
  ::rust::Box<::cxx_ffi::QueryResultTypeStream>
  execute(const std::string &query_batch);

private:
  TdsConnection(rust::Box<cxx_ffi::TdsConnection> &&inner,
                rust::Box<cxx_ffi::ClientContext> &&context);
  rust::Box<cxx_ffi::TdsConnection> _inner;
  rust::Box<cxx_ffi::ClientContext> _context;
};
} // namespace tds
