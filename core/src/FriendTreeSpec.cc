#include <util.h>

namespace rdf_config_detail {

FriendTreeSpec makeDefaultFriendTreeSpec() {
  // Default-initialization calls the implicit constructor. An empty aggregate
  // initializer would initialize the fields directly and omit the ABI symbols.
  FriendTreeSpec spec;
  return spec;
}

} // namespace rdf_config_detail
