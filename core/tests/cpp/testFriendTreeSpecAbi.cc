#include <util.h>

#include <new>
#include <type_traits>

// Old Linux ELF consumer objects can reference either constructor entry point.
// Refer to the symbols directly, so this test cannot emit its own inline copy.
extern "C" void constructComplete(void *) asm("_ZN14FriendTreeSpecC1Ev");
extern "C" void constructBase(void *) asm("_ZN14FriendTreeSpecC2Ev");

static_assert(std::is_aggregate_v<FriendTreeSpec>);

static bool checkDefaults(void (*construct)(void *)) {
  alignas(FriendTreeSpec) unsigned char storage[sizeof(FriendTreeSpec)];
  construct(storage);
  auto *spec = std::launder(reinterpret_cast<FriendTreeSpec *>(storage));
  const bool valid = spec->alias.empty() && spec->treeName == "Events" &&
                     spec->files.empty() && spec->directory.empty() &&
                     spec->globs == std::vector<std::string>{".root"} &&
                     spec->antiglobs == std::vector<std::string>{"FAIL"} &&
                     spec->indexBranches.empty();
  spec->~FriendTreeSpec();
  return valid;
}

int main() {
  return checkDefaults(constructComplete) && checkDefaults(constructBase) ? 0 : 1;
}
