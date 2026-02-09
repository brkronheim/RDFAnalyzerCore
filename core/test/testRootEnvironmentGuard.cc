#include <ConfigurationManager.h>
#include <DataManager.h>

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

namespace {
class EnvGuard {
public:
  EnvGuard(const char* key, const char* value) : key_(key) {
    const char* existing = std::getenv(key_);
    if (existing) {
      old_ = existing;
      hadOld_ = true;
    }
    if (value) {
      setenv(key_, value, 1);
    } else {
      unsetenv(key_);
    }
  }

  ~EnvGuard() {
    if (hadOld_) {
      setenv(key_, old_.c_str(), 1);
    } else {
      unsetenv(key_);
    }
  }

private:
  const char* key_;
  std::string old_;
  bool hadOld_ = false;
};
} // namespace

TEST(RootEnvironmentGuard, ThrowsOnMismatchedRootsys) {
  EnvGuard guard("ROOTSYS", "/tmp/invalid-root");
  const std::string configPath = std::string(TEST_SOURCE_DIR) + "/cfg/test_data_config_minimal.txt";
  ConfigurationManager config(configPath);

  EXPECT_THROW({
    DataManager dm(config);
  }, std::runtime_error);
}
