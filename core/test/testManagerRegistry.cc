#include <api/IPluggableManager.h>
#include <cassert>
#include <iostream>
#include <string>

/**
 * @brief Dummy manager for plugin system test
 */
class DummyManager : public IPluggableManager {
public:
    DummyManager() = default;
    std::string type() const override { return "DummyManager"; }

    void setContext(ManagerContext& ctx) override {
        // Dummy implementation
    }

    void setupFromConfigFile() override {
        // Dummy implementation: No action needed for testing purposes.
    }
};

void testManagerInterface() {
    DummyManager mgr;
    assert(mgr.type() == "DummyManager");
    std::cout << "DummyManager type() test passed.\n";
}

int main() {
    testManagerInterface();
    std::cout << "All ManagerRegistry tests passed.\n";
    return 0;
}
