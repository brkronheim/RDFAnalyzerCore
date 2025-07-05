#include <ManagerRegistry.h>
#include <api/IPluggableManager.h>
#include <cassert>
#include <iostream>
#include <string>

/**
 * @brief Dummy manager for plugin system test
 */
class DummyManager : public IPluggableManager {
public:
    DummyManager(int v) : value(v) {}
    std::string type() const override { return "DummyManager"; }
    int value;
};

// Register DummyManager
REGISTER_MANAGER_TYPE(DummyManager, int)

void testDummyManager() {
    int v = 42;
    std::vector<void*> args = {&v};
    auto mgr = ManagerRegistry::instance().create("DummyManager", args);
    assert(mgr);
    assert(mgr->type() == "DummyManager");
    assert(static_cast<DummyManager*>(mgr.get())->value == 42);
    std::cout << "DummyManager test passed.\n";
}

int main() {
    testDummyManager();
    // BDTManager test could be added here with a mock IConfigurationProvider if needed
    std::cout << "All ManagerRegistry tests passed.\n";
    return 0;
} 