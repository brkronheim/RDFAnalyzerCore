#include <ManagerRegistry.h>
#include <stdexcept>

ManagerRegistry& ManagerRegistry::instance() {
    static ManagerRegistry instance;
    return instance;
}

void ManagerRegistry::registerFactory(const std::string& typeName, FactoryFunction factory) {
    factories_m[typeName] = std::move(factory);
}

std::unique_ptr<IPluggableManager> ManagerRegistry::create(const std::string& typeName, std::vector<void*> args) const {
    auto it = factories_m.find(typeName);
    if (it != factories_m.end()) {
        return it->second(args);
    }
    throw std::runtime_error("Manager type not registered: " + typeName);
} 