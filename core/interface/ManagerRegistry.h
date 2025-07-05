#ifndef MANAGERREGISTRY_H_INCLUDED
#define MANAGERREGISTRY_H_INCLUDED

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <api/IPluggableManager.h>

/**
 * @brief Registry for pluggable manager types.
 *
 * Allows registration and creation of manager types by string name, supporting
 * arbitrary constructor arguments.
 */
class ManagerRegistry {
public:
    using FactoryFunction = std::function<std::unique_ptr<IPluggableManager>(std::vector<void*>)>;

    /**
     * @brief Get the singleton instance of the registry.
     */
    static ManagerRegistry& instance();

    /**
     * @brief Register a manager factory function with a type name.
     * @param typeName The string type name.
     * @param factory The factory function.
     */
    void registerFactory(const std::string& typeName, FactoryFunction factory);

    /**
     * @brief Create a manager by type name, forwarding constructor arguments.
     * @param typeName The string type name.
     * @param args Constructor arguments as void* pointers (must match factory signature).
     * @return Unique pointer to the created manager, or nullptr if not found.
     */
    std::unique_ptr<IPluggableManager> create(const std::string& typeName, std::vector<void*> args) const;

private:
    std::unordered_map<std::string, FactoryFunction> factories_m;
    ManagerRegistry() = default;
    ManagerRegistry(const ManagerRegistry&) = delete;
    ManagerRegistry& operator=(const ManagerRegistry&) = delete;
};

/**
 * @brief Macro to register a manager type with the registry.
 *
 * Usage: REGISTER_MANAGER_TYPE(MyManagerType, ArgType1, ArgType2, ...)
 */
#define REGISTER_MANAGER_TYPE(TYPE, ...) \
    namespace { \
    template <typename... Args, size_t... Is> \
    std::unique_ptr<IPluggableManager> make_##TYPE##_from_voids(std::vector<void*>& args, std::index_sequence<Is...>) { \
        return std::make_unique<TYPE>(*(reinterpret_cast<typename std::tuple_element<Is, std::tuple<__VA_ARGS__>>::type*>(args[Is]))...); \
    } \
    struct TYPE##Registrar { \
        TYPE##Registrar() { \
            ManagerRegistry::instance().registerFactory(#TYPE, \
                [](std::vector<void*> args) -> std::unique_ptr<IPluggableManager> { \
                    return make_##TYPE##_from_voids<__VA_ARGS__>(args, std::index_sequence_for<__VA_ARGS__>{}); \
                }); \
        } \
    }; \
    static TYPE##Registrar global_##TYPE##Registrar; \
    }

#endif // MANAGERREGISTRY_H_INCLUDED 