#pragma once

#include <api/IAnalysisService.h>
#include <api/IDataFrameProvider.h>
#include <functional>
#include <mutex>
#include <unordered_map>

namespace rdf::detail {

// A graph-construction hook for provider filters. Store it out of line to
// preserve the layouts and virtual tables of installed providers and Analyzer.
// Its lifetime follows the owning Analyzer's service list.
class PreFilterRegistration final : public IAnalysisService {
public:
    PreFilterRegistration(IDataFrameProvider& provider, std::function<void()> book)
        : provider_(provider), book_(std::move(book)) {
        auto& registry = registrations();
        std::lock_guard<std::mutex> lock(registry.mutex);
        registry.entries.emplace(&provider_, this);
    }
    ~PreFilterRegistration() override {
        auto& registry = registrations();
        std::lock_guard<std::mutex> lock(registry.mutex);
        registry.entries.erase(&provider_);
    }

    static void notify(IDataFrameProvider& provider) {
        PreFilterRegistration* registration = nullptr;
        auto& registry = registrations();
        {
            std::lock_guard<std::mutex> lock(registry.mutex);
            auto found = registry.entries.find(&provider);
            if (found != registry.entries.end()) registration = found->second;
        }
        // Booking can call user code. Each graph has one authoring owner.
        if (registration) registration->book_();
    }

    void initialize(ManagerContext&) override {}
    void finalize(ROOT::RDF::RNode&) override {}

private:
    struct Registry {
        std::mutex mutex;
        std::unordered_map<IDataFrameProvider*, PreFilterRegistration*> entries;
    };
    static Registry& registrations() {
        static Registry instance;
        return instance;
    }
    IDataFrameProvider& provider_;
    std::function<void()> book_;
};

} // namespace rdf::detail
