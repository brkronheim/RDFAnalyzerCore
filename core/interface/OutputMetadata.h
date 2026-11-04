#ifndef RDF_OUTPUT_METADATA_H
#define RDF_OUTPUT_METADATA_H

#include <cmath>
#include <cstdint>
#include <iomanip>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>

namespace rdf {
inline constexpr const char* jobMetadataKey = "__rdf_job_metadata_v1";

// Sufficient statistics, not a histogram whose bins can all be added.
struct WeightAuditMetadata {
    std::uint64_t count = 0, negativeCount = 0, zeroCount = 0;
    double sum = 0., minimum = 0., maximum = 0.;
};

struct OutputMetadata {
    std::map<std::string, std::string> regions;
    std::map<std::string, double> normalizations;
    std::map<std::string, WeightAuditMetadata> weightAudits;

    void merge(const OutputMetadata& other) {
        auto append = [](auto& target, const auto& source) {
            for (const auto& [key, value] : source)
                if (!target.emplace(key, value).second)
                    throw std::runtime_error("Duplicate output metadata key: " + key);
        };
        append(regions, other.regions);
        append(normalizations, other.normalizations);
        append(weightAudits, other.weightAudits);
    }
};

inline std::string metadataJsonString(const std::string& value) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : value) {
        switch (c) {
        case '"': out << "\\\""; break;
        case '\\': out << "\\\\"; break;
        case '\n': out << "\\n"; break;
        case '\r': out << "\\r"; break;
        case '\t': out << "\\t"; break;
        default:
            if (c < 0x20) out << "\\u" << std::hex << std::setw(4)
                             << std::setfill('0') << unsigned(c) << std::dec;
            else out << c;
        }
    }
    out << '"';
    return out.str();
}

inline std::string metadataJsonNumber(double value) {
    if (!std::isfinite(value))
        throw std::runtime_error("Nonfinite numeric output metadata");
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out << std::setprecision(17) << value;
    return out.str();
}

template<class Map>
std::string metadataJsonMap(const Map& entries) {
    std::ostringstream out;
    out << '{';
    bool first = true;
    for (const auto& [key, value] : entries) {
        if (!first) out << ',';
        first = false;
        out << metadataJsonString(key) << ':' << metadataJsonString(value);
    }
    out << '}';
    return out.str();
}
} // namespace rdf
#endif
