#ifndef RDFANALYZER_RUN_RESULT_H
#define RDFANALYZER_RUN_RESULT_H
// Optional C++17 result values. No ROOT/Analyzer dependency or event execution.
// Summaries must be evaluated explicitly by the caller before they are copied here.
// Serialization records the supplied completion scope; paths remain references.
#include <cmath>
#include <charconv>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <locale>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace rdfauthoring {
namespace detail {
template <class F> void codepoints(const std::string& text, F function) {
  for (std::size_t i=0; i<text.size();) {
    const auto first=static_cast<unsigned char>(text[i]);
    unsigned width=1; std::uint32_t point=first, minimum=0;
    if (first>=0xc2 && first<=0xdf) { width=2;point=first&0x1f;minimum=0x80; }
    else if (first>=0xe0 && first<=0xef) { width=3;point=first&0xf;minimum=0x800; }
    else if (first>=0xf0 && first<=0xf4) { width=4;point=first&7;minimum=0x10000; }
    else if (first>=0x80) throw std::invalid_argument("result string is not valid UTF-8");
    if (i+width>text.size()) throw std::invalid_argument("truncated UTF-8 result string");
    for (unsigned j=1; j<width; ++j) {
      const auto next=static_cast<unsigned char>(text[i+j]);
      if ((next&0xc0)!=0x80) throw std::invalid_argument("invalid UTF-8 continuation");
      point=(point<<6)|(next&0x3f);
    }
    if (point<minimum || point>0x10ffff || (point>=0xd800 && point<=0xdfff))
      throw std::invalid_argument("invalid UTF-8 codepoint");
    function(point);i+=width;
  }
}
inline bool has_text(const std::string& text) {
  bool found=false;
  codepoints(text,[&](std::uint32_t c) {
    // Match Python str.strip's whitespace policy, including separators.
    const bool space=(c>=9 && c<=13)||(c>=28 && c<=32)||c==133||c==160||c==0x1680||
        (c>=0x2000 && c<=0x200a)||c==0x2028||c==0x2029||c==0x202f||c==0x205f||c==0x3000;
    found=found||!space;
  });
  return found;
}
inline void quote(std::ostream& out,const std::string& text) {
  codepoints(text,[](std::uint32_t) {});
  constexpr char hex[]="0123456789abcdef";
  out.put('"');
  for (unsigned char c:text) {
    if (c=='"' || c=='\\') { out.put('\\');out.put(static_cast<char>(c)); }
    else if (c<32) { out.write("\\u00",4);out.put(hex[c>>4]);out.put(hex[c&15]); }
    else out.put(static_cast<char>(c));
  }
  out.put('"');
}
} // namespace detail

class Json {
public:
  using Array=std::vector<Json>;
  using Object=std::map<std::string,Json>;
  using Value=std::variant<std::nullptr_t,bool,std::int64_t,std::uint64_t,double,std::string,Array,Object>;
  Json():value_(nullptr) {}
  Json(std::nullptr_t):value_(nullptr) {}
  template <class T,std::enable_if_t<std::is_same_v<T,bool>,int> =0>
  Json(T value):value_(value) {}
  template <class T,std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T,bool>,int> =0>
  Json(T value):value_(static_cast<std::conditional_t<std::is_signed_v<T>,std::int64_t,std::uint64_t>>(value)) {}
  template <class T,std::enable_if_t<std::is_floating_point_v<T>,int> =0>
  Json(T value):value_(static_cast<double>(value)) {
    if (!std::isfinite(static_cast<double>(value))) throw std::invalid_argument("nonfinite result number");
  }
  Json(std::string value):value_(std::move(value)) {}
  Json(const char* value):Json(value ? std::string(value) : throw std::invalid_argument("null string pointer")) {}
  Json(Array value):value_(std::move(value)) {}
  Json(Object value):value_(std::move(value)) {}
  // Exact source types only: a native handle must not run a conversion operator
  // while being turned into a supposedly detached value.
  template <class T,std::enable_if_t<
      !std::is_same_v<std::decay_t<T>,Json> &&
      !std::is_same_v<std::decay_t<T>,std::nullptr_t> &&
      !std::is_arithmetic_v<std::decay_t<T>> &&
      !std::is_same_v<std::decay_t<T>,std::string> &&
      !std::is_same_v<std::decay_t<T>,const char*> &&
      !std::is_same_v<std::decay_t<T>,char*> &&
      !std::is_same_v<std::decay_t<T>,Array> &&
      !std::is_same_v<std::decay_t<T>,Object>,int> =0>
  Json(T&&)=delete;
  const Value& value() const { return value_; }
  void write(std::ostream& out) const {
    std::visit([&](const auto& value) {
      using T=std::decay_t<decltype(value)>;
      if constexpr (std::is_same_v<T,std::nullptr_t>) out.write("null",4);
      else if constexpr (std::is_same_v<T,bool>) {
        if (value) out.write("true",4);else out.write("false",5);
      }
      else if constexpr (std::is_same_v<T,std::string>) detail::quote(out,value);
      else if constexpr (std::is_same_v<T,Array>) {
        out.put('[');bool first=true;
        for (const auto& item:value) { if (!first) out.put(',');first=false;item.write(out); }
        out.put(']');
      } else if constexpr (std::is_same_v<T,Object>) {
        out.put('{');bool first=true;
        for (const auto& item:value) {
          if (!first) out.put(',');
          first=false;detail::quote(out,item.first);out.put(':');item.second.write(out);
        }
        out.put('}');
      } else if constexpr (std::is_same_v<T,double>) {
        char buffer[64];
        const auto converted=std::to_chars(buffer,buffer+sizeof(buffer),value,
            std::chars_format::general,std::numeric_limits<double>::max_digits10);
        if (converted.ec!=std::errc{}) throw std::runtime_error("cannot serialize result number");
        std::string text(buffer,converted.ptr);
        if (text.find_first_of(".eE")==std::string::npos) text+=".0";
        out.write(text.data(),static_cast<std::streamsize>(text.size()));
      } else {
        char buffer[32];
        const auto converted=std::to_chars(buffer,buffer+sizeof(buffer),value);
        if (converted.ec!=std::errc{}) throw std::runtime_error("cannot serialize result integer");
        out.write(buffer,converted.ptr-buffer);
      }
    },value_);
  }
private:
  Value value_;
};

// Unknown, or an exact non-bool integral source in the signed64 count range.
// A separate value type prevents optional<int64_t>'s implicit bool/float/handle
// conversions before validation can observe the original source type.
class EventLoopCount {
public:
  EventLoopCount()=default;
  EventLoopCount(std::nullopt_t):value_(std::nullopt) {}
  template <class T,std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T,bool>,int> =0>
  EventLoopCount(T value) { assign(value); }
  template <class T,std::enable_if_t<
      !std::is_same_v<std::decay_t<T>,EventLoopCount> &&
      !std::is_same_v<std::decay_t<T>,std::nullopt_t> &&
      (!std::is_integral_v<std::decay_t<T>> || std::is_same_v<std::decay_t<T>,bool>),int> =0>
  EventLoopCount(T&&)=delete;
  template <class T,std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T,bool>,int> =0>
  EventLoopCount& operator=(T value) { assign(value);return *this; }
  EventLoopCount& operator=(std::nullopt_t) { value_.reset();return *this; }
  template <class T,std::enable_if_t<
      !std::is_same_v<std::decay_t<T>,EventLoopCount> &&
      !std::is_same_v<std::decay_t<T>,std::nullopt_t> &&
      (!std::is_integral_v<std::decay_t<T>> || std::is_same_v<std::decay_t<T>,bool>),int> =0>
  EventLoopCount& operator=(T&&)=delete;
  explicit operator bool() const { return value_.has_value(); }
  std::int64_t operator*() const { return value_.value(); }
private:
  template <class T> void assign(T value) {
    static_assert(sizeof(T)<=sizeof(std::uint64_t),"event-loop source exceeds supported integer width");
    if constexpr (std::is_signed_v<T>) {
      if (value<0) throw std::invalid_argument("negative event_loops");
    }
    if (static_cast<std::uint64_t>(value)>static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
      throw std::invalid_argument("event_loops exceeds signed64 range");
    value_=static_cast<std::int64_t>(value);
  }
  std::optional<std::int64_t> value_;
};

enum class Status { Succeeded, Failed, TimedOut, Cancelled };
inline std::string status_name(Status status) {
  switch (status) {
    case Status::Succeeded:return "succeeded";
    case Status::Failed:return "failed";
    case Status::TimedOut:return "timed_out";
    case Status::Cancelled:return "cancelled";
  }
  throw std::invalid_argument("invalid result status");
}

struct RunResult {
  std::string backend;
  Status status;
  std::string completion_scope;
  Json::Object counts,histograms,sums,timings,provenance,details;
  std::map<std::string,std::string> outputs;
  std::vector<Json::Object> issues;
  EventLoopCount event_loops;

  RunResult(std::string backend_,Status status_,std::string scope_)
      :backend(std::move(backend_)),status(status_),completion_scope(std::move(scope_)) { validate(); }
  std::string to_json() const {
    validate();
    Json::Object output_values;
    for (const auto& item:outputs) output_values.emplace(item.first,Json(item.second));
    Json::Array issue_values;
    for (const auto& item:issues) issue_values.emplace_back(item);
    Json::Object document{{"schema","rdf-run-result-v1"},{"backend",backend},{"status",status_name(status)},
        {"success",status==Status::Succeeded},{"completion_scope",completion_scope},
        {"counts",counts},{"histograms",histograms},{"sums",sums},{"timings",timings},
        {"outputs",output_values},{"provenance",provenance},{"details",details},{"issues",issue_values},
        {"event_loops",event_loops ? Json(*event_loops) : Json(nullptr)}};
    std::ostringstream out;out.imbue(std::locale::classic());Json(std::move(document)).write(out);return out.str();
  }
private:
  void validate() const {
    if (!detail::has_text(backend)||!detail::has_text(completion_scope))
      throw std::invalid_argument("backend and completion_scope require nonempty text");
    status_name(status);
    if (event_loops && *event_loops<0) throw std::invalid_argument("negative event_loops");
    for (const auto& item:counts) {
      const auto& value=item.second.value();
      if (const auto* number=std::get_if<std::int64_t>(&value)) {
        if (*number<0) throw std::invalid_argument("negative count");
      } else if (!std::holds_alternative<std::uint64_t>(value)) throw std::invalid_argument("count must be an integer");
    }
    for (const auto& item:timings) {
      const bool valid=std::visit([](const auto& value) {
        using T=std::decay_t<decltype(value)>;
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T,bool>) return value>=0;
        else return false;
      },item.second.value());
      if (!valid) throw std::invalid_argument("timing must be nonnegative seconds");
    }
    for (const auto& item:outputs) if (item.second.empty()) throw std::invalid_argument("empty output reference");
  }
};
} // namespace rdfauthoring
#endif
