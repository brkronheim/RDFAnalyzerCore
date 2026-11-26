// Public RunResult contract fixture. Standalone C++17; no ROOT/event loop.
#include <RunResult.h>
#include <iostream>
#include <iomanip>
#include <limits>
#include <array>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
using namespace rdfauthoring;
struct LazyHandle { operator bool() const { throw std::logic_error("must not be consumed"); } };
struct StringHandle { operator std::string() const { throw std::logic_error("must not be consumed"); } };
struct PointerHandle { operator const char*() const { throw std::logic_error("must not be consumed"); } };
struct NullHandle { operator std::nullptr_t() const { throw std::logic_error("must not be consumed"); } };
struct IntegerHandle { operator std::int64_t() const { throw std::logic_error("must not be consumed"); } };
struct FloatHandle { operator double() const { throw std::logic_error("must not be consumed"); } };
struct ArrayHandle { operator Json::Array() const { throw std::logic_error("must not be consumed"); } };
struct ObjectHandle { operator Json::Object() const { throw std::logic_error("must not be consumed"); } };
template <class T> constexpr bool blocked_handle =
    !std::is_constructible_v<Json,T> && !std::is_assignable_v<Json&,T> &&
    !std::is_constructible_v<EventLoopCount,T> && !std::is_assignable_v<EventLoopCount&,T>;
static_assert(blocked_handle<LazyHandle> && blocked_handle<StringHandle> && blocked_handle<PointerHandle> &&
              blocked_handle<NullHandle> && blocked_handle<IntegerHandle> && blocked_handle<FloatHandle> &&
              blocked_handle<ArrayHandle> && blocked_handle<ObjectHandle>,"native handles must not convert implicitly");
static_assert(!std::is_constructible_v<EventLoopCount,bool> && !std::is_assignable_v<EventLoopCount&,bool>);
static_assert(!std::is_constructible_v<EventLoopCount,double> && !std::is_assignable_v<EventLoopCount&,double>);
static_assert(std::is_constructible_v<Json,Json&> && std::is_assignable_v<Json&,Json&>);
static_assert(std::is_constructible_v<EventLoopCount,EventLoopCount&> &&
              std::is_assignable_v<EventLoopCount&,EventLoopCount&>);
struct Comma final:std::numpunct<char> {
  char do_decimal_point() const override { return ','; }
  char do_thousands_sep() const override { return '_'; }
  std::string do_grouping() const override { return "\3"; }
};
namespace {
std::string render(const std::string& mode) {
    RunResult result("compiled-cpp",Status::Succeeded,"caller-supplied summaries; paths are references only");
    result.counts={{"input",std::numeric_limits<std::uint64_t>::max()},{"selected",42}};
    result.timings={{"execute_s",1.25},{"setup_s",0}};
    result.outputs={{"metadata","/missing path/metadata.root"}};
    result.sums={{"weight",-2.5},{"negative_zero",-0.0}};
    result.histograms={{"example",Json::Object{{"contents",Json::Array{2.,-2.,0.}},{"sumw2",Json::Array{4.,4.,2.}}}}};
    result.details={{"escape",std::string("quote\"\\\n\t\0",10)},{"unicode",u8"μ jets"},{"unknown",nullptr},
                    {"nested",Json::Array{true,false,Json::Object{{"child",7}}}}};
    std::string controls;
    for (int c=0;c<32;++c) controls.push_back(static_cast<char>(c));
    result.details["controls"]=controls;
    std::string source="owned";
    result.details["owned_string"]=Json(source);source="mutated";
    Json::Object object{{"child",7}};
    result.details["owned_object"]=Json(object);object["child"]=99;
    result.provenance={{"scope","analysis owned"}};
    result.issues={{{"code","example"},{"message","not a certification"}}};
    if (mode=="failed") result.status=Status::Failed;
    else if (mode=="timed_out") result.status=Status::TimedOut;
    else if (mode=="cancelled") result.status=Status::Cancelled;
    else if (mode=="known-loops") result.event_loops=1;
    else if (mode=="max-loops") result.event_loops=std::numeric_limits<std::int64_t>::max();
    else if (mode=="reset-loops") { result.event_loops=4;result.event_loops=std::nullopt; }
    else if (mode=="formatted-writer") {
      Json value(Json::Object{{"integer",1234567},{"unsigned",std::numeric_limits<std::uint64_t>::max()},
          {"negative",-42},{"float",1.25},{"bool",true},{"null",nullptr},{"control","\n"}});
      std::ostringstream out;
      out.imbue(std::locale(std::locale::classic(),new Comma));
      out<<std::hex<<std::showbase<<std::showpos<<std::boolalpha<<std::setfill('*')<<std::setw(30);
      value.write(out);return out.str();
    }
    else if (mode=="empty") {
      RunResult empty("compiled-cpp",Status::Succeeded,"description only");empty.event_loops=0;return empty.to_json();
    } else if (mode=="negative-count") result.counts["bad"]=-1;
    else if (mode=="boolean-count") result.counts["bad"]=true;
    else if (mode=="negative-time") result.timings["bad"]=-.25;
    else if (mode=="boolean-time") result.timings["bad"]=false;
    else if (mode=="negative-loops") result.event_loops=-1;
    else if (mode=="overflow-loops") result.event_loops=std::numeric_limits<std::uint64_t>::max();
    else if (mode=="empty-path") result.outputs["bad"]="";
    else if (mode=="null-string") result.details["bad"]=Json(static_cast<const char*>(nullptr));
    else if (mode=="nan") result.details["bad"]=std::numeric_limits<double>::quiet_NaN();
    else if (mode=="infinity") result.histograms["bad"]=std::numeric_limits<double>::infinity();
    else if (mode=="bad-status") result.status=static_cast<Status>(99);
    else if (mode=="blank-backend") result.backend=u8" \t ";
    else if (mode=="blank-scope") result.completion_scope="\x1c";
    else if (mode=="bad-utf8") result.details["bad"]=std::string("\xff");
    else if (mode=="surrogate") result.details["bad"]=std::string("\xed\xa0\x80");
    else if (mode=="truncated-utf8") result.details["bad"]=std::string("\xe2\x82");
    else if (mode=="above-unicode") result.details["bad"]=std::string("\xf4\x90\x80\x80");
    else if (mode=="overlong-utf8") result.details["bad"]=std::string("\xc0\xaf");
    else if (mode=="bad-utf8-key") result.details[std::string("\xff")]=0;
    else if (mode!="succeeded") throw std::logic_error("unknown probe mode");
    std::locale::global(std::locale(std::locale::classic(),new Comma));
    return result.to_json();
}

// These are public contract cases rather than copies of validation logic. The
// independent Python receiver checks full values and types from all8 envelopes.
constexpr std::array<const char*,9> valid_modes{{
    "succeeded","failed","timed_out","cancelled","known-loops","empty",
    "max-loops","reset-loops","formatted-writer"}};
constexpr std::array<const char*,19> rejected_modes{{
    "negative-count","boolean-count","negative-time","boolean-time","negative-loops","empty-path",
    "nan","infinity","bad-status","blank-backend","blank-scope","bad-utf8","surrogate","truncated-utf8",
    "above-unicode","overlong-utf8","overflow-loops","null-string","bad-utf8-key"}};

void selfTest() {
  for (const auto* mode:valid_modes)
    if (render(mode).empty()) throw std::runtime_error(std::string("Empty valid result: ")+mode);
  for (const auto* mode:rejected_modes) {
    try { render(mode); }
    catch (const std::invalid_argument&) { continue; }
    throw std::runtime_error(std::string("Invalid result accepted: ")+mode);
  }
}
} // namespace

int main(int argc,char** argv) {
  try {
    const std::string mode=argc>1?argv[1]:"self-test";
    if (mode=="self-test") selfTest();
    else {
      try { std::cout<<render(mode); }
      catch (const std::invalid_argument&) { std::cout<<"rejected"; }
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr<<error.what()<<'\n';return 1;
  }
}
