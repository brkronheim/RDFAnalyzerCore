#ifndef RDFANALYZER_XRD_OPEN_TRACE_H
#define RDFANALYZER_XRD_OPEN_TRACE_H

#include <XrdCl/XrdClFile.hh>
#include <XrdCl/XrdClURL.hh>
#include <XProtocol/XProtocol.hh>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

namespace rdf::xrd::detail {

// Keep native redirects/recovery. XrdCl supplies HostList at completion,
// including failures; it does not expose a public callback before each hop.
struct OpenTrace {
  std::string origin, file, candidate, role;
  std::uint64_t handle = 0, attempt = 0;
  std::function<void(const std::string &)> log;
  std::function<std::string(const std::string &)> site;
  std::chrono::steady_clock::time_point started = std::chrono::steady_clock::now();
  std::string operation = "open";

  static std::string endpoint(const XrdCl::URL &url) {
    auto host = url.GetHostName();
    if (host.empty()) return "unknown";
    if (host.find(':') != std::string::npos && host.front() != '[') host = '[' + host + ']';
    return host + ':' + std::to_string(url.GetPort());
  }

  std::string prefix(const char *event) const {
    return std::string("[XrdAdaptor] ") + event + " origin=" + origin +
           " handle=" + std::to_string(handle) + " attempt=" + std::to_string(attempt) +
           " role=" + role + " file=" + file + " epoch=" + std::to_string(std::time(nullptr));
  }
  void begin() const {
    const XrdCl::URL url(candidate);
    const auto server = endpoint(url);
    log(prefix((operation + "-start").c_str()) + " server=" + (server.empty() ? "unknown" : server) +
        " site=" + site(url.GetHostName()) + " endpoint_role=unresolved");
  }
  void result(XrdCl::File &opened, const XrdCl::XRootDStatus &status,
              const XrdCl::HostList *hosts, bool resolveServer = true) const {
    std::string endpointRole = !resolveServer && operation == "stat" ? "storage" : "unresolved";
    auto server = endpoint(XrdCl::URL(candidate));
    std::string host = XrdCl::URL(candidate).GetHostName();
    if (hosts) {
      std::size_t hop = 0;
      for (const auto &entry : *hosts) {
        server = endpoint(entry.url); host = entry.url.GetHostName();
        endpointRole = entry.flags & kXR_isManager ? "redirector" :
                       entry.flags & kXR_isServer ? "storage" : "unresolved";
        log(prefix("open-hop") + " hop=" + std::to_string(hop++) +
            " server=" + server + " site=" + site(host) + " endpoint_role=" + endpointRole);
      }
    }
    if (resolveServer && (status.IsOK() || operation == "stat")) {
      std::string dataServer;
      if (opened.GetProperty("DataServer", dataServer) && !dataServer.empty()) {
        const XrdCl::URL dataUrl("root://" + dataServer + "//");
        server = endpoint(dataUrl);
        host = dataUrl.GetHostName();
        endpointRole = "storage";
      }
    }
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    log(prefix((operation + "-result").c_str()) + " server=" + (server.empty() ? "unknown" : server) +
        " site=" + site(host) + " endpoint_role=" + endpointRole +
        " status=" + (status.IsOK() ? "opened" : "failed") +
        " elapsed_s=" + std::to_string(elapsed) + " detail=" + status.ToStr());
  }
};

class TracedOpenHandler final : public XrdCl::ResponseHandler {
public:
  struct State {
    std::mutex mutex;
    std::condition_variable condition;
    bool done = false;
    XrdCl::XRootDStatus status;
  };
  TracedOpenHandler(std::shared_ptr<State> state, XrdCl::File &file, OpenTrace trace)
      : state_(std::move(state)), file_(file), trace_(std::move(trace)) {}
  void HandleResponse(XrdCl::XRootDStatus *status, XrdCl::AnyObject *response) override {
    HandleResponseWithHosts(status, response, nullptr);
  }
  void HandleResponseWithHosts(XrdCl::XRootDStatus *status, XrdCl::AnyObject *response,
                               XrdCl::HostList *hosts) override {
    std::unique_ptr<XrdCl::XRootDStatus> ownedStatus(status);
    std::unique_ptr<XrdCl::AnyObject> ownedResponse(response);
    std::unique_ptr<XrdCl::HostList> ownedHosts(hosts);
    const XrdCl::XRootDStatus result = status ? *status : XrdCl::XRootDStatus(XrdCl::stError, XrdCl::errInternal);
    trace_.result(file_, result, hosts);
    auto state = state_;
    delete this;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->status = result;
    state->done = true;
    state->condition.notify_one();
  }
private:
  std::shared_ptr<State> state_;
  XrdCl::File &file_;
  OpenTrace trace_;
};

inline XrdCl::XRootDStatus tracedOpen(XrdCl::File &file, const OpenTrace &trace,
                                     unsigned timeout) {
  trace.begin();
  auto state = std::make_shared<TracedOpenHandler::State>();
  auto *handler = new TracedOpenHandler(state, file, trace);
  const auto queued = file.Open(trace.candidate, XrdCl::OpenFlags::Read,
                                XrdCl::Access::None, handler, timeout);
  if (!queued.IsOK()) {
    delete handler;
    trace.result(file, queued, nullptr);
    return queued;
  }
  std::unique_lock<std::mutex> lock(state->mutex);
  state->condition.wait(lock, [&] { return state->done; });
  return state->status;
}

} // namespace rdf::xrd::detail
#endif
