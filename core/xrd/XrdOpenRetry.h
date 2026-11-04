#ifndef RDFANALYZER_XRD_OPEN_RETRY_H
#define RDFANALYZER_XRD_OPEN_RETRY_H

#include <XrdCl/XrdClURL.hh>

#include <chrono>
#include <string>
#include <vector>

#ifdef RDF_XRD_OPEN_POLICY_HEADER
#include RDF_XRD_OPEN_POLICY_HEADER
#endif

namespace rdf::xrd::detail {

// An optional source extension can supply URL policy without changing retries.
inline std::vector<std::string> initialOpenUrls(const std::string &url) {
#ifdef RDF_XRD_OPEN_POLICY_HEADER
  return extensionInitialOpenUrls(url);
#else
  return {url, url, url};
#endif
}

// The callbacks keep transport ownership and status reporting in the caller,
// and allow the bounded retry behavior to be tested without network access.
template <typename TryOpen, typename Wait>
bool openWithRetry(const std::string &url, TryOpen tryOpen, Wait wait) {
  const auto urls = initialOpenUrls(url);
  for (std::size_t attempt = 0; attempt < urls.size(); ++attempt) {
    if (attempt) wait(std::chrono::seconds(1U << (attempt - 1)));
    if (tryOpen(urls[attempt], attempt + 1)) return true;
  }
  return false;
}

} // namespace rdf::xrd::detail

#endif
