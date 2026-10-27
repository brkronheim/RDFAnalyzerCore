#ifndef RDFANALYZER_XRD_OPEN_RETRY_H
#define RDFANALYZER_XRD_OPEN_RETRY_H

#include <XrdCl/XrdClURL.hh>

#include <chrono>
#include <string>
#include <vector>

namespace rdf::xrd::detail {

// Only public CMS dataset paths have a known federation fallback. Other
// namespaces retain their original endpoint (including private/user storage).
inline std::vector<std::string> initialOpenUrls(const std::string &url) {
  std::vector<std::string> urls{url};
  XrdCl::URL parsed(url);
  auto path = parsed.GetPath();
  const std::string marker = "/store/test/xrootd/";
  const auto markerAt = path.find(marker);
  if (markerAt != std::string::npos) {
    const auto siteEnd = path.find('/', markerAt + marker.size());
    if (siteEnd != std::string::npos) path = path.substr(siteEnd);
  }
  const auto first = path.find_first_not_of('/');
  path = first == std::string::npos ? "/" : "/" + path.substr(first);
  const bool publicCMS = path.rfind("/store/mc/", 0) == 0 ||
                         path.rfind("/store/data/", 0) == 0 ||
                         path.rfind("/store/relval/", 0) == 0 ||
                         path.rfind("/store/himc/", 0) == 0;
  if (parsed.IsValid() && publicCMS && parsed.GetProtocol() == "root") {
    for (const auto *host : {"cms-xrd-global.cern.ch", "cmsxrootd.fnal.gov",
                             "xrootd-cms.infn.it"}) {
      if (parsed.GetHostName() == host) continue;
      auto fallback = parsed;
      fallback.SetHostPort(host, 1094);
      fallback.SetPath(path);
      auto params = fallback.GetParams();
      params.erase("tried");
      params.erase("triedrc");
      fallback.SetParams(params);
      urls.push_back(fallback.GetURL());
      if (urls.size() == 3) break;
    }
  }
  while (urls.size() < 3) urls.push_back(url);
  return urls;
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
