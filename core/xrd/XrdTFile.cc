#include "XrdTFile.h"

#include <TPluginManager.h>
#include <TROOT.h>
#include <TSystem.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <vector>

ClassImp(RDFXrdFile);

RDFXrdFile::RDFXrdFile(const char *path, Option_t *option, const char *title,
                       Int_t compress, Int_t /*netopt*/)
    : TFile(path, "NET", title, compress) {
  if (!path || !*path) {
    MakeZombie();
    gDirectory = gROOT;
    throw std::runtime_error("RDFXrdFile received an empty URL");
  }
  TString mode(option ? option : "READ");
  mode.ToUpper();
  const bool readWithoutGlobalRegistration =
      mode == "READ_WITHOUT_GLOBALREGISTRATION";
  if (mode != "READ" && mode != "READWRAP" &&
      !readWithoutGlobalRegistration) {
    MakeZombie();
    gDirectory = gROOT;
    throw std::runtime_error("RDFXrdFile is read-only; requested mode '" +
                             std::string(mode.Data()) + "'");
  }

  try {
    sourceManager_ = rdf::xrd::makeXrdSourceManager(path);
    fRealName = path;
    // Keep ROOT's internal suffix through Init(): it removes the suffix and
    // disables global TFile registration for this open.
    fOption = readWithoutGlobalRegistration ? mode : "READ";
    fWritable = kFALSE;
    fD = 0; // XrdCl owns the transport; ROOT only needs an open-file marker.
    Init(kFALSE);
  } catch (...) {
    MakeZombie();
    gDirectory = gROOT;
    throw;
  }
}

RDFXrdFile::~RDFXrdFile() { Close(); }

Bool_t RDFXrdFile::ReadBuffer(char *buffer, Int_t length) {
  return ReadBuffer(buffer, position_, length);
}

Bool_t RDFXrdFile::ReadBuffer(char *buffer, Long64_t position, Int_t length) {
  if (!sourceManager_ || closed_ || IsZombie() || length < 0 || position < 0 ||
      (!buffer && length)) {
    Error("ReadBuffer", "invalid read (position=%lld, length=%d)", position,
          length);
    return kTRUE;
  }
  if (length == 0) return kFALSE;
  const rdf::xrd::ReadRange range{static_cast<std::uint64_t>(position),
                                  static_cast<std::uint32_t>(length), buffer};
  if (!sourceManager_->readv({range}, false)) {
    Error("ReadBuffer", "XrdCl read failed at offset %lld for %d bytes",
          position, length);
    return kTRUE;
  }
  position_ = position + length;
  fOffset = position_;
  fBytesRead += length;
  ++fReadCalls;
  return kFALSE;
}

Bool_t RDFXrdFile::ReadBufferAsync(Long64_t position, Int_t length) {
  if (!sourceManager_ || closed_ || IsZombie() || position < 0 || length < 0)
    return kTRUE;
  if (length == 0) return kFALSE;
  const rdf::xrd::ReadRange range{static_cast<std::uint64_t>(position),
                                  static_cast<std::uint32_t>(length), nullptr};
  if (!sourceManager_->readv({range}, true)) {
    Error("ReadBufferAsync", "XrdCl prefetch failed at offset %lld for %d bytes",
          position, length);
    return kTRUE;
  }
  fBytesReadExtra += length;
  return kFALSE;
}

Bool_t RDFXrdFile::ReadBuffers(char *buffer, Long64_t *positions,
                               Int_t *lengths, Int_t count) {
  if (!sourceManager_ || closed_ || IsZombie() || count < 0 ||
      (count && (!positions || !lengths))) {
    Error("ReadBuffers", "invalid vector read request");
    return kTRUE;
  }
  std::vector<rdf::xrd::ReadRange> ranges;
  ranges.reserve(static_cast<std::size_t>(count));
  Long64_t total = 0;
  for (Int_t i = 0; i < count; ++i) {
    if (positions[i] < 0 || lengths[i] < 0 ||
        total > std::numeric_limits<Long64_t>::max() - lengths[i]) {
      Error("ReadBuffers", "invalid range at index %d", i);
      return kTRUE;
    }
    auto *target = buffer ? buffer + total : nullptr;
    ranges.push_back({static_cast<std::uint64_t>(positions[i]),
                      static_cast<std::uint32_t>(lengths[i]), target});
    total += lengths[i];
  }
  const bool prefetch = buffer == nullptr;
  if (!sourceManager_->readv(ranges, prefetch)) {
    Error("ReadBuffers", "XrdCl %s failed for %d ranges",
          prefetch ? "prefetch" : "vector read", count);
    return kTRUE;
  }
  if (prefetch) fBytesReadExtra += total;
  else {
    fBytesRead += total;
    ++fReadCalls;
  }
  return kFALSE;
}

Long64_t RDFXrdFile::GetSize() const {
  return sourceManager_ ? static_cast<Long64_t>(sourceManager_->size()) : 0;
}

Bool_t RDFXrdFile::IsOpen() const {
  return sourceManager_ && !closed_ && !IsZombie();
}

void RDFXrdFile::Close(Option_t *option) {
  if (closed_) return;
  if (sourceManager_) sourceManager_->stop();
  TFile::Close(option);
  closed_ = true;
}

Int_t RDFXrdFile::SysOpen(const char *, Int_t, UInt_t) { return 0; }

Int_t RDFXrdFile::SysClose(Int_t) { return 0; }

Int_t RDFXrdFile::SysRead(Int_t, void *buffer, Int_t length) {
  if (!sourceManager_ || closed_ || length < 0 || (!buffer && length)) return -1;
  if (length == 0) return 0;
  const rdf::xrd::ReadRange range{static_cast<std::uint64_t>(position_),
                                  static_cast<std::uint32_t>(length),
                                  static_cast<char *>(buffer)};
  if (!sourceManager_->readv({range}, false)) return -1;
  position_ += length;
  fOffset = position_;
  fBytesRead += length;
  ++fReadCalls;
  return length;
}

Long64_t RDFXrdFile::SysSeek(Int_t, Long64_t offset, Int_t whence) {
  if (!sourceManager_ || closed_) return -1;
  Long64_t target = offset;
  if (whence == SEEK_CUR) target += position_;
  else if (whence == SEEK_END) target += GetSize();
  else if (whence != SEEK_SET) return -1;
  if (target < 0) return -1;
  position_ = target;
  return position_;
}

Int_t RDFXrdFile::SysStat(Int_t, Long_t *id, Long64_t *size, Long_t *flags,
                          Long_t *modtime) {
  if (!sourceManager_) return 1;
  if (id) *id = static_cast<Long_t>(std::hash<std::string>{}(sourceManager_->url()));
  if (size) *size = GetSize();
  if (flags) *flags = 0;
  if (modtime) *modtime = 0;
  return 0;
}

namespace rdf::xrd {
void registerXrdAdaptor() {
  static std::once_flag registered;
  std::call_once(registered, [] {
    if (!gROOT || !gROOT->GetPluginManager())
      throw std::runtime_error("ROOT's plugin manager is unavailable");
    gROOT->GetPluginManager()->AddHandler(
        "TFile", "^root:", "RDFXrdFile", "RDFXrdAdaptor",
        "RDFXrdFile(char const*,Option_t*,char const*,Int_t,Int_t)");
  });
}
} // namespace rdf::xrd
