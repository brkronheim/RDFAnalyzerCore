#ifndef RDFANALYZER_XRD_TFILE_H
#define RDFANALYZER_XRD_TFILE_H

#include "XrdSourceManager.h"

#include <TFile.h>

#include <memory>
#include <string>

class RDFXrdFile final : public TFile {
public:
  RDFXrdFile(const char *path, Option_t *option, const char *title,
             Int_t compress, Int_t netopt);
  ~RDFXrdFile() override;

  Bool_t ReadBuffer(char *buffer, Int_t length) override;
  Bool_t ReadBuffer(char *buffer, Long64_t position, Int_t length) override;
  Bool_t ReadBufferAsync(Long64_t position, Int_t length) override;
  Bool_t ReadBuffers(char *buffer, Long64_t *positions, Int_t *lengths,
                     Int_t count) override;
  Long64_t GetSize() const override;
  Bool_t IsOpen() const override;
  void Close(Option_t *option = "") override;

protected:
  Int_t SysOpen(const char *path, Int_t flags, UInt_t mode) override;
  Int_t SysClose(Int_t fd) override;
  Int_t SysRead(Int_t fd, void *buffer, Int_t length) override;
  Long64_t SysSeek(Int_t fd, Long64_t offset, Int_t whence) override;
  Int_t SysStat(Int_t fd, Long_t *id, Long64_t *size, Long_t *flags,
                Long_t *modtime) override;

private:
  std::shared_ptr<rdf::xrd::XrdSourceManager> sourceManager_; //!
  Long64_t position_ = 0; //!
  bool closed_ = false; //!

  ClassDefOverride(RDFXrdFile, 1);
};

#endif
