#include <boost/histogram.hpp>
#include <memory>
#include <vector>
#include <THn.h>


class NDHistogramManager {
   using Storage = boost::histogram::storage_adaptor<boost::histogram::weight_storage>;

   // Placeholder type for prototype, actual type comes from make_histogram_with
   using Prototype = decltype(
       boost::histogram::make_histogram_with(
           Storage{},
           boost::histogram::axis::regular<>(10, 0.0, 1.0)  // placeholder
       )
   );
   std::vector<std::shared_ptr<Prototype>> fPerThreadResults;

   std::shared_ptr<THnF> fFinalResult;

public:
   using Result_t = THnF;

   template <typename... Axes>
   NDHistogramManager(unsigned int nSlots, Axes&&... axes)
       : fPerThreadResults(nSlots)
   {
       // Create per-thread Boost histograms
       for (unsigned int i = 0; i < nSlots; ++i) {
           fPerThreadResults[i] = std::make_shared<Prototype>(
               boost::histogram::make_histogram_with(Storage{}, std::forward<Axes>(axes)...)
           );
       }
   }

   template <typename... Args>
   void Fill(unsigned int slot, Args&&... values) {
       (*fPerThreadResults[slot])(std::forward<Args>(values)..., boost::histogram::weight(1.0));
   }

   void Finalize() {
       if (fPerThreadResults.empty()) return;

       // Use the first histogram as a prototype
       const auto& proto = *fPerThreadResults[0];

       unsigned int ndim = proto.rank();
       std::vector<int> nbins(ndim);
       std::vector<double> lows(ndim), highs(ndim);

       // Get axis info
       for (unsigned int i = 0; i < ndim; ++i) {
           const auto& ax = proto.axis(i);
           nbins[i] = ax.size();
           lows[i]  = ax.value(0);
           highs[i] = ax.value(ax.size());
       }

       // Create THnF
       fFinalResult = std::make_shared<THnF>(
           "hFinal", "Merged Histogram", ndim, nbins.data(), lows.data(), highs.data()
       );

       // Iterate over all bins and merge
       for (auto&& idx : boost::histogram::indexed(proto)) {
           double content = 0.0;
           double variance = 0.0;

           auto indices = idx.indices();

           // Sum across threads
           for (auto& hptr : fPerThreadResults) {
               auto&& bin = (*hptr)[indices];
               content  += bin.value();
               variance += bin.variance();
           }

           
           std::vector indexData(indices.begin(), indices.end());

           int binNumber = fFinalResult->GetBin(indexData.data());
           fFinalResult->SetBinContent(binNumber, content);
           fFinalResult->SetBinError(binNumber, std::sqrt(variance));
       }
   }

   std::shared_ptr<THnF> GetResultPtr() const { return fFinalResult; }
};
