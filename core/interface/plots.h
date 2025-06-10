#ifndef PLOTS_H_INCLUDED
#define PLOTS_H_INCLUDED


#include <ROOT/RDF/HistoModels.hxx>
#include <memory>
#include <vector>
#include <string>

#include <ROOT/RDataFrame.hxx>
#include <Math/MinimizerOptions.h>
#include <TCanvas.h>
#include <TPad.h>
#include <TFile.h>
#include <TLegend.h>
#include <TAxis.h>
#include <TRatioPlot.h>
#include <TH1F.h>
#include <TH1D.h>
#include <TH1.h>
#include <TH1I.h>
#include <TF1.h>
#include <THn.h>
#include <THnSparse.h>
#include <TGraphErrors.h>
#include <TLine.h>
#include <TVirtualFitter.h>
#include <TFitResult.h>
#include <TText.h>



#include <TGraph.h>
#include <TBox.h>
#include <TROOT.h>
#include <TStyle.h>

class THnMulti : public ROOT::Detail::RDF::RActionImpl<THnMulti> {
    
public:
    // We use a public type alias to advertise the type of the result of this action
    using Result_t = THnSparseD;

    THnMulti(unsigned int nSlots, std::string name, std::string title, Int_t dim, Int_t nFills, std::vector<Int_t> nbins, std::vector<Double_t> xmin, std::vector<Double_t> xmax) {
        nSlots_m = nSlots;
        dim_m = dim;
        nbins_m = nbins;
        xmin_m = xmin;
        xmax_m = xmax;
        nFills_m = nFills;
        for(unsigned int i = 0; i< nSlots; i++){
            
            fPerThreadResults.push_back(std::make_shared<THnSparseD>((name+"_"+std::to_string(i)).c_str(), title.c_str(), dim, nbins.data(), xmin.data(), xmax.data()));
            fPerThreadResults[i]->Sumw2();
        }
        fFinalResult = std::make_shared<Result_t>((name).c_str(), title.c_str(), dim, nbins.data(), xmin.data(), xmax.data());
        // Define all the histograms
    }

    THnMulti(THnMulti &&) = default;

    std::shared_ptr<Result_t> GetResultPtr(){
        return(fFinalResult);
    }



    // Called before the event loop to retrieve the address of the result that will be filled/generated.
    std::shared_ptr<THnSparseD> GetResultPtr() const { return fFinalResult; }

    // Called at the beginning of the event loop.
    void Initialize() {

    }

    // Called at the beginning of each processing task.
    void InitTask(TTreeReader *, int) {}

    /// Called at every entry.
    void Exec(unsigned int slot, ROOT::VecOps::RVec<Double_t> &val) {
        Double_t *array;
        Double_t weight;
        for(int i = 0; i<nFills_m; i++){
            array = val.data()+i*(dim_m+1);
            weight = array[dim_m];
            if(weight==0.0){
                continue;
            }

            fPerThreadResults[slot]->Fill(array, weight);   // Hits
        }
        
    }

    // Called at the end of the event loop.
    void Finalize()
    {
        for(auto hist :fPerThreadResults){
            //auto rawPtr = ;
            fFinalResult->Add(hist.get());
        }

    }

    // Called by RDataFrame to retrieve the name of this action.
    std::string GetActionName() const { return "THnMulti"; }

private:
    std::shared_ptr<THnSparseD> fFinalResult = std::make_shared<THnSparseD>();
    std::vector<std::shared_ptr<THnSparseD>> fPerThreadResults;
    unsigned int nSlots_m;
    Int_t dim_m;
    std::vector<Int_t> nbins_m;
    std::vector<Double_t> xmin_m;
    std::vector<Double_t> xmax_m;
    Int_t nFills_m;
    
};


// Store metadata on a hist
// Stores a name, variable, lable, weight, number of bins, lower bound, and upper bound
class histInfo {
    public:
        histInfo(const char name[], const char variable[], const char label[], const char weight[], int bins, float lowerBound, float upperBound):
            name_m(name),
            variable_m(variable),
            label_m(label),
            weight_m(weight),
            bins_m(bins),
            lowerBound_m(lowerBound),
            upperBound_m(upperBound){}

        constexpr const std::string &name() const{
            return(name_m);
        }

        constexpr const std::string &weight() const{
            return(weight_m);
        }

        constexpr const std::string &variable() const{
            return(variable_m);
        }

        constexpr const std::string &label() const{
            return(label_m);
        }

        constexpr const int &bins() const{
            return(bins_m);
        }

        constexpr const float &lowerBound() const{
            return(lowerBound_m);
        }

        constexpr const float &upperBound() const{
            return(upperBound_m);
        }


    private:
        const std::string name_m;
        const std::string variable_m;
        const std::string label_m;
        const std::string weight_m;
        const int bins_m;
        const float lowerBound_m;
        const float upperBound_m;
};

// Holds selection info, which is a subset of histInfo.
// The point of this is to hold a set of selections which will be applied to a predefined set of histograms.
// For example, there might be a selectionInfo with resolved cuts and one with boosted cuts, applied to the same histograms
class selectionInfo {
    public:
        selectionInfo(std::string variable, int bins, double lowerBound, double upperBound):
            variable_m(variable),
            bins_m(bins),
            lowerBound_m(lowerBound),
            upperBound_m(upperBound){}

        constexpr const std::string &variable() const{
            return(variable_m);
        }

        constexpr const int &bins() const{
            return(bins_m);
        }

        constexpr const float &lowerBound() const{
            return(lowerBound_m);
        }

        constexpr const float &upperBound() const{
            return(upperBound_m);
        }


    private:
        const std::string variable_m;
        const int bins_m;
        const float lowerBound_m;
        const float upperBound_m;
};

// Class to hold many hists
class histHolder {
    public:
        histHolder(){}

        template <typename... types>
        void bookND(std::vector<histInfo> &infos, std::vector<selectionInfo> &selection, ROOT::RDF::RNode df, std::string suffix){
            
            // Store the selection info in some vectors
            std::vector<int> binVectorBase;
            std::vector<double> lowerBoundVectorBase;
            std::vector<double> upperBoundVectorBase;
            std::vector<std::string> varVectorBase;

            for(auto const &selectionInfo : selection){
                binVectorBase.push_back(selectionInfo.bins());
                lowerBoundVectorBase.push_back(selectionInfo.lowerBound());
                upperBoundVectorBase.push_back(selectionInfo.upperBound());
                varVectorBase.push_back(selectionInfo.variable());
            }

            
            for(auto const &info : infos){
                std::string newName = info.name()+"."+suffix; // name of hist info
                std::vector<int> binVector(binVectorBase); // vectors from selectionInfos
                std::vector<double> lowerBoundVector(lowerBoundVectorBase);
                std::vector<double> upperBoundVector(upperBoundVectorBase);
                std::vector<std::string> varVector(varVectorBase);
                // add selection info from the histInfos object
                binVector.push_back(info.bins());
                lowerBoundVector.push_back(info.lowerBound());
                upperBoundVector.push_back(info.upperBound());
                varVector.push_back(info.variable());
                varVector.push_back(info.weight());

                // Book the THnSparseD histo
                const ROOT::RDF::THnDModel tempModel(newName.c_str(), newName.c_str(), selection.size()+1,binVector.data(), lowerBoundVector.data(), upperBoundVector.data());

                //histos_m.push_back(tempModel.GetResultPtr());
                histos_m.emplace(histos_m.end(), df.HistoND<types..., float>(tempModel, varVector));
                
            }
        }

        // Iterate over all the histos and save them (triggering their execution)
        void save(){
            for(auto &histo_m : histos_m){
                auto ptr = histo_m.GetPtr();
		        ptr->Write();
                //histo_m->Write();
            }
        }

    private:
        std::vector<ROOT::RDF::RResultPtr<THnSparseD>> histos_m;

};


#endif
