#ifndef PLOTS_H_INCLUDED
#define PLOTS_H_INCLUDED


#include <ROOT/RDF/HistoModels.hxx>
#include <cmath>
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

    /*THnMulti(THnMulti &&copy){
        fFinalResult(copy.fFinalResult);
        fPerThreadResults(copy.fPerThreadResults);

        nSlots_m(copy.nSlots_m);
        dim_m(copy.dim_m);
        nbins_m(copy.nbins_m);
        xmin_m(copy.xmin_m);
        xmax_m(copy.xmax_m);
    }*/


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
            
            /*for(int j =0; j<dim_m; j++){
                std::cerr << array[j] << ", " ;
            }*/
            
            //std::cerr << weight << std::endl;
            
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
                //
                //std::string branchVector = an->DefineVector<Double_t>(varVector);
                //THnMulti<Double_t> tempModel(df.GetNSlots(), newName.c_str(), newName.c_str(), selection.size()+1,binVector, lowerBoundVector, upperBoundVector, branchVector);
                //tempModel.Exe

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
        //std::vector<std::shared_ptr<THnSparseD>> histos_m;

};



/*
//
// Plotting functions here
//

// General ratio plot plotting function, really needs to be a template
template <typename T>
void plot(ROOT::RDF::RResultPtr<T> plot_lo, ROOT::RDF::RResultPtr<T> plot_nlo, std::string name, std::string suffix, std::string xlabel, std::string title, TCanvas &canvas,
          bool isNAddJets, std::string label1, std::string label2){

    // yscale is log
    gPad->SetLogy(1);

    // set colors for the plots
    plot_lo->SetLineColor(kRed);
    plot_lo->SetLineWidth(2);
    plot_nlo->SetLineColor(kBlue);
    plot_nlo->SetLineWidth(2);

    // calculate x axis range

    double minX = plot_lo->GetBinLowEdge(1);
    double maxX = plot_lo->GetBinLowEdge(plot_lo->GetNbinsX()) + plot_lo->GetBinWidth(plot_lo->GetNbinsX());


    // calculate upper y axixs range
    double nloMin;
    double nloMax;
    double loMin;
    double loMax;
    double allMin;
    double allMax;

    plot_nlo->GetMinimumAndMaximum(nloMin, nloMax);
    plot_lo->GetMinimumAndMaximum(loMin, loMax);

    allMin = loMin < nloMin ? loMin : nloMin;
    allMax = loMax > nloMax ? loMax : nloMax;
    allMin = allMin <= 0. ? 10 : allMin;

    // Format the histograms
    plot_nlo->SetStats(0);
    plot_lo->SetStats(0);

    // Make the ratio plot
    TRatioPlot ratioPlot(plot_nlo.GetPtr(), plot_lo.GetPtr());

    // Upper histograms should show errorss
    ratioPlot.SetH1DrawOpt("E");
    ratioPlot.SetH2DrawOpt("E");

    // Don't want extra lines in the ratio plot'
    ratioPlot.Draw("nogrid");

    // Format upper pannel

    // Set y range on upper plot and add legend
    ratioPlot.GetUpperRefYaxis()->SetRangeUser(allMin/2, allMax*2);
    ratioPlot.GetUpperRefYaxis()->SetTitle("entries");

    // Make the legend
    TLegend legend(0.75,0.7,0.89,0.9);
    legend.SetHeader("Sample","C"); // option "C" allows to center the header
    legend.AddEntry(plot_lo.GetPtr(),label1.c_str(),"l");
    legend.AddEntry(plot_nlo.GetPtr(),label2.c_str(),"l");

    // Add legend
    ratioPlot.GetUpperPad()->cd();
    legend.Draw();

    // Format the lower pannel


    // Deterine bounds of ratio plot
    double ratioPlotMax = 1.01;
    double ratioPlotMin = 0.99;



    int entries = ratioPlot.GetLowerRefGraph()->GetN();

    for(int i = 0; i<entries; i++){
        float yVal = ratioPlot.GetLowerRefGraph()->GetPointY(i);
        //std::cout << yVal << std::endl;
        if(yVal<1){
            yVal=1 - (1-yVal)*1.1;
            ratioPlotMin = ratioPlotMin < yVal ? ratioPlotMin : yVal;

        } else if(yVal>1){
            yVal=1 + (yVal-1)*1.1;
            ratioPlotMax = ratioPlotMax > yVal ? ratioPlotMax : yVal;
        }

    }

    ratioPlotMin = ratioPlotMin < 0.5 ? 0.5 : ratioPlotMin;
    ratioPlotMax = ratioPlotMax > 1.5 ? 1.5 : ratioPlotMax;

    // Set the lower graph to display errors as well
    ratioPlot.GetLowerRefYaxis()->SetTitle("ratio");
    ratioPlot.GetXaxis()->SetTitle(xlabel.c_str());
    ratioPlot.GetLowerRefYaxis()->SetRangeUser(ratioPlotMin,ratioPlotMax);
    ratioPlot.GetLowerRefYaxis()->SetNdivisions(1,0,0);
    ratioPlot.GetLowerRefGraph()->Draw("E");

    // format the lower axes

    ratioPlot.GetLowerPad()->cd();


    // Make a line at 1
    TLine line(minX, 1., maxX, 1.);
    line.SetLineStyle(kDashed);
    line.SetLineWidth(2);
    line.Draw("SAME");
    TLine sep1(3.,ratioPlotMin, 3., ratioPlotMax);
    std::cout << "3, " << ratioPlotMin << ", 3 " << ratioPlotMax << std::endl;
    sep1.SetLineStyle(kDashed);
    sep1.SetLineWidth(2);

    TLine sep2(6.,ratioPlotMin, 6., ratioPlotMax);
    std::cout << "6, " << ratioPlotMin << ", 6 " << ratioPlotMax << std::endl;
    sep2.SetLineStyle(kDashed);
    sep2.SetLineWidth(2);

    if(isNAddJets){
        std::cout << "Doing extra plots" << std::endl;
        //x1, y1, x2, y2
        sep1.Draw("SAME");
        sep2.Draw("SAME");


        TText addJets0(0.1,0.05,"0 additional jets");
        addJets0.SetTextSize(0.1);
        addJets0.DrawTextNDC(0.15,0.05,"0 additional jets");

        TText addJets1(0.4,0.05,"1 additional jets");
        addJets0.SetTextSize(0.1);
        addJets0.DrawTextNDC(0.45,0.05,"1 additional jets");

        TText addJets2(0.7,0.05,"2 additional jets");
        addJets0.SetTextSize(0.1);
        addJets0.DrawTextNDC(0.7,0.05,"2 additional jets");
    }

    // Save the plot
    //std::string firstName = name+"_"+suffix+ ".png";
    std::string firstName = name + ".png";
    canvas.Print(firstName.c_str());
    canvas.Clear();

}

void fitCorrection(TH1F &ratioPlot, TF1 &fitFunc, TF1 &fitFuncUp, TF1 &fitFuncDown, TCanvas &canvas, std::string name, float scale, float yMin, float yMax, std::string extraLabel);


// Class to make and manage plots of variables of interest
class plotHolder {
    public:
        plotHolder(ROOT::RDF::RNode df_LO,ROOT::RDF::RNode df_NLO, std::string weightBranchLO, std::string weightBranchNLO, std::string prefix, std::string suffix, std::string label1, std::string label2);

        // Calculate the scaleling between the vpt histograms
        void getScale(double &scale, double &error);

        // Plot the histograms to the supplied canvas
        void plotContent(TCanvas &canvas);

    private:
        std::vector<ROOT::RDF::RResultPtr<TH1D>> histos;
        std::string suffix_m;
        std::string prefix_m;
        std::string label1_m;
        std::string label2_m;
};

*/



template <int bins_m>
class plotRatio{
    public:
        plotRatio(int dpi=72):
            canvas_m("c", "canvas", 11*dpi,11*dpi),
            histPad_m("histPad", "histPad", 0, 0.4, 0.9, 1.0),
            ratioPad_m("ratioPad", "ratioPad", 0, 0.25, 0.9, 0.4),
            pullPad_m("pullPad", "pullPad", 0, 0.0, 0.9, 0.25),
            legend_m(0.82,0.4,1.0,0.9),
            pull0(),
            ratio1(),
            dpi_m(dpi)
        {
            gStyle->SetPadTickX(1);  // To get tick marks on the opposite side of the frame
            gStyle->SetPadTickY(1);

            histPad_m.SetBottomMargin(0); // Upper and lower plot are joined
            ratioPad_m.SetTopMargin(0);
            ratioPad_m.SetBottomMargin(0.0);
            pullPad_m.SetTopMargin(0);
            pullPad_m.SetBottomMargin(0.4);
            
            // legend_m.SetHeader("Legend","C"); // option "C" allows to center the header
            legend_m.SetBorderSize(0);
            

            //histPad_m.SetFrameFillStyle(0);
            //ratioPad_m.SetFrameFillStyle(0);
            
            pull0.SetLineStyle(kDashed);
            pull0.SetLineWidth(2);

            ratio1.SetLineStyle(kDashed);
            ratio1.SetLineWidth(2);
            
            pull0.SetY1(0.0);
            pull0.SetY2(0.0);
            ratio1.SetY1(1.0);
            ratio1.SetY2(1.0);

            for(int i = 0; i<bins_m; i++){
                upperBoxes[i].SetFillColorAlpha(kBlue+1, 0.5);
                upperBoxes[i].SetLineColorAlpha(kBlue+1, 0.0);
                upperBoxes[i].SetFillStyle(3244);
                lowerBoxes[i].SetFillColorAlpha(kRed, 0.5);
                lowerBoxes[i].SetLineColorAlpha(kRed, 0.0);
                lowerBoxes[i].SetFillStyle(3244);
                ratioBoxes[i].SetFillColorAlpha(kBlack, 0.5);
                ratioBoxes[i].SetLineColorAlpha(kBlack, 0.0);
                ratioBoxes[i].SetFillStyle(3244);
                pullBoxes[i].SetLineColorAlpha(kBlack, 0.0);
                pullBoxes[i].SetFillStyle(3244);
            }
            
        };

        void plot(TH1F &histTop, TH1F &histBottom, const std::string fileName, const std::string xlabel,
                  const std::string labelTop, const std::string labelBottom){
            
            // Reset the main canvas
            canvas_m.Clear();

            // Draw the top hist pad
            histPad_m.Draw();
            histPad_m.cd();

            
            

            // Draw the two main histograms
            histTop.Draw("HIST");
            histBottom.Draw("SAME HIST");
            double maxHist = 0.0;
            double minHist = 0.0;
            
            for(int i=1; i<=bins_m; i++){
                //binCenters_m[i-1] = histTop.GetBinCenter(i);
                const double topError = histTop.GetBinError(i);
                const double bottomError  = histBottom.GetBinError(i);
                const double topVal = histTop.GetBinContent(i);
                const double bottomVal = histBottom.GetBinContent(i);
                
                upperBoxes[i-1].DrawBox(histTop.GetBinLowEdge(i),topVal-topError,histTop.GetBinLowEdge(i+1),topVal+topError);
                lowerBoxes[i-1].DrawBox(histBottom.GetBinLowEdge(i),bottomVal-bottomError,histBottom.GetBinLowEdge(i+1),bottomVal+bottomError);
                
                maxHist = std::max<double>({topVal+topError,bottomVal+bottomError, maxHist});

            }
            


            
            // histTop settings
            histTop.SetStats(0);
            histTop.SetLineColor(kBlue+1);
            histTop.SetLineWidth(int(2*dpi_m/72));
            
            // Y axis plot settings
            histTop.GetYaxis()->SetTitleSize(int(20*dpi_m/72));
            histTop.GetYaxis()->SetTitleFont(int(43*dpi_m/72));
            histTop.GetYaxis()->SetTitleOffset(1.55);
            histTop.GetYaxis()->SetTitle("Counts");
            histTop.GetYaxis()->ChangeLabel(1, -1, -1, -1, -1, -1, " ");
            histTop.GetYaxis()->SetLabelFont(int(43*dpi_m/72)); // Absolute font size in pixel (precision 3)
            histTop.GetYaxis()->SetLabelSize(int(15*dpi_m/72));
            histTop.GetYaxis()->SetRangeUser(minHist, maxHist);


            // histBottom settings
            histBottom.SetStats(0);
            histBottom.SetLineColor(kRed);
            histBottom.SetLineWidth(int(2*dpi_m/72));


            //TAxis rightAxis
            canvas_m.cd();
            legend_m.Clear();
            legend_m.AddEntry(&histTop,labelTop.c_str(),"l");
            legend_m.AddEntry(&histBottom,labelBottom.c_str(),"l");
            
            legend_m.Draw();


            plotRatiosPulls(histTop, histBottom, xlabel);
            // Save the histogram
            canvas_m.Print((fileName+"_lin.png").c_str());
            
        }

    private:

        void plotRatiosPulls(TH1F &histTop, TH1F &histBottom, const std::string &xlabel){
            
            canvas_m.cd();
            // Set the range on the reference lines for the pull plot and ratio plot
            pull0.SetX1(histTop.GetBinLowEdge(1));
            pull0.SetX2(histTop.GetBinLowEdge(bins_m+1));
            ratio1.SetX1(histTop.GetBinLowEdge(1));
            ratio1.SetX2(histTop.GetBinLowEdge(bins_m+1));


            // make the ratio hist
            TH1F histRatio = histTop/histBottom;

            double maxPull = 1.0;
            double minPull = -1.0;
            double minRatio = 0.9;
            double maxRatio = 1.1;
            
            TH1F pullHisto("Pulls", "Pulls",bins_m, histTop.GetBinLowEdge(1), histTop.GetBinLowEdge(bins_m+1));
            
            for(int i=1; i<=bins_m; i++){
                const double topErrors = histTop.GetBinError(i);
                const double bottomErrors = histBottom.GetBinError(i);
                const double topVal = histTop.GetBinContent(i);
                const double bottomVal = histBottom.GetBinContent(i);
                const double ratioUncert = histRatio.GetBinError(i);
                // calculate and store the pulls
                if(topErrors==0 || bottomErrors==0){
                    pulls_m[i-1]=0;
                } else {
                    pulls_m[i-1] = (topVal-bottomVal)/sqrt(topErrors*topErrors + bottomErrors*bottomErrors);
                    pulls_m[i-1] = std::min<float>(pulls_m[i-1], 5.2);
                    pulls_m[i-1] = std::max<float>(pulls_m[i-1], -5.2);
                }

                // fill the pull histo
                pullHisto.Fill(histTop.GetBinCenter(i),pulls_m[i-1]);

                // calculate the pull plot min and max
                minPull = std::min<float>(pulls_m[i-1]*1.1, minPull);
                maxPull = std::max<float>(pulls_m[i-1]*1.1, maxPull);

                // calculate the ratio plot min and max
                minRatio = std::min<float>((histRatio.GetBinContent(i)-ratioUncert)*0.9, minRatio);
                maxRatio = std::max<float>((histRatio.GetBinContent(i)+ratioUncert)*1.1, maxRatio);

                

            }
            
            maxRatio = std::min<float>(maxRatio, 1.6);
            minRatio = std::max<float>(minRatio, 0.4);

            maxPull = std::min<float>(maxPull,  5.2);
            minPull = std::max<float>(minPull, -5.2);


            // Draw and change to the ratio pad
            ratioPad_m.Draw();
            ratioPad_m.cd();
            
            
            
            // Draw hist plot
            
            // Ratio plot general settings
            histRatio.SetTitle(""); // Remove the ratio title
            histRatio.SetLineColor(kBlack);
            histRatio.Sumw2();
            histRatio.SetStats(0);
            histRatio.SetMarkerStyle(int(21*dpi_m/72));
            histRatio.Draw("HIST ][");
            
            /*
            // Ratio Y axis plot settings
            histRatio.GetYaxis()->SetTitle("Ratio");
            histRatio.GetYaxis()->SetRangeUser(minRatio, maxRatio);
            histRatio.GetYaxis()->SetNdivisions(505);
            histRatio.GetYaxis()->SetTitleSize(int(20*dpi_m/72));
            histRatio.GetYaxis()->SetTitleFont(int(43*dpi_m/72));
            histRatio.GetYaxis()->SetTitleOffset(1.55);
            histRatio.GetYaxis()->SetLabelFont(int(43*dpi_m/72)); // Absolute font size in pixel (precision 3)
            histRatio.GetYaxis()->SetLabelSize(int(15*dpi_m/72));
            
            // Ratio X axis plot settings
            histRatio.GetXaxis()->SetTitleSize(int(20*dpi_m/72));
            histRatio.GetXaxis()->SetTitleFont(int(43*dpi_m/72));
            histRatio.GetXaxis()->SetTitleOffset(1);
            histRatio.GetXaxis()->SetLabelFont(int(43*dpi_m/72));

            // Draw the error bars inside the canvas
            for(int i=0; i<bins_m; i++){
                double uncert = histRatio.GetBinError(i+1);
                double upperEdge = histRatio.GetBinContent(i+1) + uncert;
                double lowerEdge = histRatio.GetBinContent(i+1) - uncert;
                upperEdge = std::min<double>(upperEdge, 1.6);
                lowerEdge = std::max<double>(lowerEdge, 0.4);
 
                upperEdge = std::max<double>(upperEdge, 0.4);
                lowerEdge = std::min<double>(lowerEdge, 1.6);
 
                ratioBoxes[i].DrawBox(histRatio.GetBinLowEdge(i+1),lowerEdge,histTop.GetBinLowEdge(i+2), upperEdge);//,"SAME");
 
            }

            // Draw the ratio reference line
            ratio1.Draw();

            // Change to the main canvas
            canvas_m.cd();

            // Draw the pull pad and change to its directory
            pullPad_m.Draw();
            pullPad_m.cd();

            
            
            // Pull histo general settings
            pullHisto.SetStats(0);
            pullHisto.SetTitle(""); // Remove the pull title
            pullHisto.SetLineColor(kBlack);

            // Draw the histogram
            pullHisto.Draw("Hist ][");
            
            // Pull Y axis ratio plot settings
            pullHisto.GetYaxis()->SetTitle("Pull");
            pullHisto.GetYaxis()->SetRangeUser(minPull, maxPull);
            pullHisto.GetYaxis()->SetNdivisions(505);
            pullHisto.GetYaxis()->SetTitleSize(int(20*dpi_m/72));
            pullHisto.GetYaxis()->SetTitleFont(int(43*dpi_m/72));
            pullHisto.GetYaxis()->SetTitleOffset(1.55);
            pullHisto.GetYaxis()->SetLabelFont(int(43*dpi_m/72)); // Absolute font size in pixel (precision 3)
            pullHisto.GetYaxis()->SetLabelSize(int(15*dpi_m/72));
            
            // Pull X axis ratio plot settings
            pullHisto.GetXaxis()->SetTitle(xlabel.c_str());
            pullHisto.GetXaxis()->SetTitleSize(int(20*dpi_m/72));
            pullHisto.GetXaxis()->SetTitleFont(int(43*dpi_m/72));
            pullHisto.GetXaxis()->SetTitleOffset(0);
            pullHisto.GetXaxis()->SetLabelFont(int(43*dpi_m/72));
            pullHisto.GetXaxis()->SetLabelSize(int(15*dpi_m/72));
            
            // Set the boxes for the pulls and draw them
            for(int i=0; i<bins_m; i++){
                if(pulls_m[i]>0.0){
                    pullBoxes[i].SetFillColor(kRed);//, 0.5);
                } else if(pulls_m[i]<0.0) {
                    pullBoxes[i].SetFillColor(kBlue);//, 0.5);    
                } else {
                    pullBoxes[i].SetFillColor(kBlack);//, 0.5);
                }

                pullBoxes[i].DrawBox(histRatio.GetBinLowEdge(i+1),0,histTop.GetBinLowEdge(i+2), pulls_m[i]);//,"SAME");
            }

            // Draw the pull reference line
            pull0.Draw();
            */
            // Change to the main directory
            canvas_m.cd();

        }


        TCanvas canvas_m;
        TPad histPad_m;
        TPad ratioPad_m;
        TPad pullPad_m;
        TLegend legend_m;
        TLine pull0;
        TLine ratio1;
        TBox upperBoxes[bins_m];
        TBox lowerBoxes[bins_m];
        TBox ratioBoxes[bins_m];
        TBox pullBoxes[bins_m];
        const int dpi_m;
        //double binCenters_m[bins_m];
        //double topErrors_m[bins_m];
        //double bottomErrors_m[bins_m];
        //double ratios_m[bins_m];
        double pulls_m[bins_m]; 
};



#endif
