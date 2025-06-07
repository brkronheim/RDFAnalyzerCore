
#include <RtypesCore.h>
#include <string>
#include <chrono>
#include <iostream>

#include <TRandom2.h>
#include <TH1F.h>
#include <TGraph.h>
#include <TCanvas.h>
#include <TAxis.h>
#include <TBox.h>
#include <TROOT.h>
#include <TStyle.h>
#include <TLegend.h>
#include <TLine.h>

#include "plots.h"

/*
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
            TH1F histRatio = histTop/histBottom;
            canvas_m.Clear();
            histPad_m.Draw();
            histPad_m.cd();
            histTop.SetStats(0);
            histBottom.SetStats(0);
            histTop.Draw("HIST");
            histBottom.Draw("SAME HIST");
            double maxPull = 0.1;
            double minPull = -0.1;
            double minRatio = 0.9;
            double maxRatio = 1.1;
            double maxHist = 0.0;
            double minHist = 0.0;
            TH1F pullHisto("Pulls", "Pulls",bins_m, histTop.GetBinLowEdge(1), histTop.GetBinLowEdge(bins_m+1));
            //TH1F pullHisto("Pulls", "Pulls",100,-5,5);
            //pullHisto.Clear();
            for(int i=1; i<=bins_m; i++){
                binCenters_m[i-1] = histTop.GetBinCenter(i);
                topErrors_m[i-1] = histTop.GetBinError(i);
                bottomErrors_m[i-1] = histBottom.GetBinError(i);
                double topVal = histTop.GetBinContent(i);//+topErrors_m[i-1];
                double bottomVal = histBottom.GetBinContent(i);//+topErrors_m[i-1];
                double ratioUncert = histRatio.GetBinError(i);
                ratios_m[i-1] = histRatio.GetBinContent(i);
                if(topErrors_m[i-1]==0 || bottomErrors_m[i-1]==0){
                    pulls_m[i-1]=0;
                } else {
                    pulls_m[i-1] = (topVal-bottomVal)/sqrt(topErrors_m[i-1]*topErrors_m[i-1] + bottomErrors_m[i-1]*bottomErrors_m[i-1]);
                }
                upperBoxes[i-1].DrawBox(histTop.GetBinLowEdge(i),topVal-topErrors_m[i-1],histTop.GetBinLowEdge(i+1),topVal+topErrors_m[i-1]);//,"SAME");
                lowerBoxes[i-1].DrawBox(histBottom.GetBinLowEdge(i),bottomVal-bottomErrors_m[i-1],histBottom.GetBinLowEdge(i+1),bottomVal+bottomErrors_m[i-1]);//,"SAME");
                double binMax = std::max<float>(topVal+topErrors_m[i-1],bottomVal+bottomErrors_m[i-1]);
                maxHist = std::max<float>(binMax, maxHist);
                minRatio = std::min<float>((ratios_m[i-1]-ratioUncert)*0.9, minRatio);
                maxRatio = std::max<float>((ratios_m[i-1]+ratioUncert)*1.1, maxRatio);
                pullHisto.Fill(binCenters_m[i-1],pulls_m[i-1]);
                //std::cout << binCenters_m[i-1] << ", " << pulls_m[i-1] << std::endl;

            }
            

            pull0.SetX1(histTop.GetBinLowEdge(1));
            pull0.SetX2(histTop.GetBinLowEdge(bins_m+1));
            ratio1.SetX1(histTop.GetBinLowEdge(1));
            ratio1.SetX2(histTop.GetBinLowEdge(bins_m+1));

            maxRatio = std::min<float>(maxRatio, 1.6);
            minRatio = std::max<float>(minRatio, 0.4);


            TAxis *axis = histTop.GetYaxis();
            axis->ChangeLabel(1, -1, -1, -1, -1, -1, " ");
            axis->SetLabelFont(int(43*dpi_m/72)); // Absolute font size in pixel (precision 3)
            axis->SetLabelSize(int(15*dpi_m/72));
            axis->SetRangeUser(minHist, maxHist);
            //TAxis rightAxis
            canvas_m.cd();
            legend_m.Clear();
            legend_m.AddEntry(&histTop,labelTop.c_str(),"l");
            legend_m.AddEntry(&histBottom,labelBottom.c_str(),"l");
            
            legend_m.Draw();

            
            canvas_m.cd();
            ratioPad_m.Draw();
            ratioPad_m.cd();
            histRatio.SetLineColor(kBlack);
            histRatio.Sumw2();
            histRatio.SetStats(0);
            histRatio.SetMarkerStyle(int(21*dpi_m/72));
            histRatio.Draw("HIST ][");
            histRatio.GetYaxis()->SetRangeUser(minRatio, maxRatio);
            
            for(int i=0; i<bins_m; i++){
                double uncert = histRatio.GetBinError(i+1);
                double upperEdge = ratios_m[i] + uncert;
                double lowerEdge = ratios_m[i] - uncert;
                upperEdge = std::min<double>(upperEdge, 1.6);
                lowerEdge = std::max<double>(lowerEdge, 0.4);
 
                upperEdge = std::max<double>(upperEdge, 0.4);
                lowerEdge = std::min<double>(lowerEdge, 1.6);
 

                ratioBoxes[i].DrawBox(histRatio.GetBinLowEdge(i+1),lowerEdge,histTop.GetBinLowEdge(i+2), upperEdge);//,"SAME");
 
            }

   
            // histTop settings
            histTop.SetLineColor(kBlue+1);
            histTop.SetLineWidth(int(2*dpi_m/72));
            
            // Y axis h1 plot settings
            histTop.GetYaxis()->SetTitleSize(int(20*dpi_m/72));
            histTop.GetYaxis()->SetTitleFont(int(43*dpi_m/72));
            histTop.GetYaxis()->SetTitleOffset(1.55);
            
            // h2 settings
            histBottom.SetLineColor(kRed);
            histBottom.SetLineWidth(int(2*dpi_m/72));
            
            // Ratio plot (h3) settings
            histRatio.SetTitle(""); // Remove the ratio title
            
            // Y axis ratio plot settings
            histRatio.GetYaxis()->SetTitle("Ratio");
            histTop.GetYaxis()->SetTitle("Counts");
            histRatio.GetYaxis()->SetNdivisions(505);
            histRatio.GetYaxis()->SetTitleSize(int(20*dpi_m/72));
            histRatio.GetYaxis()->SetTitleFont(int(43*dpi_m/72));
            histRatio.GetYaxis()->SetTitleOffset(1.55);
            histRatio.GetYaxis()->SetLabelFont(int(43*dpi_m/72)); // Absolute font size in pixel (precision 3)
            histRatio.GetYaxis()->SetLabelSize(int(15*dpi_m/72));
            
            // X axis ratio plot settings
            histRatio.GetXaxis()->SetTitleSize(int(20*dpi_m/72));
            histRatio.GetXaxis()->SetTitleFont(int(43*dpi_m/72));
            histRatio.GetXaxis()->SetTitleOffset(1);
            histRatio.GetXaxis()->SetLabelFont(int(43*dpi_m/72)); //
            ratio1.Draw();
            canvas_m.cd();
            pullPad_m.Draw();
            pullPad_m.cd();
            
            pullHisto.SetStats(0);
            pullHisto.SetTitle(""); // Remove the pull title
            pullHisto.SetLineColor(kBlack);
            pullHisto.Draw("Hist ][");
            pullHisto.GetXaxis()->SetTitle(xlabel.c_str());
            pullHisto.GetYaxis()->SetTitle("Pull");
            pullHisto.GetYaxis()->SetNdivisions(505);
            pullHisto.GetYaxis()->SetTitleSize(int(20*dpi_m/72));
            pullHisto.GetYaxis()->SetTitleFont(int(43*dpi_m/72));
            pullHisto.GetYaxis()->SetTitleOffset(1.55);
            pullHisto.GetYaxis()->SetLabelFont(int(43*dpi_m/72)); // Absolute font size in pixel (precision 3)
            pullHisto.GetYaxis()->SetLabelSize(int(15*dpi_m/72));
            
            // X axis ratio plot settings
            pullHisto.GetXaxis()->SetTitleSize(int(20*dpi_m/72));
            pullHisto.GetXaxis()->SetTitleFont(int(43*dpi_m/72));
            pullHisto.GetXaxis()->SetTitleOffset(0);
            pullHisto.GetXaxis()->SetLabelFont(int(43*dpi_m/72));
            pullHisto.GetXaxis()->SetLabelSize(int(15*dpi_m/72));
            //
            for(int i=0; i<bins_m; i++){
                if(pulls_m[i]>0.0){
                    pullBoxes[i].SetFillColor(kRed);//, 0.5);
                } else if(pulls_m[i]<0.0) {
                    pullBoxes[i].SetFillColor(kBlue);//, 0.5);    
                } else {
                    pullBoxes[i].SetFillColor(kBlack);//, 0.5);
                }
                //pullBoxes[i].SetFillStyle(3244);
                
                pullBoxes[i].DrawBox(histRatio.GetBinLowEdge(i+1),0,histTop.GetBinLowEdge(i+2), pulls_m[i]);//,"SAME");
            }
            pull0.Draw();
            

            
            canvas_m.Print((fileName+"_lin.png").c_str());
        }

    private:
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
        double binCenters_m[bins_m];
        double topErrors_m[bins_m];
        double bottomErrors_m[bins_m];
        double ratios_m[bins_m];
        double pulls_m[bins_m]; 
};
*/
template <int bins>
int testRatioPlot(int index, TRandom2 &randGenerator, plotRatio<bins> &ratioPlotter){
    
    
    
    
    TH1F hist1("Hist1", "Hist 1", bins, -5,5);
    TH1F hist2("Hist2", "Hist 2", bins, -5,5);
    for(int i = 0; i<10000; i++){
        hist1.Fill(randGenerator.Gaus(0.0,1.0),randGenerator.Gaus(1.0,0.1));
        hist2.Fill(randGenerator.Gaus(0.0,0.9),randGenerator.Gaus(1.0,0.1));
    }
    std::string fileName = "RatioHist"+std::to_string(index);
    ratioPlotter.plot(hist1, hist2, fileName,"p_{T}", "Top", "Bottom");
    return(0);
}

int main(){

    /*gStyle->SetCanvasColor(kWhite);
    gStyle->SetCanvasDefH(600); //Height of canvas
    gStyle->SetCanvasDefW(600); //Width of canvas
    gStyle->SetCanvasDefX(0);   //POsition on screen
    gStyle->SetCanvasDefY(0);

    // For the Pad:
    gStyle->SetPadBorderMode(0);
    // tdrStyle->SetPadBorderSize(Width_t size = 1);
    gStyle->SetPadColor(kWhite);
    gStyle->SetPadGridX(false);
    gStyle->SetPadGridY(false);
    gStyle->SetGridColor(0);
    gStyle->SetGridStyle(3);
    gStyle->SetGridWidth(1);*/
    
    const long timeStart = std::chrono::duration_cast< std::chrono::milliseconds >(std::chrono::system_clock::now().time_since_epoch()).count();
    const int seed = 42;
    TRandom2 randGenerator(seed);
    plotRatio<100> ratioPlotter(72);
    gROOT->ProcessLine( "gErrorIgnoreLevel = 2001;");
    const long plotNumber = 10;
    for(int i = 0; i<plotNumber; i++){
        testRatioPlot(i, randGenerator, ratioPlotter);
    }
    const long timeEnd = std::chrono::duration_cast< std::chrono::milliseconds >(std::chrono::system_clock::now().time_since_epoch()).count();
    std::cout << timeEnd-timeStart << " ms elapsed" << std::endl;
    std::cout << float(plotNumber*1000)/float(timeEnd-timeStart) << " Hz" << std::endl;
    return(0);
}