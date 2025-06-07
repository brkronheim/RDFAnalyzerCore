/*
#include <cmath>
#include <vector>
#include <string>
#include <iostream>
#include <chrono>

#include <ROOT/RDataFrame.hxx>
#include <Math/MinimizerOptions.h>
#include <TCanvas.h>
#include <TPad.h>
#include <TFile.h>
#include <TLegend.h>
#include <TAxis.h>
#include <TRatioPlot.h>
#include <TH1F.h>
#include <TH1.h>
#include <TH1I.h>
#include <TF1.h>
#include <TGraphErrors.h>
#include <TLine.h>
#include <TVirtualFitter.h>
#include <TFitResult.h>
#include <TText.h>
*/
//#include <plots.h>

//
// These are very specific plotting functions, they are probably not really needed
//
/*
void fitCorrection(TH1F &ratioPlot, TF1 &fitFunc, TF1 &fitFuncUp, TF1 &fitFuncDown, TCanvas &canvas, std::string name, float scale, float yMin, float yMax, std::string extraLabel){
        gPad->SetLogy(0);
        std::string filename = "/home/brkronheim/cernbox/vhbb/DYSkim/" +name + ".png";

        // peform the fit
        auto fitResult = ratioPlot.Fit(&fitFunc, "MULTITHREAD S M 0 Q");

        // get the range of the histogram
        double minX = ratioPlot.GetBinLowEdge(1);
        int nBins = ratioPlot.GetNbinsX();
        double maxX = ratioPlot.GetBinLowEdge(nBins) + ratioPlot.GetBinWidth(nBins);
        double binSize = (maxX-minX)/double(nBins);

        // store points for the uncertainty graph
        double xpoint[nBins*2+1];
        double ypoint[nBins*2+1];
        double yUp[nBins*2+1];
        double yDown[nBins*2+1];

        double yUpRatio[nBins*2+1];
        double yDownRatio[nBins*2+1];

        double ex[nBins*2+1];
        double ey[nBins*2+1];

        double dataX[nBins];
        double dataY[nBins];
        double dataErrorX[nBins];
        double dataErrorY[nBins];

        for(int i = 0; i<nBins*2+1; i++){
            xpoint[i] = minX + double(i)*binSize/2.;
            ypoint[i] = fitFunc.Eval(xpoint[i]);
            ex[i] = 0.0;
        }

        //get the 2 sigma uncertainty bands
        fitResult->GetConfidenceIntervals(nBins*2-1,1,0,xpoint,ey, 0.95);
        for(int i = 0; i<nBins*2+1; i++){
            ey[i]*=scale;
            yUp[i] = ypoint[i] + ey[i];
            yDown[i] = ypoint[i] - ey[i];

        }

        // Set the parameters for the error bands to be equal to the nominal to start
        for(int i = 0; i<fitFunc.GetNumberFreeParameters() ; i++){
            fitFuncUp.SetParameter(i, fitFunc.GetParameter(i));
            fitFuncDown.SetParameter(i ,fitFunc.GetParameter(i));
        }

        if(fitFunc.GetNumberFreeParameters()==2){
            fitFuncUp.SetParameter(0, 1.1);
            fitFuncDown.SetParameter(0, 0.9);

            fitFuncUp.SetParameter(1, 0.);
            fitFuncDown.SetParameter(1, 0.);

            //fitFuncUp.SetParameter(2, 0.0);
            //fitFuncDown.SetParameter(2, 0.0);
        }

        // Fit the error bands
        TGraph upGraph(nBins*2+1,xpoint,yUp);
        upGraph.Fit(&fitFuncUp, "MULTITHREAD S M 0 Q");
        TGraph downGraph(nBins*2+1,xpoint,yDown);
        downGraph.Fit(&fitFuncDown, "MULTITHREAD S M 0 Q");
        // log the fit parameters
        std::cout << "parameter index | nominal     | up         | down " << std::endl;
        for(int i = 0; i<fitFunc.GetNumberFreeParameters() ; i++){
            std::cout << "[" << i << "] | " << fitFunc.GetParameter(i) << " | " << fitFuncUp.GetParameter(i) << " | " << fitFuncDown.GetParameter(i) << std::endl;
        }
        std::cout << std::endl;

        // get ratio of data
        float histMin = 100000;
        float histMax = 0;
        for(int i = 0; i<nBins; i++){
            dataX[i]=minX+binSize*(0.5+float(i));
            dataY[i]=ratioPlot.GetBinContent(i+1)/ypoint[i*2+1];



            dataErrorX[i]=0;
            dataErrorY[i]=ratioPlot.GetBinError(i+1)/ypoint[i*2+1];
        }



        // get ratio of data
        float ratioMin = 0.99;
        float ratioMax = 1.01;
        for(int i = 0; i<nBins*2+1; i++){
            float fitFuncUpEval = fitFuncUp.Eval(xpoint[i]);
            float fitFuncDownEval = fitFuncDown.Eval(xpoint[i]);
            yUpRatio[i]=fitFuncUpEval/ypoint[i];
            yDownRatio[i]=fitFuncDownEval/ypoint[i];

            histMin = fitFuncDownEval<histMin ? fitFuncDownEval : histMin;
            histMax = fitFuncUpEval>histMax ? fitFuncUpEval : histMax;

            float downOption = 1 - (1-yDownRatio[i])*1.1;
            float upOption = 1 + (yUpRatio[i]-1)*1.1;;
            ratioMin = downOption<ratioMin ? downOption : ratioMin;
            ratioMax = upOption>ratioMax ? upOption : ratioMax;
        }
        std::cout << "ymin: " << histMin << "; ymax: " << histMax << std::endl;
        yMin = yMin > histMin*0.9 ? yMin : histMin*0.9;
        yMax = yMax < histMax*1.1 ? yMax : histMax*1.1;

        TGraph upGraphRatio(nBins*2+1,xpoint,yUpRatio);
        TGraphErrors dataRatio(nBins,dataX, dataY, dataErrorX, dataErrorY);
        TGraph downGraphRatio(nBins*2+1,xpoint,yDownRatio);


        TPad upperPad("upperPad", "upperPad", 0, 0.3, 1, 1);
        upperPad.SetTopMargin(0.15);
        upperPad.SetBottomMargin(0.1); // Upper and lower plot are joined
        upperPad.SetLeftMargin(0.1);
        upperPad.SetRightMargin(0.05);
        upperPad.Draw();             // Draw the upper pad: pad1
        upperPad.cd();

        // Plot the raw data
        ratioPlot.SetTitle(name.c_str());
        ratioPlot.GetYaxis()->SetRangeUser(yMin, yMax);
        ratioPlot.GetYaxis()->SetNdivisions(5,0,0);
        ratioPlot.GetXaxis()->SetRangeUser(minX, maxX);
        ratioPlot.SetStats(0);
        ratioPlot.Draw();

        // plot the fits
        fitFunc.SetLineColor(kBlue);
        fitFunc.SetLineWidth(2);
        fitFunc.Draw("SAME");

        fitFuncUp.SetLineColor(kRed);
        fitFuncUp.SetLineWidth(2);
        fitFuncUp.Draw("SAME");

        fitFuncDown.SetLineColor(kRed);
        fitFuncDown.SetLineWidth(2);
        fitFuncDown.Draw("SAME");

        std::string intervalLabel = "95% Fit Interval x "+std::to_string(scale);

        // Make the legend
        TLegend legend(0.62,0.70,0.99,0.90);
        legend.AddEntry(&ratioPlot,"NLO/LO","l");
        legend.AddEntry(&fitFunc,"Nominal","l");
        legend.AddEntry(&fitFuncUp,intervalLabel.c_str());

        legend.Draw();

        TText textbox(0.15,0.8,extraLabel.c_str());
        textbox.SetTextSize(0.05);
        textbox.DrawTextNDC(0.15,0.8,extraLabel.c_str());

        canvas.cd();

        TPad lowerPad("lowerPad", "lowerPad", 0, 0.05, 1, 0.3);
        lowerPad.SetTopMargin(0.10);
        lowerPad.SetBottomMargin(0.25); // Upper and lower plot are joined
        lowerPad.SetLeftMargin(0.1);
        lowerPad.SetRightMargin(0.05);
        lowerPad.Draw();             // Draw the upper pad: pad1
        lowerPad.cd();

        dataRatio.SetLineColor(kBlack);
        upGraphRatio.SetLineColor(kRed);
        downGraphRatio.SetLineColor(kRed);
        dataRatio.SetTitle(";VpT;Ratio");
        dataRatio.GetYaxis()->SetRangeUser(ratioMin, ratioMax);
        dataRatio.GetYaxis()->SetTitleOffset(0.25);
        dataRatio.GetYaxis()->SetLabelSize(0.1);
        dataRatio.GetYaxis()->SetTitleSize(0.15);
        dataRatio.GetYaxis()->SetNdivisions(5,0,0);
        dataRatio.GetXaxis()->SetRangeUser(minX, maxX);
        dataRatio.GetXaxis()->SetLabelSize(0.1);
        dataRatio.GetXaxis()->SetTitleOffset(0.7);
        dataRatio.GetXaxis()->SetTitleSize(0.15);



        dataRatio.Draw("0AP");

        upGraphRatio.SetLineWidth(2);
        upGraphRatio.Draw("SAME");

        downGraphRatio.SetLineWidth(2);
        downGraphRatio.Draw("SAME");

        TLine line(minX, 1, maxX, 1);
        line.SetLineStyle(kDashed);
        line.SetLineWidth(2);
        line.Draw();




        canvas.cd();
        canvas.Print(filename.c_str());
        canvas.Clear();
}


// Class to make and manage plots of variables of interest
plotHolder::plotHolder(ROOT::RDF::RNode df_LO,ROOT::RDF::RNode df_NLO, std::string weightBranchLO, std::string weightBranchNLO, std::string prefix, std::string suffix, std::string label1, std::string label2):
    suffix_m(""),
    prefix_m(prefix),
    label1_m(label1),
    label2_m(label2)
{

    // Make plots of vpt, v mass, ht, nb_LHE, and nadd
    histos.reserve(20);
    histos.emplace(histos.end(), std::move(df_LO.Histo1D<float, float>({std::string(prefix+"_vptLO").c_str(), std::string("V pt "+prefix_m).c_str(), 100, 0, 1000},
                                                                        "VpT",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<float, float>({std::string(prefix+"_vptNLO").c_str(), std::string("V pt"+prefix_m).c_str(), 100, 0, 1000},
                                                                        "VpT",weightBranchNLO.c_str())));
    histos.emplace(histos.end(), std::move(df_LO.Histo1D<float, float>({std::string(prefix+"_vMLO").c_str(), std::string("V mass "+prefix_m).c_str(), 100, 0, 1000},
                                                                        "Vm",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<float, float>({std::string(prefix+"_vMNLO").c_str(), std::string("V mass "+prefix_m).c_str(), 100, 0, 1000},
                                                                        "Vm",weightBranchNLO.c_str())));
    histos.emplace(histos.end(), std::move(df_LO.Histo1D<float, float>({std::string(prefix+"_htLO").c_str(), std::string("HT "+prefix_m).c_str(), 100, 0, 3000},
                                                                        "HT",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<float, float>({std::string(prefix+"_htNLO").c_str(), std::string("HT "+prefix_m).c_str(), 100, 0, 3000},
                                                                        "HT",weightBranchNLO.c_str())));
    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_nbLO").c_str(), std::string("N LHE b "+prefix_m).c_str(), 3, 0, 3},
                                                                        "Nb",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_nbNLO").c_str(), std::string("N LHE b "+prefix_m).c_str(), 3, 0, 3},
                                                                        "Nb",weightBranchNLO.c_str())));
    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_NShowerb").c_str(), std::string("N Shower B hadrons "+prefix_m).c_str(), 3, 0, 3},
                                                                        "NShowerb",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_NShowerb").c_str(), std::string("N Shower B hadrons "+prefix_m).c_str(), 3, 0,
        3}, "NShowerb",weightBranchNLO.c_str())));
    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_naddLO").c_str(), std::string("N Shower B hadrons + 3 * Additional Jets "+prefix_m).c_str(), 9, 0, 9},
                                                                        "nAddnB",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_naddNLO").c_str(), std::string("N Shower B hadrons + 3 * Additional Jets "+prefix_m).c_str(), 9, 0, 9},
                                                                        "nAddnB",weightBranchNLO.c_str())));



    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_nbBinLO_LO").c_str(), std::string("LO stich nb x vpt bin "+prefix_m).c_str(), 9, 0, 9},
                                                                        "nbBinLO",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_nbBinLO_NLO").c_str(), std::string("LO stich nb x vpt bin "+prefix_m).c_str(), 9, 0, 9},
                                                                        "nbBinLO",weightBranchNLO.c_str())));

    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_ptBinLO_LO").c_str(), std::string("LO stich ht bin "+prefix_m).c_str(), 9, 0, 9},
                                                                        "ptBinLO",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_ptBinLO_NLO").c_str(), std::string("LO stich ht bin "+prefix_m).c_str(), 9, 0, 9},
                                                                        "ptBinLO",weightBranchNLO.c_str())));


    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_nbBinNLO_LO").c_str(), std::string("NLO stich nb bin "+prefix_m).c_str(), 4, 0, 4},
                                                                        "nbBinNLO",weightBranchLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_nbBinNLO_NLO").c_str(), std::string("NLO stich nb bin "+prefix_m).c_str(), 4, 0, 4},
                                                                        "nbBinNLO",weightBranchNLO.c_str())));


    histos.emplace(histos.end(), std::move(df_LO.Histo1D<int, float>({std::string(prefix+"_ptBinNLO_LO").c_str(), std::string("NLO stich vpt bin "+prefix_m).c_str(), 8, 0, 8},
                                                                        "ptBinNLO",weightBranchNLO.c_str())));
    histos.emplace(histos.end(), std::move(df_NLO.Histo1D<int, float>({std::string(prefix+"_ptBinNLO_NLO").c_str(), std::string("NLO stich vpt bin "+prefix_m).c_str(), 8, 0, 8},
                                                                        "ptBinNLO",weightBranchNLO.c_str())));




}

// Calculate the scaleling between the vpt histograms
void plotHolder::getScale(double &scale, double &error){
    double nloError;
    double loError;
    double integralNLO = histos[1]->IntegralAndError(0,histos[1]->GetNbinsX()+1,nloError,"");
    double integralLO = histos[0]->IntegralAndError(0,histos[0]->GetNbinsX()+1,loError,"");
    scale = integralNLO/integralLO;
    error = scale*sqrt(pow(nloError/integralNLO,2)+pow(loError/integralLO,2));
}

// Plot the histograms to the supplied canvas
void plotHolder::plotContent(TCanvas &canvas){
    plot(histos[0], histos[1], "vpt/"+prefix_m+"_v_pt", suffix_m, "Vp_T [GeV]", "", canvas, false, label1_m, label2_m);
    plot(histos[2], histos[3], "v_mass/"+prefix_m+"_v_mass", suffix_m, "H_T [GeV]", "", canvas, false, label1_m, label2_m);
    plot(histos[4], histos[5], "ht/"+prefix_m+"_ht", suffix_m, "V mass [GeV]", "", canvas, false, label1_m, label2_m);
    plot(histos[6], histos[7], "nb/"+prefix_m+"_nb", suffix_m, "LHE_nB", "", canvas, false, label1_m, label2_m);
    plot(histos[8], histos[9], "nShower/"+prefix_m+"_nShower", suffix_m, "N Shower B Hadrons", "", canvas, false, label1_m, label2_m);
    plot(histos[10], histos[11], "nadd/"+prefix_m+"_nadd", suffix_m, "Additional Reco Jets", "", canvas, true, label1_m, label2_m);

    plot(histos[12], histos[13], "nbLO/"+prefix_m+"_nbLO", suffix_m, "nb x pt bin LO", "", canvas, false, label1_m, label2_m);
    plot(histos[14], histos[15], "nptLO/"+prefix_m+"_nptLO", suffix_m, "ht bin LO", "", canvas, false, label1_m, label2_m);
    plot(histos[16], histos[17], "nbNLO/"+prefix_m+"_nbNLO", suffix_m, "nb bin NLO", "", canvas, false, label1_m, label2_m);
    plot(histos[18], histos[19], "nptNLO/"+prefix_m+"_nptNLO", suffix_m, "pt bin NLO", "", canvas, false, label1_m, label2_m);
}*/
