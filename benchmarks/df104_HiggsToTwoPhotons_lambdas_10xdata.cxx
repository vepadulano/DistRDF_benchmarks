#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"
#include "ROOT/RResultPtr.hxx"
#include "Math/Vector4D.h"
#include "TH1F.h"
#include "TF1.h"
#include "TPad.h"
#include "TCanvas.h"
#include "TLatex.h"
#include "TLegend.h"
#include <TStopwatch.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace ROOT::VecOps;
using rvec_f = const RVec<float> &;
using rvec_b = const RVec<bool> &;
using rvec_i = const RVec<int> &;

// Function that computes the invariant mass of the diphoton system
float ComputeInvariantMass(rvec_f pt, rvec_f eta, rvec_f phi, rvec_f e) {
    ROOT::Math::PtEtaPhiEVector p1(pt[0], eta[0], phi[0], e[0]);
    ROOT::Math::PtEtaPhiEVector p2(pt[1], eta[1], phi[1], e[1]);
    return (p1 + p2).mass() / 1000.0;
}

void run() {
    std::string path = "data";

    // Create a ROOT dataframe for each dataset
    std::unordered_map<std::string,ROOT::RDataFrame> df;

    // Paths to gamma gamma dataset (10x)
    std::vector<std::string> gamgam_files;
    gamgam_files.reserve(4);
    for (const auto &letter : { "A", "B", "C", "D" }) {
        std::ostringstream oss;
        oss << "/data_" << letter << ".GamGam.root";
        gamgam_files.push_back(path + oss.str());
    }
    std::vector<std::string> gamgam_files_10x;
    gamgam_files_10x.reserve(gamgam_files.size()*10);
    for(int i = 0; i < 10; i++){
        std::copy(gamgam_files.begin(), gamgam_files.end(), std::back_inserter(gamgam_files_10x));
    }

    // Paths to ggH dataset (10x)
    std::vector<std::string> ggH_files_10x(10, path + "/mc_343981.ggH125_gamgam.GamGam.root");
    // Paths to vbf dataset (10x)
    std::vector<std::string> VBF_files_10x(10, path + "/mc_345041.VBFH125_gamgam.GamGam.root");

    ROOT::RDataFrame df_data("mini", gamgam_files_10x);
    ROOT::RDataFrame df_ggH("mini", ggH_files_10x);
    ROOT::RDataFrame df_VBF("mini", VBF_files_10x);

    // Apply scale factors and MC weight for simulated events and a weight of 1 for the data
    auto weight = [](float scaleFactor_PHOTON, float scaleFactor_PhotonTRIGGER, float scaleFactor_PILEUP, float mcWeight) { return scaleFactor_PHOTON * scaleFactor_PhotonTRIGGER * scaleFactor_PILEUP * mcWeight; };
    std::vector<std::string> weight_cols = { "scaleFactor_PHOTON", "scaleFactor_PhotonTRIGGER", "scaleFactor_PILEUP", "mcWeight" };
    auto df_ggH_weight = df_ggH.Define("weight", weight, weight_cols);
    auto df_VBF_weight = df_VBF.Define("weight", weight, weight_cols);
    auto df_data_weight = df_data.Define("weight", []() { return 1.0f; });

    std::vector<std::pair<std::string,ROOT::RDF::RNode>> processes = { { "ggH", df_ggH_weight }, { "VBF", df_VBF_weight }, { "data", df_data_weight } };
    std::unordered_map<std::string,ROOT::RDF::RResultPtr<TH1D>> hists;
    for (const auto &p : processes) {
        auto name = p.first.c_str();
        auto df = p.second;

        // Select the events for the analysis

        // Apply preselection cut on photon trigger
        auto df_trigP = df.Filter([](bool trigP) { return trigP; }, {"trigP"});

        // Find two good muons with tight ID, pt > 25 GeV and not in the transition region between barrel and encap
        auto df_2gp = df_trigP.Define("goodphotons", [](rvec_b photon_isTightID, rvec_f photon_pt, rvec_f photon_eta) { return photon_isTightID && (photon_pt > 25000) && (abs(photon_eta) < 2.37) && ((abs(photon_eta) < 1.37) || (abs(photon_eta) > 1.52)); }, { "photon_isTightID", "photon_pt", "photon_eta" } )
                              .Filter([](rvec_i goodphotons) { return Sum(goodphotons) == 2; }, {"goodphotons"});

        // Take only isolated photons
        auto df_isolated = df_2gp.Filter([](rvec_f photon_ptcone30, rvec_f photon_pt, rvec_i goodphotons) { return Sum(photon_ptcone30[goodphotons] / photon_pt[goodphotons] < 0.065) == 2; }, { "photon_ptcone30", "photon_pt", "goodphotons" })
                                 .Filter([](rvec_f photon_etcone20, rvec_f photon_pt, rvec_i goodphotons) { return Sum(photon_etcone20[goodphotons] / photon_pt[goodphotons] < 0.065) == 2; }, { "photon_etcone20", "photon_pt", "goodphotons" });

        // Define a new column with the invariant mass and perform final event selection

        // Make four vectors and compute invariant mass
        auto df_im = df_isolated.Define("m_yy", [](rvec_f photon_pt, rvec_f photon_eta, rvec_f photon_phi, rvec_f photon_E, rvec_i goodphotons) { return ComputeInvariantMass(photon_pt[goodphotons], photon_eta[goodphotons], photon_phi[goodphotons], photon_E[goodphotons]); }, {"photon_pt", "photon_eta", "photon_phi", "photon_E", "goodphotons"});

        // Make additional kinematic cuts and select mass window
        auto df_kmc = df_im.Filter([](rvec_f photon_pt, rvec_i goodphotons, float m_yy) { return photon_pt[goodphotons][0] / 1000.0 / m_yy > 0.35; }, {"photon_pt", "goodphotons", "m_yy"})
                           .Filter([](rvec_f photon_pt, rvec_i goodphotons, float m_yy) { return photon_pt[goodphotons][1] / 1000.0 / m_yy > 0.25; }, {"photon_pt", "goodphotons", "m_yy"})
                           .Filter([](float m_yy) { return m_yy > 105 && m_yy < 160; }, {"m_yy"});

        // Book histogram of the invariant mass with this selection
        hists[name] = df_kmc.Histo1D<float,float>({ name, "Diphoton invariant mass; m_{#gamma#gamma} [GeV];Events", 30, 105, 160 }, "m_yy", "weight");
    }

    // Run the event loop
    TStopwatch watch;
    auto ggh  = *hists["ggH"];
    double elapsed_ggh{watch.RealTime()};
    watch.Start();
    auto vbf  = *hists["VBF"];
    double elapsed_vbf{watch.RealTime()};
    watch.Start();
    auto data = *hists["data"];
    double elapsed_data{watch.RealTime()};

    std::cout << "Event loop ggH: "  << std::fixed << std::setprecision(2) << elapsed_ggh << " s\n";
    std::cout << "Event loop VBF: "  << std::fixed << std::setprecision(2) << elapsed_vbf << " s\n";
    std::cout << "Event loop data: " << std::fixed << std::setprecision(2) << elapsed_data << " s\n";

    // Create the plot

    // Set styles
    gROOT->SetStyle("ATLAS");

    // Create canvas with pads for main plot and data/MC ratio
    TCanvas c("c", "", 700, 750);

    TPad upper_pad("upper_pad", "", 0, 0.35, 1, 1);
    TPad lower_pad("lower_pad", "", 0, 0, 1, 0.35);
    for (auto p : { &upper_pad, &lower_pad }) {
        p->SetLeftMargin(0.14);
        p->SetRightMargin(0.05);
        p->SetTickx(false);
        p->SetTicky(false);
    }
    upper_pad.SetBottomMargin(0);
    lower_pad.SetTopMargin(0);
    lower_pad.SetBottomMargin(0.3);

    upper_pad.Draw();
    lower_pad.Draw();

    // Fit signal + background model to data
    upper_pad.cd();
    TF1 fit("fit", "([0]+[1]*x+[2]*x^2+[3]*x^3)+[4]*exp(-0.5*((x-[5])/[6])^2)", 105, 160);
    fit.FixParameter(5, 125.0);
    fit.FixParameter(4, 119.1);
    fit.FixParameter(6, 2.39);
    fit.SetLineColor(2);
    fit.SetLineStyle(1);
    fit.SetLineWidth(2);
    data.Fit("fit", "", "E SAME", 105, 160);
    fit.Draw("SAME");

    // Draw background
    TF1 bkg("bkg", "([0]+[1]*x+[2]*x^2+[3]*x^3)", 105, 160);
    for (int i = 0; i < 4; ++i)
        bkg.SetParameter(i, fit.GetParameter(i));
    bkg.SetLineColor(4);
    bkg.SetLineStyle(2);
    bkg.SetLineWidth(2);
    bkg.Draw("SAME");

    // Draw data
    data.SetMarkerStyle(20);
    data.SetMarkerSize(1.2);
    data.SetLineWidth(2);
    data.SetLineColor(kBlack);
    data.Draw("E SAME");
    data.SetMinimum(1e-3);
    data.SetMaximum(8e3);
    data.GetYaxis()->SetLabelSize(0.045);
    data.GetYaxis()->SetTitleSize(0.05);
    data.SetStats(0);
    data.SetTitle("");

    // Scale simulated events with luminosity * cross-section / sum of weights
    // and merge to single Higgs signal
    const auto lumi = 10064.0;
    ggh.Scale(lumi * 0.102 / 55922617.6297);
    vbf.Scale(lumi * 0.008518764 / 3441426.13711);
    auto higgs = *(TH1D*)(ggh.Clone());
    higgs.Add(&vbf);
    higgs.Draw("HIST SAME");

    // Draw ratio
    lower_pad.cd();

    TF1 ratiobkg("zero", "0", 105, 160);
    ratiobkg.SetLineColor(4);
    ratiobkg.SetLineStyle(2);
    ratiobkg.SetLineWidth(2);
    ratiobkg.SetMinimum(-125);
    ratiobkg.SetMaximum(250);
    ratiobkg.GetXaxis()->SetLabelSize(0.08);
    ratiobkg.GetXaxis()->SetTitleSize(0.12);
    ratiobkg.GetXaxis()->SetTitleOffset(1.0);
    ratiobkg.GetYaxis()->SetLabelSize(0.08);
    ratiobkg.GetYaxis()->SetTitleSize(0.09);
    ratiobkg.GetYaxis()->SetTitle("Data - Bkg.");
    ratiobkg.GetYaxis()->CenterTitle();
    ratiobkg.GetYaxis()->SetTitleOffset(0.7);
    ratiobkg.GetYaxis()->SetNdivisions(503, false);
    ratiobkg.GetYaxis()->ChangeLabel(-1, -1, 0);
    ratiobkg.GetXaxis()->SetTitle("m_{#gamma#gamma} [GeV]");
    ratiobkg.Draw();

    TH1F ratiosig("ratiosig", "ratiosig", 5500, 105, 160);
    ratiosig.Eval(&fit);
    ratiosig.SetLineColor(2);
    ratiosig.SetLineStyle(1);
    ratiosig.SetLineWidth(2);
    ratiosig.Add(&bkg, -1);
    ratiosig.Draw("SAME");

    auto ratiodata = *(TH1D*)(data.Clone());
    ratiodata.Add(&bkg, -1);
    ratiodata.Draw("E SAME");
    for (int i=1; i < data.GetNbinsX(); ++i)
        ratiodata.SetBinError(i, data.GetBinError(i));

    // Add legend
    upper_pad.cd();
    TLegend legend(0.55, 0.55, 0.89, 0.85);
    legend.SetTextFont(42);
    legend.SetFillStyle(0);
    legend.SetBorderSize(0);
    legend.SetTextSize(0.05);
    legend.SetTextAlign(32);
    legend.AddEntry(&data, "Data" ,"lep");
    legend.AddEntry(&bkg, "Background", "l");
    legend.AddEntry(&fit, "Signal + Bkg.", "l");
    legend.AddEntry(&higgs, "Signal", "l");
    legend.Draw("SAME");

    // Add ATLAS label
    TLatex text;
    text.SetNDC();
    text.SetTextFont(72);
    text.SetTextSize(0.05);
    text.DrawLatex(0.18, 0.84, "ATLAS");
    text.SetTextFont(42);
    text.DrawLatex(0.18 + 0.13, 0.84, "Open Data");
    text.SetTextSize(0.04);
    text.DrawLatex(0.18, 0.78, "#sqrt{s} = 13 TeV, 10 fb^{-1}");

    // Save the plot
    c.SaveAs("HiggsToTwoPhotons.pdf");
}

int main() {
    run();
}
