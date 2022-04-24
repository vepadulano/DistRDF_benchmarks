#include <iostream>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RVec.hxx"
#include "TCanvas.h"
#include "TH1D.h"
#include "TLatex.h"
#include "TStyle.h"
#include "TStopwatch.h"
#include <ROOT/RLogger.hxx>

auto verbosity = ROOT::Experimental::RLogScopedVerbosity(ROOT::Detail::RDF::RDFLogChannel(), ROOT::Experimental::ELogLevel::kInfo);

void dimuonSpectrum() {
    // Create dataframe from NanoAOD files
    std::vector<std::string> files(10, "data/Run2012BC_DoubleMuParked_Muons.root");
    ROOT::RDataFrame df("Events", files);

    // Select events with exactly two muons
    auto df_2mu = df.Filter([](unsigned int nMuon){ return nMuon == 2; }, {"nMuon"}, "Events with exactly two muons");

    // Select events with two muons of opposite charge
    auto df_os = df_2mu.Filter([](const ROOT::VecOps::RVec<int> &muonCharge){ return muonCharge[0] != muonCharge[1]; },
                               {"Muon_charge"}, "Muons with opposite charge");

    // Compute invariant mass of the dimuon system
    auto df_mass = df_os.Define("Dimuon_mass", ROOT::VecOps::InvariantMass<float>,
                                {"Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass"});

    // Book histogram of dimuon mass spectrum
    const auto bins = 30000; // Number of bins in the histogram
    const auto low = 0.25; // Lower edge of the histogram
    const auto up = 300.0; // Upper edge of the histogram
    auto hist = df_mass.Histo1D<float>({"Dimuon_mass", "Dimuon_mass", bins, low, up}, "Dimuon_mass");

    // Create canvas for plotting
    gStyle->SetOptStat(0);
    gStyle->SetTextFont(42);
    auto c = new TCanvas("c", "", 800, 700);
    c->SetLogx();
    c->SetLogy();

    // Trigger event loop execution and time it
    TStopwatch t;
    hist->SetTitle("");
    double elapsed{t.RealTime()};
    // Store elapsed time in a csv file
    std::cout << "C++ Lambda Event Loop " << std::fixed << std::setprecision(2) << elapsed << " s\n";
    // Draw histogram
    hist->GetXaxis()->SetTitle("m_{#mu#mu} (GeV)");
    hist->GetXaxis()->SetTitleSize(0.04);
    hist->GetYaxis()->SetTitle("N_{Events}");
    hist->GetYaxis()->SetTitleSize(0.04);
    hist->DrawClone();

    // Draw labels
    TLatex label;
    label.SetTextAlign(22);
    label.DrawLatex(0.55, 3.0e4, "#eta");
    label.DrawLatex(0.77, 7.0e4, "#rho,#omega");
    label.DrawLatex(1.20, 4.0e4, "#phi");
    label.DrawLatex(4.40, 1.0e5, "J/#psi");
    label.DrawLatex(4.60, 1.0e4, "#psi'");
    label.DrawLatex(12.0, 2.0e4, "Y(1,2,3S)");
    label.DrawLatex(91.0, 1.5e4, "Z");
    label.SetNDC(true);
    label.SetTextAlign(11);
    label.SetTextSize(0.04);
    label.DrawLatex(0.10, 0.92, "#bf{CMS Open Data}");
    label.SetTextAlign(31);
    label.DrawLatex(0.90, 0.92, "#sqrt{s} = 8 TeV, L_{int} = 11.6 fb^{-1}");

}

int main() {
    dimuonSpectrum();
}
