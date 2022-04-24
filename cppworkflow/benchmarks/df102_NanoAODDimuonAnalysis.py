# DistRDF version of the RDataFrame benchmark df102
# - Used DistRDF RDataFrame constructor for Spark
# - Added npartitions parameter for Spark RDataFrame constructor
# - Report was removed as it is not supported yet by DistRDF

import os
import ROOT

RDataFrame = ROOT.RDF.Experimental.Distributed.Spark.RDataFrame

def run(path, npartitions):
    # Create dataframe from NanoAOD files
    files = os.path.join(path, "Run2012BC_DoubleMuParked_Muons.root")
    df = RDataFrame("Events", files, npartitions=npartitions)

    # For simplicity, select only events with exactly two muons and require opposite charge
    df_2mu = df.Filter("nMuon == 2", "Events with exactly two muons")
    df_os = df_2mu.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge")

    # Compute invariant mass of the dimuon system
    df_mass = df_os.Define("Dimuon_mass", "InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)")

    # Make histogram of dimuon mass spectrum
    h = df_mass.Histo1D(("Dimuon_mass", "Dimuon_mass", 30000, 0.25, 300), "Dimuon_mass")

    # Produce plot
    ROOT.gStyle.SetOptStat(0); ROOT.gStyle.SetTextFont(42)
    c = ROOT.TCanvas("c", "", 800, 700)
    c.SetLogx(); c.SetLogy()

    watch = ROOT.TStopwatch()
    h.SetTitle("")
    elapsed = watch.RealTime()
    print("\tEvent loop dimuon_data:", elapsed, "s")
    h.GetXaxis().SetTitle("m_{#mu#mu} (GeV)"); h.GetXaxis().SetTitleSize(0.04)
    h.GetYaxis().SetTitle("N_{Events}"); h.GetYaxis().SetTitleSize(0.04)
    h.Draw()

    label = ROOT.TLatex(); label.SetNDC(True)
    label.DrawLatex(0.175, 0.740, "#eta")
    label.DrawLatex(0.205, 0.775, "#rho,#omega")
    label.DrawLatex(0.270, 0.740, "#phi")
    label.DrawLatex(0.400, 0.800, "J/#psi")
    label.DrawLatex(0.415, 0.670, "#psi'")
    label.DrawLatex(0.485, 0.700, "Y(1,2,3S)")
    label.DrawLatex(0.755, 0.680, "Z")
    label.SetTextSize(0.040); label.DrawLatex(0.100, 0.920, "#bf{CMS Open Data}")
    label.SetTextSize(0.030); label.DrawLatex(0.630, 0.920, "#sqrt{s} = 8 TeV, L_{int} = 11.6 fb^{-1}")

    c.SaveAs("dimuon_spectrum.pdf")
