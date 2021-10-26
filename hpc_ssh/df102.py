import os
import sys
import ROOT

RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame

from dask.distributed import Client, SSHCluster
import dask

def createSSHCluster(nodes, nprocs):
    parsed_nodes = nodes.split(',')
    scheduler = parsed_nodes[:1]
    workers = parsed_nodes[1:]
    
    print("List of nodes: scheduler ({}) and workers ({})".format(scheduler, workers))

    cluster = SSHCluster(scheduler + workers, connect_options={ "known_hosts": None }, worker_options={ "nprocs" : nprocs, "nthreads": 1, "memory_limit" : "10GB", "local_directory" : "/tmp/etejedor" })
    client = Client(cluster)

    return cluster, client

def createDataset(num_files):
    dataset = [ 'Run2012BC_DoubleMuParked_Muons.root' ] + [ 'Run2012BC_DoubleMuParked_Muons_{}.root'.format(i) for i in range(1, num_files) ]
    print("Dataset: {}".format(dataset))
    return dataset

def run(path, dataset, npartitions, client):
    # Create dataframe from NanoAOD files
    files = [ os.path.join(path, data_file) for data_file in dataset ]
    df = RDataFrame("Events", files, npartitions=npartitions, daskclient=client)

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
    print("\tEvent loop dimuon_data, npartitions {}:".format(npartitions), elapsed, "s")
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

if __name__ == "__main__":
    nodes = sys.argv[1]
    nprocs = int(sys.argv[2])
    nfiles = int(sys.argv[3])
    npartitions = int(sys.argv[4])
    ntests = int(sys.argv[5])

    cluster, client = createSSHCluster(nodes, nprocs)
    dataset = createDataset(nfiles)
    path = '/hpcscratch/user/etejedor/DistRDF_tests/data'

    for _ in range(ntests):
        run(path, dataset, npartitions, client)
