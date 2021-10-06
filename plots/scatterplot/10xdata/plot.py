import ROOT

import os
import pandas

DATA_DIR = "../../../table_results/10xdata/"


def combine_datasets():
    """Combine datasets of different benchmarks and save to csv"""
    df102 = pandas.read_csv(DATA_DIR + "df102_results.csv")
    df103 = pandas.read_csv(DATA_DIR + "df103_results.csv")
    df104 = pandas.read_csv(DATA_DIR + "df104_results.csv")
    combined = pandas.concat([df102, df103, df104])
    combined.to_csv("combined_results.csv", index=None)


def create_dataset_toplot():
    """Manipulate combined results into final dataset for plotting"""

    results = pandas.read_csv("combined_results.csv")
    results["test_type_full"] = results.build_type + "_" + results.test_type
    getvalue_trigger_results = results[results["time_type"] == "getvalue_trigger"]
    toplot = getvalue_trigger_results.groupby(["dataset_name", "test_type_full"]).mean().reset_index()
    toplot["dataset_code"] = pandas.factorize(toplot["dataset_name"])[0] + 1
    toplot.to_csv("toplot.csv", index=None)


def scatterplot():
    """Create the scatter plot of runtimes of all computation graphs according to test type"""
    rdf = ROOT.RDF.MakeCsvDataFrame("toplot.csv")
    test_types = ["no_op_distrdf_py", "opt_cpp_lambdas", "opt_distrdf_cpp", "opt_distrdf_py"]
    actions = [rdf.Filter("test_type_full == \"{}\"".format(test_type)).Graph("dataset_code", "time")
               for test_type in test_types]
    graphs = [action.GetValue() for action in actions]

    # Graphics
    height = 1000
    width = 1000
    gtitle = "Time to plot - all benchmarks"
    xtitle = "Dataset"
    ytitle = "Time [s]"

    ROOT.gStyle.SetOptTitle(ROOT.kFALSE)
    d = ROOT.TCanvas("d", "", width, height)

    # Create MultiGraph
    mg = ROOT.TMultiGraph()
    colors = [ROOT.kBlue-2, ROOT.kOrange+1, ROOT.kRed, ROOT.kGreen+1]
    for graph, name, color in zip(graphs, test_types, colors):
        graph.SetLineWidth(0)
        graph.SetMarkerColor(color)
        graph.SetTitle(name)
        graph.SetMarkerStyle(ROOT.kFullCircle)
        graph.SetMarkerSize(2)
        mg.Add(graph, "P")

    mg.SetTitle("{};{};{}".format(gtitle, xtitle, ytitle))
    mg.Draw("A")

    d.BuildLegend(0.1, 0.7, 0.5, 0.9)
    d.SaveAs("scatterplot_alldf.png")


def cleanup():
    """Remove temporary files"""
    os.remove("combined_results.csv")
    os.remove("toplot.csv")


if __name__ == "__main__":
    combine_datasets()
    create_dataset_toplot()
    scatterplot()
    cleanup()
