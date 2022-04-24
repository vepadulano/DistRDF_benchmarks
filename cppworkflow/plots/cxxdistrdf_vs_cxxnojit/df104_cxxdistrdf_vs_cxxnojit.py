# Times for DistRDF Python and C++ optimized modes

import ROOT

df104 = ROOT.RDF.MakeCsvDataFrame("csv/df104_results.csv")

df104_times_cxxdistrdf = {}
df104_times_cxxnojit = {}

df104_opt = df104.Filter('benchmark_name == "df104"').Filter('build_type == "opt"')
df104_opt_cxxdistrdf = df104_opt.Filter('test_type == "distrdf_cpp"') 
df104_opt_cxxnojit   = df104_opt.Filter('test_type == "cpp_lambdas"')

datasets = [ 'ggh', 'vbf', 'data', 'all_datasets' ]
time_types = [ 'event_loop', 'jit', 'compile_macro', 'compilation', 'getvalue_trigger' ]

for dataset in datasets:
    for df,times_dict in (df104_opt_cxxdistrdf,df104_times_cxxdistrdf), (df104_opt_cxxnojit,df104_times_cxxnojit):
        df_dataset = df.Filter('dataset_name == "{}"'.format(dataset))
        times_dict[dataset] = {}
        for time_type in time_types:
            times_dict[dataset][time_type] = df_dataset.Filter('time_type == "{}"'.format(time_type)) \
                                                       .Mean('time') \
                                                       .GetValue()

# Print the average compilation time for the C++ no JIT
print("Compilation time CxxNoJIT, all datasets: {} s".format(df104_times_cxxnojit['all_datasets']['compilation']))

# Set 'rest' to be "time that is not jitting nor running event loop nor running CompileMacro (if it applies)"
for times_dict in df104_times_cxxdistrdf, df104_times_cxxnojit:
    for dataset in datasets:
        times_dict[dataset]['rest'] = times_dict[dataset]['getvalue_trigger'] - times_dict[dataset]['jit'] - times_dict[dataset]['event_loop'] - times_dict[dataset]['compile_macro']

print(df104_times_cxxdistrdf)
print(df104_times_cxxnojit)

c = ROOT.TCanvas()

hs = ROOT.THStack("hs", "df104 benchmark")

n = 10
hists = {}
colors = [ ROOT.kBlue-2, ROOT.kOrange+1, ROOT.kRed, ROOT.kGreen+1 ]
time_types = [ 'event_loop', 'jit', 'compile_macro', 'rest' ]
for time_type, color in zip(time_types,colors):
    h = ROOT.TH1D(time_type,"",n,0,n)
    hists[time_type] = h

    t = []
    for dataset in datasets:
        t.append(0)
        t.append(df104_times_cxxdistrdf[dataset][time_type])
        t.append(df104_times_cxxnojit[dataset][time_type])
    t.append(0)
    for i,elem in enumerate(t):
        h.SetBinContent(i+1, elem)

    hs.Add(h, "bar")
    h.SetBarWidth(0.8)
    h.SetBarOffset(0.1)
    h.SetFillColor(color)

hs.Draw()

# Setup of X axis
ax = hs.GetXaxis()
ax.ChangeLabel(1,-1,0)
ax.ChangeLabel(2,-1,0)
ax.ChangeLabel(3,-1,0)
ax.ChangeLabel(4,-1,0)
ax.ChangeLabel(5,-1,0)
ax.ChangeLabel(6,-1,0)
ax.ChangeLabel(7,-1,0)
ax.ChangeLabel(8,-1,0)
ax.ChangeLabel(9,-1,0)
ax.ChangeLabel(-1,-1,0)
ax.ChangeLabel(-2,-1,0)

ax.SetTickLength(0.) # Remove ticks

#ax.SetLabelOffset(0.04)

# Setup of Y axis
ay = hs.GetYaxis()
ay.SetTitle("time (s)")
ay.CenterTitle()

# Add legend
legend = ROOT.TLegend(0.4, 0.7, 0.57, 0.85)
#legend.SetFillColor(0)
#legend.SetBorderSize(0)
legend.SetTextSize(0.03)
legend.AddEntry(hists['event_loop'], "Event loop", "f")
legend.AddEntry(hists['jit'], "JIT", "f")
legend.AddEntry(hists['compile_macro'], "CompileMacro", "f")
legend.AddEntry(hists['rest'], "Other", "f")
legend.Draw()

#c.BuildLegend()
c.Modified()
c.Update()
