# DistRDF py times for opt and no opt modes

import ROOT

df103 = ROOT.RDF.MakeCsvDataFrame("csv/df103_results.csv")

df103_times_opt = {}
df103_times_noop = {}

df103_distrdfpy = df103.Filter('benchmark_name == "df103"').Filter('test_type == "distrdf_py"')
df103_distrdfpy_opt = df103_distrdfpy.Filter('build_type == "opt"')
df103_distrdfpy_noopt = df103_distrdfpy.Filter('build_type == "no_op"')

datasets = [ 'df_sig_4l', 'df_bkg_4mu', 'df_data_doublemu', 'df_bkg_4el', 'df_data_doubleel'  ] 
time_types = [ 'event_loop', 'jit', 'getvalue_trigger' ]

for dataset in datasets:
    for df,times_dict in (df103_distrdfpy_opt,df103_times_opt), (df103_distrdfpy_noopt,df103_times_noop):
        df_dataset = df.Filter('dataset_name == "{}"'.format(dataset))
        times_dict[dataset] = {}
        for time_type in time_types:
            times_dict[dataset][time_type] = df_dataset.Filter('time_type == "{}"'.format(time_type)) \
                                                       .Mean('time') \
                                                       .GetValue()

# Set 'rest' to be "time that is not jitting nor running event loop"
for times_dict in df103_times_opt, df103_times_noop:
    for dataset in datasets:
        times_dict[dataset]['rest'] = times_dict[dataset]['getvalue_trigger'] - times_dict[dataset]['jit'] - times_dict[dataset]['event_loop']

print(df103_times_opt)
print(df103_times_noop)

c = ROOT.TCanvas()

hs = ROOT.THStack("hs", "df103 benchmark")

n = 16
hists = {}
colors = [ ROOT.kBlue-2, ROOT.kOrange+1, ROOT.kGreen+1 ]
time_types = [ 'event_loop', 'jit', 'rest' ]
for time_type, color in zip(time_types,colors):
    h = ROOT.TH1D(time_type,"",n,0,n)
    hists[time_type] = h

    t = []
    for dataset in datasets:
        t.append(0)
        t.append(df103_times_noop[dataset][time_type])
        t.append(df103_times_opt[dataset][time_type])
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
ax.ChangeLabel(10,-1,0)
ax.ChangeLabel(11,-1,0)
ax.ChangeLabel(12,-1,0)
#ax.ChangeLabel(4,-1,-1,-1,-1,-1,"no opt")
#ax.ChangeLabel(5,45,-1,-1,-1,-1,"graph 1")
#ax.ChangeLabel(6,-1,-1,-1,-1,-1,"opt")
#ax.ChangeLabel(7,-1,0)
ax.ChangeLabel(-1,-1,0)
ax.ChangeLabel(-2,-1,0)

ax.SetTickLength(0.) # Remove ticks

#ax.SetLabelOffset(0.04)

# Setup of Y axis
ay = hs.GetYaxis()
ay.SetTitle("time (s)")
ay.CenterTitle()

# Add legend
legend = ROOT.TLegend(0.7, 0.7, 0.85, 0.85)
#legend.SetFillColor(0)
#legend.SetBorderSize(0)
legend.SetTextSize(0.03)
legend.AddEntry(hists['event_loop'], "Event loop", "f")
legend.AddEntry(hists['jit'], "JIT", "f")
legend.AddEntry(hists['rest'], "Other", "f")
legend.Draw()

#c.BuildLegend()
c.Modified()
c.Update()
