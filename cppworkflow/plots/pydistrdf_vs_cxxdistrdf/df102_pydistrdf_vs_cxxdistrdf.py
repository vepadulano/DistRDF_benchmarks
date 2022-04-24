# Times for DistRDF Python and C++ optimized modes

import ROOT

df102 = ROOT.RDF.MakeCsvDataFrame("csv/df102_results.csv")

df102_times_pydistrdf = {}
df102_times_cxxdistrdf = {}

df102_opt = df102.Filter('benchmark_name == "df102"').Filter('build_type == "opt"')
df102_opt_pydistrdf  = df102_opt.Filter('test_type == "distrdf_py"')
df102_opt_cxxdistrdf = df102_opt.Filter('test_type == "distrdf_cpp"') 

time_types = [ 'event_loop', 'jit', 'compile_macro', 'getvalue_trigger' ]

for df,times_dict in (df102_opt_pydistrdf,df102_times_pydistrdf), (df102_opt_cxxdistrdf,df102_times_cxxdistrdf):
    for time_type in time_types:
        times_dict[time_type] = df.Filter('time_type == "{}"'.format(time_type)) \
                                  .Mean('time') \
                                  .GetValue()

# Set 'rest' to be "time that is not jitting nor running event loop nor running CompileMacro (if it applies)"
for times_dict in df102_times_pydistrdf, df102_times_cxxdistrdf:
    times_dict['rest'] = times_dict['getvalue_trigger'] - times_dict['jit'] - times_dict['event_loop'] - times_dict['compile_macro']

print(df102_times_pydistrdf)
print(df102_times_cxxdistrdf)

c = ROOT.TCanvas()

hs = ROOT.THStack("hs", "df102 benchmark")

n = 4 
hists = {}
colors = [ ROOT.kBlue-2, ROOT.kOrange+1, ROOT.kRed, ROOT.kGreen+1 ]
time_types = [ 'event_loop', 'jit', 'compile_macro', 'rest' ]
for time_type, color in zip(time_types,colors):
    h = ROOT.TH1D(time_type,"",n,0,n)
    hists[time_type] = h

    t = []
    t.append(0)
    t.append(df102_times_pydistrdf[time_type])
    t.append(df102_times_cxxdistrdf[time_type])
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
ax.ChangeLabel(4,-1,-1,-1,-1,-1,"PyDistRDF")
#ax.ChangeLabel(5,-1,-1,23,-1,-1,"graph 1")
ax.ChangeLabel(5,-1,0)
ax.ChangeLabel(6,-1,-1,-1,-1,-1,"CxxDistRDF")
ax.ChangeLabel(7,-1,0)
ax.ChangeLabel(-1,-1,0)
ax.ChangeLabel(-2,-1,0)

ax.SetTickLength(0.) # Remove ticks

ax.SetLabelOffset(0.04)

# Setup of Y axis
ay = hs.GetYaxis()
ay.SetTitle("time (s)")
ay.CenterTitle()

# Add legend
legend = ROOT.TLegend(0.7, 0.7, 0.87, 0.85)
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
