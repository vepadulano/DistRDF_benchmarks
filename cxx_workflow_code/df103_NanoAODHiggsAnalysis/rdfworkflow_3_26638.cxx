

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RResultHandle.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <vector>
#include <tuple>


namespace DistRDF_Internal {

using CppWorkflowResult = std::tuple<std::vector<ROOT::RDF::RResultHandle>,
                          std::vector<std::string>,
                          std::vector<ROOT::RDF::RNode>>;

CppWorkflowResult __RDF_WORKFLOW_FUNCTION__3(ROOT::RDF::RNode &rdf0)
{
  std::vector<ROOT::RDF::RResultHandle> result_handles;
  std::vector<std::string> result_types;
  std::vector<ROOT::RDF::RNode> output_nodes;

  // To make Snapshots lazy
  ROOT::RDF::RSnapshotOptions lazy_options;
  lazy_options.fLazy = true;




  auto rdf1 = rdf0.Filter("nElectron>=4", "At least our electrons");
  auto rdf2 = rdf1.Filter("All(abs(Electron_pfRelIso03_all)<0.40)", "Require good isolation");
  auto rdf3 = rdf2.Filter("All(Electron_pt>7) && All(abs(Electron_eta)<2.5)", "Good Electron kinematics");
  auto rdf4 = rdf3.Define("Electron_ip3d", "sqrt(Electron_dxy*Electron_dxy + Electron_dz*Electron_dz)");
  auto rdf5 = rdf4.Define("Electron_sip3d", "Electron_ip3d/sqrt(Electron_dxyErr*Electron_dxyErr + Electron_dzErr*Electron_dzErr)");
  auto rdf6 = rdf5.Filter("All(Electron_sip3d<4) && All(abs(Electron_dxy)<0.5) && All(abs(Electron_dz)<1.0)", "Track close to primary vertex with small uncertainty");
  auto rdf7 = rdf6.Filter("nElectron==4 && Sum(Electron_charge==1)==2 && Sum(Electron_charge==-1)==2", "Two positive and two negative electrons");
  auto rdf8 = rdf7.Define("Z_idx", "reco_zz_to_4l(Electron_pt, Electron_eta, Electron_phi, Electron_mass, Electron_charge)");
  auto rdf9 = rdf8.Filter("filter_z_dr(Z_idx, Electron_eta, Electron_phi)", "Delta R separation of Electrons building Z system");
  auto rdf10 = rdf9.Define("Z_mass", "compute_z_masses_4l(Z_idx, Electron_pt, Electron_eta, Electron_phi, Electron_mass)");
  auto rdf11 = rdf10.Filter("Z_mass[0] > 40 && Z_mass[0] < 120", "Mass of first Z candidate in [40, 120]");
  auto rdf12 = rdf11.Filter("Z_mass[1] > 12 && Z_mass[1] < 120", "Mass of second Z candidate in [12, 120]");
  auto rdf13 = rdf12.Define("H_mass", "compute_higgs_mass_4l(Z_idx, Electron_pt, Electron_eta, Electron_phi, Electron_mass)");
  auto rdf14 = rdf13.Define("weight", "0.0008243923225577064");
  auto res_ptr0 = rdf14.Histo1D({"h_bkg_4el","",36,70,180}, "H_mass", "weight");
  result_handles.emplace_back(res_ptr0);
  auto c0 = TClass::GetClass(typeid(res_ptr0));
  if (c0 == nullptr)
    throw std::runtime_error(
    "Cannot get type of result 0 of action Histo1D during "
    "generation of RDF C++ workflow");
  result_types.emplace_back(c0->GetName());

  return { result_handles, result_types, output_nodes };
}

}
