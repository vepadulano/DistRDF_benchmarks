

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RResultHandle.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <vector>
#include <tuple>


namespace DistRDF_Internal {

using CppWorkflowResult = std::tuple<std::vector<ROOT::RDF::RResultHandle>,
                          std::vector<std::string>,
                          std::vector<ROOT::RDF::RNode>>;

CppWorkflowResult __RDF_WORKFLOW_FUNCTION__0(ROOT::RDF::RNode &rdf0)
{
  std::vector<ROOT::RDF::RResultHandle> result_handles;
  std::vector<std::string> result_types;
  std::vector<ROOT::RDF::RNode> output_nodes;

  // To make Snapshots lazy
  ROOT::RDF::RSnapshotOptions lazy_options;
  lazy_options.fLazy = true;




  auto rdf1 = rdf0.Filter("nMuon == 2", "Events with exactly two muons");
  auto rdf2 = rdf1.Filter("Muon_charge[0] != Muon_charge[1]", "Muons with opposite charge");
  auto rdf3 = rdf2.Define("Dimuon_mass", "InvariantMass(Muon_pt, Muon_eta, Muon_phi, Muon_mass)");
  auto res_ptr0 = rdf3.Histo1D({"Dimuon_mass","Dimuon_mass",30000,0.25,300}, "Dimuon_mass");
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
