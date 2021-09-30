

#include "ROOT/RDataFrame.hxx"
#include "ROOT/RResultHandle.hxx"
#include "ROOT/RDFHelpers.hxx"

#include <vector>
#include <tuple>


namespace DistRDF_Internal {

using CppWorkflowResult = std::tuple<std::vector<ROOT::RDF::RResultHandle>,
                          std::vector<std::string>,
                          std::vector<ROOT::RDF::RNode>>;

CppWorkflowResult __RDF_WORKFLOW_FUNCTION__2(ROOT::RDF::RNode &rdf0)
{
  std::vector<ROOT::RDF::RResultHandle> result_handles;
  std::vector<std::string> result_types;
  std::vector<ROOT::RDF::RNode> output_nodes;

  // To make Snapshots lazy
  ROOT::RDF::RSnapshotOptions lazy_options;
  lazy_options.fLazy = true;




  auto rdf1 = rdf0.Define("weight", "1.0");
  auto rdf2 = rdf1.Filter("trigP");
  auto rdf3 = rdf2.Define("goodphotons", "photon_isTightID && (photon_pt > 25000) && (abs(photon_eta) < 2.37) && ((abs(photon_eta) < 1.37) || (abs(photon_eta) > 1.52))");
  auto rdf4 = rdf3.Filter("Sum(goodphotons) == 2");
  auto rdf5 = rdf4.Filter("Sum(photon_ptcone30[goodphotons] / photon_pt[goodphotons] < 0.065) == 2");
  auto rdf6 = rdf5.Filter("Sum(photon_etcone20[goodphotons] / photon_pt[goodphotons] < 0.065) == 2");
  auto rdf7 = rdf6.Define("m_yy", "ComputeInvariantMass(photon_pt[goodphotons], photon_eta[goodphotons], photon_phi[goodphotons], photon_E[goodphotons])");
  auto rdf8 = rdf7.Filter("photon_pt[goodphotons][0] / 1000.0 / m_yy > 0.35");
  auto rdf9 = rdf8.Filter("photon_pt[goodphotons][1] / 1000.0 / m_yy > 0.25");
  auto rdf10 = rdf9.Filter("m_yy > 105 && m_yy < 160");
  auto res_ptr0 = rdf10.Histo1D({"data","Diphoton invariant mass; m_{#gamma#gamma} [GeV];Events",30,105,160}, "m_yy", "weight");
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
