#ifndef SYSTEMATICS_H_INCLUDED
#define SYSTEMATICS_H_INCLUDED

#include <Math/Vector4D.h>
#include <Math/Polar2D.h>
#include <ROOT/RDataFrame.hxx>
#include <RtypesCore.h>
#include <vector>
#include <string>



ROOT::RDF::RNode applySystematic(ROOT::RDF::RNode df, std::string systName, std::vector<std::string> branchNames);



#endif


