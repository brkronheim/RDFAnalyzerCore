// LinkDef.h
#ifdef __CINT__
#pragma link off all globals;
#pragma link off all classes;
#pragma link off all functions;

// Tell ROOT how to stream the pair and the map
#pragma link C++ class std::pair<Int_t, Int_t>+;
#pragma link C++ class std::map<std::pair<Int_t, Int_t>, Int_t>+;
#endif