
#ifndef MAP_DICT_TYPES_H
#define MAP_DICT_TYPES_H

#include <map>
#include <utility>
#include <TObject.h> // Needed if you're using ROOT types like Int_t

// Just declarations to inform ROOT
// No need for real code

#endif


#ifdef __CLING__
#pragma link off all globals;
#pragma link off all classes;
#pragma link off all functions;

#pragma link C++ class std::pair<Int_t, Int_t>+;
#pragma link C++ class std::map<std::pair<Int_t, Int_t>, Int_t>+;
#endif