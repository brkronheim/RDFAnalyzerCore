#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <fstream>
#include <algorithm>

#include <TChain.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDFHelpers.hxx>

#include <dirent.h>

#include <util.h>
#include <plots.h>
#include <functions.h>

// Fill a TChain with all the files in a directory containing anything in globs, and skipping anything in antiglobs.
// This is not done recursively, it only checks one level
// Returns the number files found
int scan(TChain &chain, const std::string &directory, const std::vector<std::string> &globs, const std::vector<std::string> &antiglobs, bool base){
    std::cout << "Checking " << directory << std::endl;
    int filesFound = 0;
    
    // Attempt to read a directory. Display an error if it is not found.
    DIR *dr = 0;
    struct dirent *en;
    dr = opendir(directory.c_str());
    if(dr==0 && base){
        std::cerr << "Error: No directory found. Returning" << std::endl;
        return(filesFound);
    }
    // Iterate over content of directory
    while ((en = readdir(dr)) != NULL){
        std::string name = en->d_name; // entry name
        std::string fullName = directory + "/" + name;

        if(name.find(".root") != std::string::npos){ // Is this a root file?
            bool found = false; // Currently does not match any of the globs
            for(auto const &glob:globs){ // Check if any of the globs are in this file
                if(name.find(glob)!=std::string::npos){
                    found = true;
                    break;
                }
            }
            for(auto const &glob:antiglobs){ // Check if any of the anti globs are in this file
                if(fullName.find(glob) != std::string::npos){
                    found = false;
                    break;
                }
            }
            if(found && name.at(0)=='.'){
                found = false;
            }
            // Check if marked to be found, if so add to TChain
            if(found){       
                std::string rootFileName = directory+"/"+name;
                chain.Add(rootFileName.c_str());
                filesFound++;
            }
        } else if(name.size()>0 && name.find(".") == std::string::npos){ // Recursively check other directories
            filesFound+=scan(chain, directory+"/"+name+"/", globs, antiglobs, false);
        }
    }

    if(filesFound==0 && base){
        std::cerr << "Error: No files found. Returning" << std::endl;
        return(filesFound);
    }
    return(filesFound);
}

// Read a config file, return a map between the variables in it and their values
std::unordered_map<std::string, std::string> readConfig(const std::string &configFile){
    
    std::unordered_map<std::string, std::string> configMap;

    // iterate over the config file
    std::ifstream file(configFile);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            line = line.substr(0,line.find("#")); // drop everything after the comment
            long unsigned int splitIndex = line.find("="); // split entries on '=', first piece is the key, second is the value
            if(splitIndex < line.size() - 1){ // assuming = was not at the end, split on it 
                configMap.emplace(line.substr(0, splitIndex), line.substr(splitIndex+1));
            }
        }
        file.close();
    } else {
        std::cerr << "Error: Configuration file " << configFile << " could not be opened." << std::endl;
    }

    return(configMap);
}


// Split a string based on a delimiter into a vector of substrings
std::vector<std::string> splitString(std::string input, const std::string &delimiter){
    std::vector<std::string> splitStringVector;
    // get size of delimiter
    const int delimSize = delimiter.size();

    // keep spliting while there are more dlimiters
    while(input.find(delimiter)!=std::string::npos){
        int splitIndex = input.find(delimiter);
        // add each component assuming they aren't empty
        if(splitIndex!=0){
            auto str = input.substr(0,splitIndex);
	        str.erase(remove_if(str.begin(), str.end(), isspace), str.end());
	        if(str.size()>0){
	            splitStringVector.push_back(str);
	        }
        }
        // repeat with what is left of the string
        input = input.substr(splitIndex+delimSize);
    }
    // add the last piece assuming it isn't empty
    input.erase(remove_if(input.begin(), input.end(), isspace), input.end());
    if(input.size()>0){
        splitStringVector.push_back(input);
    }
    return(splitStringVector);
}

// Do basic config processing and execution setup
std::unordered_map<std::string, std::string> processConfig(const std::string &configFile){
    // Global Root setting
    gROOT->ProcessLine( "gErrorIgnoreLevel = 2001;");

    // Get config
    auto configMap = readConfig(configFile);

    // Determine number of threads to use
    int threads = -1;
    if(configMap.find("threads")!=configMap.end()){
        threads = std::stoi(configMap["threads"]);
    }

	if(threads>1){
        ROOT::EnableImplicitMT(threads);
	    std::cout << "Running with " << threads << " threads" << std::endl;
	} else if(threads==1) {
	    std::cout << "Running with 1 thread" << std::endl;
    } else {
        ROOT::EnableImplicitMT();
        std::cout << "Running with maximum number of threads" << std::endl;
    }

    // Return the config
    return(configMap);
}

std::vector<std::string> configToVector(const std::string &configFile){
    std::vector<std::string> configVector;

    // iterate over the config file
    std::ifstream file(configFile);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            line = line.substr(0,line.find("#")); // drop everything after the comment
            configVector.push_back(line);
        }
        file.close();
    } else {
        std::cerr << "Error: Configuration file " << configFile << " could not be opened." << std::endl;
    }

    return(configVector);
}

// Make a basic dataframe and return it based on a config map
std::unique_ptr<TChain> makeTChain(const std::unordered_map<std::string, std::string> &configMap){
    std::string directory;
    std::string fileList;
    Int_t mode = 0;

    // Read the directory from the config. It needs to exist
    if(configMap.find("fileList")!=configMap.end()){
        fileList = configMap.at("fileList");
	mode = 1;
    } else if(configMap.find("directory")!=configMap.end()){
        directory = configMap.at("directory");
	mode = 2;
    } else {
        std::cerr << "Error: No input directory provided. Please include one in the config file." << std::endl;
    }
    
    // Determine if there is a glob list. If not the default is ".root"
    std::vector<std::string> globs = {".root"};
    if(configMap.find("globs")!=configMap.end()){
        globs = splitString(configMap.at("globs"),",");
    }

    // Determine if there is an anti glob list. If not the default is for it to be just "FAIL"
    std::vector<std::string> antiGlobs = {"FAIL"};
    if(configMap.find("antiglobs")!=configMap.end()){
        antiGlobs = splitString(configMap.at("antiglobs"),",");
    }
    //std::cout << "antiglobs: " << std::endl;
    //for(auto glob : antiGlobs){
    //    std::cout << glob << std::endl;
    //}
    // Get all the events
    std::unique_ptr<TChain> chain(new TChain("Events"));
    int fileNum = 0;
    if(mode==1){
	    auto fileListVec = splitString(fileList, ",");
        fileNum = fileListVec.size();
        for(const auto &file : fileListVec){
            std::cout << "Adding file " << file << std::endl;
            chain->Add(file.c_str());
	    }

    } else if(mode==2){
        fileNum = scan(*chain, directory, globs, antiGlobs);
    }
    std::cout << fileNum << " files found" << std::endl;

    // Return the dataframe
    return(chain);
}

// Save content of a DF
ROOT::RDF::RNode saveDF(ROOT::RDF::RNode &df, const std::unordered_map<std::string, std::string> &configMap, 
    const std::unordered_map<std::string, std::unordered_set<std::string>> &variableToSystematicMap){
    std::string saveConfig;
    std::string saveFile;
    std::string saveTree;
    // Read saveConfig, saveFile, and saveTree from the config. It needs to exist
    if(configMap.find("saveConfig")!=configMap.end()){
        saveConfig = configMap.at("saveConfig");
    } else {
        std::cerr << "Error: No saveConfig provided. Please include one in the config file." << std::endl;
        return(df);
    }

    if(configMap.find("saveFile")!=configMap.end()){
        saveFile = configMap.at("saveFile");
    } else {
        std::cerr << "Error: No saveFile provided. Please include one in the config file." << std::endl;
        return(df);
    }

    if(configMap.find("saveTree")!=configMap.end()){
        saveTree = configMap.at("saveTree");
    } else {
        std::cerr << "Error: No saveTree provided. Please include one in the config file." << std::endl;
        return(df);
    }

    // Get the list of branches to save
    std::vector<std::string> saveVectorInit = configToVector(saveConfig);
    std::vector<std::string> saveVector;
    for(auto val : saveVectorInit){
        val = val.substr(0,val.find(" ")); // drop everything after the space
	    if(val.size()>0){
            saveVector.push_back(val);
        }
    }

    const unsigned int vecSize = saveVector.size();
    for(unsigned int i =0; i<vecSize; i++){
        if(variableToSystematicMap.find(saveVector[i])!=variableToSystematicMap.end()){
            for(auto &syst : variableToSystematicMap.at(saveVector[i])){
                saveVector.push_back(saveVector[i]+"_"+syst+"Up");
                saveVector.push_back(saveVector[i]+"_"+syst+"Down");
            }
        }
    }
    // Save the output
    std::cout << "Executing Snapshot" << std::endl;
    std::cout << "Tree: " << saveTree << std::endl;
    std::cout << "SaveFile: " << saveFile << std::endl;
    df.Snapshot(saveTree, saveFile, saveVector);
    //df.Snapshot<Int_t>(saveTree, saveFile, {"nSubJet"});
    std::cout << "Done Saving" << std::endl;
    return(df);
}




// Save multi dimensional histograms and their metadata
void save(std::vector<std::vector<histInfo>> &fullHistList, histHolder &hists, std::vector<std::vector<std::string>> &allRegionNames, std::string fileName){
    std::vector<std::string> allNames;
    std::vector<std::string> allVariables;
    std::vector<std::string> allLabels;
    std::vector<int> allBins;
    std::vector<float> allLowerBounds;
    std::vector<float> allUpperBounds;
    // Get vectors of the hist names, variables, lables, bins, and bounds
    for(auto const &histList : fullHistList)
        for(auto const &info : histList){
            allNames.push_back(info.name());
            allVariables.push_back(info.variable());
            allLabels.push_back(info.label());
            allBins.push_back(info.bins());
            allLowerBounds.push_back(info.lowerBound());
            allUpperBounds.push_back(info.upperBound());
        }
        
    // Open file
    TFile saveFile(fileName.c_str(), "RECREATE");
    
    // Save hists under hists
    saveFile.mkdir("Hists");
    saveFile.cd("Hists");
    hists.save();
    saveFile.cd();

    // Save hist metadata under histData
    TTree histData("histData","histData");
    
    // Make branches
    Int_t name_len;
    Int_t var_len;
    Int_t label_len;
    Char_t name[500];
    Char_t var[500];
    Char_t label[500];

    Int_t bins;
    Float_t lowerBounds;
    Float_t upperBounds;
    

    histData.Branch("name_len",&name_len,"name_len/I");
    histData.Branch("var_len",&var_len,"var_len/I");
    histData.Branch("label_len",&label_len,"label_len/I");

    histData.Branch("name",name,"name[name_len]/C");
    histData.Branch("var",var,"var[var_len]/C");
    histData.Branch("label",label,"label[label_len]/C");
    
    histData.Branch("bins",&bins,"bins/I");
    histData.Branch("lowerBounds",&lowerBounds,"lowerBounds/F");
    histData.Branch("upperBounds",&upperBounds,"upperBounds/F");

    // Save the raw data, including null terminators
    for(long unsigned int x = 0; x<allNames.size(); x++){
        name_len = allNames[x].size();
        var_len = allVariables[x].size();
        label_len = allLabels[x].size();
        char *name_raw = allNames[x].data();
        for(int y = 0; y < name_len; y++){
            name[y] = name_raw[y];
        }
        char *var_raw = allVariables[x].data();
        for(int y = 0; y < var_len; y++){
            var[y] = var_raw[y];
        }
        char *label_raw = allLabels[x].data();
        for(int y = 0; y < label_len; y++){
            label[y] = label_raw[y];
        }
        bins = allBins[x];
        lowerBounds = allLowerBounds[x];
        upperBounds = allUpperBounds[x];
        histData.Fill();
    }

    // Save region metadata under regionData
    TTree regionData("regionData","regionData");
    Int_t region_len;
    char region[500];
    regionData.Branch("region_len",&region_len,"region_len/I");
    regionData.Branch("region",region,"region[region_len]/C");

    for(auto const &regionList : allRegionNames){
        for(auto const &regionString : regionList){
            region_len = regionString.size();
            for(int y = 0; y < region_len; y++){
                region[y] = regionString.data()[y];
            }
            regionData.Fill();
        }
    }

    histData.Write();
    regionData.Write();
}
