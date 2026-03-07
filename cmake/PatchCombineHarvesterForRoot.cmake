if(NOT DEFINED CH_SOURCE_DIR)
    message(FATAL_ERROR "CH_SOURCE_DIR is required")
endif()

set(_target_file "${CH_SOURCE_DIR}/CombineTools/src/ParseCombineWorkspace.cc")
set(_utilities_file "${CH_SOURCE_DIR}/CombineTools/src/Utilities.cc")
set(_header_file "${CH_SOURCE_DIR}/CombineTools/interface/CombineHarvester.h")

if(NOT EXISTS "${_target_file}")
    message(FATAL_ERROR "CombineHarvester source file not found: ${_target_file}")
endif()

if(NOT EXISTS "${_utilities_file}")
      message(FATAL_ERROR "CombineHarvester source file not found: ${_utilities_file}")
endif()

if(NOT EXISTS "${_header_file}")
      message(FATAL_ERROR "CombineHarvester source file not found: ${_header_file}")
endif()

file(READ "${_target_file}" _content)
file(READ "${_utilities_file}" _utilities_content)
file(READ "${_header_file}" _header_content)

set(_old_split [=[
  RooAbsData *data = ws.data(data_name.c_str());
  // Hard-coded the category as "CMS_channel". Could deduce instead...
  std::unique_ptr<TList> split(data->split(RooCategory("CMS_channel", "")));
  for (int i = 0; i < split->GetSize(); ++i) {
    RooAbsData *idat = dynamic_cast<RooAbsData*>(split->At(i));
]=])

set(_new_split [=[
  RooAbsData *data = ws.data(data_name.c_str());
  // Hard-coded the category as "CMS_channel". Could deduce instead...
#if ROOT_VERSION_CODE >= ROOT_VERSION(6, 38, 0)
  auto split = data->split(RooCategory("CMS_channel", ""));
  for (auto & idat_owner : split) {
    RooAbsData *idat = idat_owner.get();
#else
  std::unique_ptr<TList> split(data->split(RooCategory("CMS_channel", "")));
  for (int i = 0; i < split->GetSize(); ++i) {
    RooAbsData *idat = dynamic_cast<RooAbsData*>(split->At(i));
#endif
]=])

set(_old_reduce [=[
      std::unique_ptr<RooArgSet> pdf_obs(ipdf->getObservables(data->get()));
      idat = idat->reduce(*pdf_obs);
      ws.import(*idat);
]=])

set(_new_reduce [=[
      std::unique_ptr<RooArgSet> pdf_obs(ipdf->getObservables(data->get()));
#if ROOT_VERSION_CODE >= ROOT_VERSION(6, 38, 0)
  RooAbsData *reduced = idat->reduce(*pdf_obs);
  idat = reduced;
      ws.import(*idat);
#else
      idat = idat->reduce(*pdf_obs);
      ws.import(*idat);
#endif
]=])

set(_wrong_new_reduce [=[
      std::unique_ptr<RooArgSet> pdf_obs(ipdf->getObservables(data->get()));
#if ROOT_VERSION_CODE >= ROOT_VERSION(6, 38, 0)
      auto reduced = idat->reduce(*pdf_obs);
      idat = reduced.get();
      ws.import(*idat);
#else
      idat = idat->reduce(*pdf_obs);
      ws.import(*idat);
#endif
]=])

string(REPLACE "${_old_split}" "${_new_split}" _content "${_content}")
string(REPLACE "${_old_reduce}" "${_new_reduce}" _content "${_content}")
string(REPLACE "${_wrong_new_reduce}" "${_new_reduce}" _content "${_content}")

set(_old_iterator_block [=[
  std::set<std::string> names;
  RooFIter dat_it = dat_vars->fwdIterator();
  RooAbsArg *dat_arg = nullptr;
  while((dat_arg = dat_it.next())) {
    names.insert(dat_arg->GetName());
  }

  // Build a new RooArgSet from all_vars, excluding any in names
  RooArgSet result_set;
  RooFIter vars_it = all_vars->fwdIterator();
  RooAbsArg *vars_arg = nullptr;
  while((vars_arg = vars_it.next())) {
    if (!names.count(vars_arg->GetName())) {
      result_set.add(*vars_arg);
    }
  }
]=])

set(_new_iterator_block [=[
  std::set<std::string> names;
  for (auto const* dat_arg : *dat_vars) {
    names.insert(dat_arg->GetName());
  }

  // Build a new RooArgSet from all_vars, excluding any in names
  RooArgSet result_set;
  for (auto const* vars_arg : *all_vars) {
    if (!names.count(vars_arg->GetName())) {
      result_set.add(*vars_arg);
    }
  }
]=])

string(REPLACE "${_old_iterator_block}" "${_new_iterator_block}" _utilities_content "${_utilities_content}")

set(_old_access_block [=[
  std::unordered_map<std::string, bool> flags_;

  struct AutoMCStatsSettings {
]=])

set(_new_access_block [=[
  std::unordered_map<std::string, bool> flags_;

 public:
  struct AutoMCStatsSettings {
]=])

set(_old_post_struct_block [=[
  };

  std::map<std::string, AutoMCStatsSettings> auto_stats_settings_;
]=])

set(_new_post_struct_block [=[
  };

 private:
  std::map<std::string, AutoMCStatsSettings> auto_stats_settings_;
]=])

string(REPLACE "${_old_access_block}" "${_new_access_block}" _header_content "${_header_content}")
string(REPLACE "${_old_post_struct_block}" "${_new_post_struct_block}" _header_content "${_header_content}")

file(WRITE "${_target_file}" "${_content}")
file(WRITE "${_utilities_file}" "${_utilities_content}")
file(WRITE "${_header_file}" "${_header_content}")
message(STATUS "Applied ROOT compatibility patch to ${_target_file}")