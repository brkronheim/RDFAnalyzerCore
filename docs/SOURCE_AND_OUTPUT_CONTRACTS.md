# Configuring input friends, inclusive products and skim columns

### Attach friends to the source

Point the main analysis YAML at a friend definition.

```yaml
friendConfig: friends.yaml
```

```yaml
friends:
  - alias: calibrated
    treeName: Events
    fileList: [calibrated.root]
```

For a relative friend definition path, the native resolver uses an existing path in the working directory, then looks beside the main config. Friend file paths should be absolute, valid URLs or available in the worker's working directory. Match sequential friends to the main input order and population. Set `indexBranches` when using identifier-based matching.

C++ may pass a list directly at construction. File-configured and programmatic aliases must be unique. This preserves ranges, user definitions and previously booked actions on the completed source.

```cpp
FriendTreeSpec corrections;
corrections.alias = "calibrated";
corrections.files = {"calibrated.root"};
DataManager source(config, {corrections});
```

In Python, pass `options={"friendConfig": "/path/to/friends.yaml"}` when constructing AnalysisConfig, or use the typed friends setting. Existing calls to `DataManager::attachFriendTree` and `registerFriendTrees` raise a logic error. Move them to construction before booking the graph. Both C++ symbols remain available to existing binaries.

### Boolean settings

The native `enableSkim`, `enableCounters`, `enableSystematics`, `enableTaggerEfficiency` and `skimSystematics` flags accept case-insensitive true/false, yes/no, on/off and 1/0. Other nonempty values raise a configuration error. Previously, yes and on could silently disable skims or counters. Check persisted options when resuming old configurations.

### Book inclusive metadata

A configured scalar counter weight must exist on the source. Define the integer counter category and tagger input columns on the inclusive node before the first filter. Analyzer and provider filters book inclusive actions once. An analysis without a filter books them during `run()`, before writing products. For a plugin that builds a raw RNode filter, call `provider.notifyBeforeFilter()` before replacing the provider's node. C++ clients using older inline provider filter code should rebuild to use automatic notification.

Enable the tagger with `enableTaggerEfficiency: true` and point `taggerEfficiencyConfig` at a YAML file. Each row books one output.

```yaml
tagger_efficiency:
  - taggerColumn: Jet_score
    weightColumn: weight_nominal
    mode: histogram
    bins: 20
    lowerBound: 0
    upperBound: 1
  - taggerColumn: Jet_score
    mode: working_point
    workingPoints: [loose, medium, tight]
    channelVariable: channel
    channelBins: 2
    channelLabels: [electron, muon]
```

Working-point mode reads an existing `<taggerColumn>_wp_category` column with zero for fail and one-based passing categories. Channel values start at one. Unknown fields, unsupported flavor/manager options, duplicate output names and malformed numeric settings are configuration errors.

### Select skim branches

Use a YAML sequence of explicit names and patterns, such as `[event, 'Muon_*']`. Every explicit wildcard must match. An empty sequence raises an error. To write the full schema, omit `saveConfig` or specify `['*']`. The same rules apply to the Python `save_columns` option.
