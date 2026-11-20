# Provider settings, schema help and validation

Installed `rdfvalidation`/`rdfmodules` IDE signatures cover provider reports and
module composition. The CMS extension owns its recipe signatures; native access
remains open. See [typing and tutorials](PYTHON_TYPING_AND_TUTORIAL.md).

`SettingsSpec` adds provider-owned settings help and shared validation reports.
It is available from `rdfvalidation` or `rdfanalysis`. Configuration checks use
the standard library without importing ROOT, NumPy or Numba. Discovery and asset
checks are explicit. The existing [compiled controller](COMPILED_CPP_WORKFLOWS.md)
can use these reports without loading the Python analysis stack.

## Define and discover a provider

```python
from rdfvalidation import SettingsSpec
from rdfworkflow import ValidationIssue, ValidationReport

def validate(settings):
    issues = () if type(settings.get("enabled")) is bool else (
        ValidationIssue("settings.type", "Expected a boolean", "enabled"),
    )
    return ValidationReport("configuration", ("configuration",), issues)

spec = SettingsSpec("example.selection", {
    "type": "object", "properties": {"enabled": {"type": "boolean"}},
}, validate, documentation="Selection configuration")
report = spec.validate({"enabled": True})
report.raise_if_invalid()
print(spec.describe().explain())
```

The schema is descriptive help. Core does not automatically enforce JSON Schema;
the provider validator owns type, unknown-key and cross-field rules. Settings
and schema must be finite JSON-compatible data. Provider functions receive
detached copies. Descriptions are detached and safe to serialize.

Validators may return `ValidationReport`, or follow the legacy convention of
returning `None` on success and raising `ValueError`/`TypeError` on invalid
settings. A returned failing report is enforced. Booleans, dictionaries and
other return values are contract errors. Unexpected provider exceptions propagate.
Report levels must match the requested validation phase; issue locations are
relative to the provider mapping and receive the caller's location prefix.

An extension can expose `settings_specs()` returning `SettingsSpec` objects from
its existing `python_services.config_schemas` module. Explicit
`discover_settings_specs()` collects them. Existing `PLUGIN_SCHEMAS` providers
without this function remain supported and contribute no new specs. Duplicate
IDs and broken enabled providers fail visibly. Importing the frontend does not
discover or import optional providers. See [extension discovery](EXTENSIONS.md).

## Validate without running an analysis

With the provider package available through the normal extension mechanism:

```bash
python -m rdfvalidation schemas
python -m rdfvalidation schema cms.corrections
python -m rdfvalidation validate cms.corrections recipe.json
python -m rdfvalidation validate cms.corrections recipe.json --level assets
```

The CLI accepts a JSON settings object, emits a versioned JSON description or
`rdf-validation-v1` report, and returns 0 on success or 1 for validation failure.
Unknown providers/invalid command arguments return 2. JSON parse errors include
file, line and column; field errors identify `absolute/path:settings.field`.
No YAML conversion, analysis building or event loop is implicit.

`configuration` runs the provider's configuration validator. `assets` is supported
only when it supplies an explicit asset validator; configuration must pass first.
Native input-schema, graph/compile and execution-smoke validation remain separate
work. Unsupported levels fail with a report listing the supported levels.

## Validate reusable modules once

An [AnalysisModule](MODULES_AND_PLUGINS.md) invokes its optional
`validate_settings` function during construction, prefixes issues with
`module-name.settings`, and rejects invalid settings before graph lowering.
`module.validate()` returns the cached configuration report without invoking the
provider again; `module.describe()` includes a detached copy. The accessor does
not perform a new asset check even if the provider advertises that capability.
Without a validator, module construction remains allowed but `validate()` reports
`provider.validation_unavailable`; descriptive schema alone is not validation.

## CMS correction recipe

```python
from rdfanalyzer_cms import CmsCorrections

recipe = CmsCorrections(era="22pre", jes="total", met_type1=True)
print(recipe.schema())
recipe.validate().raise_if_invalid()
recipe.validate("assets").raise_if_invalid()  # explicit, scoped file checks
module = recipe.as_module()
print(module.describe().explain())
analysis.use(module)  # existing native corrections; before physics definitions
```

The extension owns the schema/defaults, override names and dependency rules.
Constructor validation now checks configuration without checking registry files.
**Migration:** callers that relied on constructor-time registry existence checks
should call `validate("assets").raise_if_invalid()` explicitly, or retain native
`.apply()` setup, which still resolves and validates its actual payloads.
Constructor fields/defaults and the native settings map remain unchanged.

CMS assets validation checks the supplied registry and explicitly overridden,
enabled local payload files only. Registry relative paths use the working
directory, matching native behavior. Payload relative/template/URL paths report
`asset.native_resolution_required`; Python does not guess native resolution or
access the network. Disabled-object payloads are ignored. A passing report does
not certify registry contents, supported eras, resolved default/transitive
payloads or correction names. Native setup retains those checks.

`as_module()` advertises new nominal corrected pT/mass columns and MET pT/phi,
and the exclusive `cms.object_corrections` resource. Raw branch requirements and
compatibility fallbacks remain native-owned. Muon mass remains an existing column.
Direct `recipe.apply(analysis)` retains its returned column mapping; module setup
uses the same native call and its return value is ignored. Downstream variation
resolution remains with the native SystematicManager. Setup failures require a
fresh Analysis. See the [CMS authoring guide](https://gitlab.cern.ch/bkronhei/RDFAnalyzerCMS/-/blob/505e81b28e5ec39bf6761c7a151e6d18dd98850f/docs/PYTHON_AUTHORING.md).

Validation adds setup work, with no Python callback per event.
