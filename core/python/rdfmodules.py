"""Optional module contracts and ordered, construction-time composition.

Metadata describes declared boundaries, not complete native plugin internals.
Providers own their settings validation and builders. No event callback, implicit
dependency sorting or mandatory C++ plugin interface is introduced here.
"""
from collections.abc import Mapping
from dataclasses import dataclass, field
from rdfvalidation import _freeze, _plain, validate_provider_settings


from _rdftext import nonempty as _text


def _names(values, location):
    if not isinstance(values, (tuple, list)):
        raise TypeError(f'{location}: expected a list or tuple')
    result=tuple(_text(value, location) for value in values)
    if len(set(result))!=len(result):
        raise ValueError(f'{location}: duplicate names')
    return result


@dataclass(frozen=True)
class ActionClaim:
    kind: str
    name: str

    def __post_init__(self):
        if self.kind not in ('count','sum','histogram'):
            raise ValueError('ActionClaim.kind must be count, sum or histogram')
        _text(self.name,'ActionClaim.name')
        if self.kind=='histogram' and (';' in self.name or any(part in ('','.','..') for part in self.name.split('/'))):
            raise ValueError('histogram claim requires a relative ROOT path without dot/empty components or cycle delimiters')

    def as_dict(self):return dict(kind=self.kind,name=self.name)


@dataclass(frozen=True)
class ModuleSpec:
    name: str
    requires: tuple=()
    produces: tuple=()
    dependencies: tuple=()
    resources: tuple=()
    actions: tuple=()
    implementation: str='python-numba'
    outputs_at: str='setup'
    settings_schema: object=None
    capabilities: tuple=()
    documentation: str=''

    def __post_init__(self):
        _text(self.name,'ModuleSpec.name');_text(self.implementation,'ModuleSpec.implementation')
        for name in ('requires','produces','dependencies','resources','capabilities'):
            object.__setattr__(self,name,_names(getattr(self,name),'ModuleSpec.'+name))
        if self.name in self.dependencies:raise ValueError('a module cannot depend on itself')
        if not isinstance(self.actions,(tuple,list)) or any(not isinstance(item,ActionClaim) for item in self.actions):
            raise TypeError('ModuleSpec.actions must be a list or tuple of ActionClaim objects')
        object.__setattr__(self,'actions',tuple(self.actions))
        if len(set(self.actions))!=len(self.actions):raise ValueError('duplicate action claims')
        if self.outputs_at not in ('setup','native-lifecycle'):
            raise ValueError('outputs_at must be setup or native-lifecycle')
        if self.settings_schema is not None and not isinstance(self.settings_schema,Mapping):
            raise TypeError('settings_schema must be a mapping or None')
        object.__setattr__(self,'settings_schema',_freeze(self.settings_schema))
        if not isinstance(self.documentation,str):raise TypeError('documentation must be a string')

    def _details(self):
        return dict(name=self.name,requires=list(self.requires),produces=list(self.produces),
            dependencies=list(self.dependencies),resources=list(self.resources),
            actions=[item.as_dict() for item in self.actions],implementation=self.implementation,
            outputs_at=self.outputs_at,settings_schema=_plain(self.settings_schema),
            capabilities=list(self.capabilities),documentation=self.documentation,
            completeness='declared boundary only; unadvertised operations and native internals are opaque')

    def describe(self):
        from rdfworkflow import WorkflowDescription
        return WorkflowDescription('module-description',('declared-contract',),self._details())


@dataclass(frozen=True)
class AnalysisModule:
    spec: ModuleSpec
    builder: object
    settings: object=field(default_factory=dict)
    validate_settings: object=None
    _settings_report: object=field(default=None,init=False,repr=False,compare=False)

    def __post_init__(self):
        if not isinstance(self.spec,ModuleSpec):raise TypeError('spec must be a ModuleSpec')
        if not callable(self.builder):raise TypeError('builder must be callable during graph construction')
        if not isinstance(self.settings,Mapping):raise TypeError('settings must be a mapping')
        object.__setattr__(self,'settings',_freeze(self.settings))
        if self.validate_settings is not None:
            if not callable(self.validate_settings):raise TypeError('validate_settings must be callable or None')
            report=validate_provider_settings(self.validate_settings,self.settings,location=self.spec.name+'.settings')
            report.raise_if_invalid()
            object.__setattr__(self,'_settings_report',report)

    def validate(self):
        """Return the cached construction check without rerunning provider code."""
        if self._settings_report is not None:return self._settings_report
        from rdfworkflow import ValidationIssue,ValidationReport
        return ValidationReport('configuration',(),(ValidationIssue('provider.validation_unavailable',
            'No validator supplied; settings schema is descriptive',self.spec.name+'.settings'),))

    def describe(self):
        from rdfworkflow import WorkflowDescription
        return WorkflowDescription('analysis-module',('declared-contract','setup-builder'),{
            **self.spec._details(),'settings':_plain(self.settings),
            'settings_validation':'provider validator' if self.validate_settings is not None else 'not supplied; schema is descriptive',
            'validation':self._settings_report.as_dict() if self._settings_report is not None else None,
            'builder':{'module':getattr(self.builder,'__module__',None),
                       'callable':getattr(self.builder,'__qualname__',type(self.builder).__name__)},
            'execution':'explicit setup only; not an event callback'})


class ModuleSequence:
    """Apply modules in supplied order, with preflight of all declared claims.

    Native lowering is not transactional. A builder failure blocks subsequent
    frontend run/use, as with a partially failed Stage. Metadata is optional for
    native plugins used directly; this does not replace their role lifecycle.
    """
    def __init__(self,modules):
        if not isinstance(modules,(list,tuple)) or not modules:
            raise ValueError('ModuleSequence requires a nonempty list or tuple')
        if any(not isinstance(item,AnalysisModule) for item in modules):
            raise TypeError('ModuleSequence requires AnalysisModule objects')
        self._modules=tuple(modules)

    def describe(self):
        from rdfworkflow import WorkflowDescription
        return WorkflowDescription('module-sequence',('declared-contract',),{
            'modules':[module.describe().as_dict()['details'] for module in self._modules],
            'order':'supplied order; dependencies must precede consumers; no implicit sorting',
            'execution':'not run; builders execute only on explicit apply/use'})

    def _plan(self,analysis):
        if getattr(analysis,'_module_failure',None) or getattr(analysis,'_stage_failure',None):
            raise RuntimeError('Previous module/stage lowering failed; start a fresh Analysis')
        if getattr(analysis,'_module_active',None):raise RuntimeError('Nested module application is not supported; compose an explicit sequence')
        if getattr(getattr(analysis,'native',None),'event_loops',0):
            raise RuntimeError('Modules must be applied before event execution')
        applied=getattr(analysis,'_modules',{})
        known=set(applied);resources={};reserved={};available=set();queries={}
        counts=set(analysis._action_names['counts']);sums=set(analysis._action_names['sums'])
        histograms=set(analysis._action_names['histograms'])
        for name,description in applied.items():
            resources.update({item:name for item in description['resources']})
            reserved.update({item:name for item in description['produces']})
            for item in description['actions']:
                if item['kind']=='sum':sums.add(item['name'])
                else:
                    counts.add(item['name'])
                    if item['kind']=='histogram':histograms.add(item['name'])
        directories={name[:i] for name in histograms for i,char in enumerate(name) if char=='/'}
        def exists(column):
            if column in available:return True
            if column not in queries:queries[column]=analysis.has_column(column)
            return queries[column]
        for module in self._modules:
            spec=module.spec
            if spec.name in known:raise ValueError('duplicate module: '+spec.name)
            missing=set(spec.dependencies)-known
            if missing:raise ValueError(f'{spec.name}: dependencies must precede this module: '+', '.join(sorted(missing)))
            for column in spec.requires:
                if not exists(column):raise ValueError(f'{spec.name}: required column {column!r} is unavailable during setup')
            for column in spec.produces:
                if column in reserved or exists(column):raise ValueError(f'{spec.name}: output column already exists or is reserved: {column}')
                reserved[column]=spec.name
            for token in spec.resources:
                if token in resources:raise ValueError(f'{spec.name}: resource {token!r} already claimed by {resources[token]}')
                resources[token]=spec.name
            for claim in spec.actions:
                name=claim.name
                if claim.kind=='sum':
                    if name in sums:raise ValueError('duplicate sum destination: '+name)
                    sums.add(name)
                else:
                    if name in counts:raise ValueError('duplicate histogram/count destination: '+name)
                    if claim.kind=='histogram':
                        parents={name[:i] for i,char in enumerate(name) if char=='/'}
                        if name in directories or parents.intersection(histograms):raise ValueError('histogram/directory path collision: '+name)
                        histograms.add(name);directories.update(parents)
                    counts.add(name)
            known.add(spec.name)
            if spec.outputs_at=='setup':available.update(spec.produces)

    def preview(self,analysis):
        self._plan(analysis)
        return self.describe()

    def apply(self,analysis):
        self._plan(analysis)  # A structural failure leaves the graph untouched.
        for module in self._modules:
            spec=module.spec;analysis._module_active=spec.name
            try:
                module.builder(analysis,_plain(module.settings))
                if analysis._stage_failure:raise RuntimeError(analysis._stage_failure)
                if spec.outputs_at=='setup':
                    for column in spec.produces:
                        if not analysis.has_column(column):raise ValueError('advertised output was not produced: '+column)
                    for claim in spec.actions:
                        key={'count':'counts','sum':'sums','histogram':'histograms'}[claim.kind]
                        if claim.name not in analysis._action_names[key]:raise ValueError('advertised action was not booked: '+claim.name)
                analysis._modules[spec.name]=module.describe().as_dict()['details']
            except Exception as error:
                analysis._module_failure=f'{spec.name}: {error}'
                raise RuntimeError(f'Module {spec.name!r} failed during setup: {error}. Native lowering is not transactional; start a fresh Analysis.') from error
            finally:
                analysis._module_active=None
        return analysis


def discover_module_specs():
    """Explicitly load optional authoring_modules services from source manifests.

    No discovery/import occurs when importing rdfmodules/rdfanalysis. Installed
    packages without source manifests can supply ModuleSpec objects directly.
    Errors from enabled providers are propagated, never treated as absence.
    """
    from optional_extensions import load_extension_services
    specs=[];names=set()
    for provider in load_extension_services('authoring_modules'):
        for spec in provider.module_specs():
            if not isinstance(spec,ModuleSpec):raise TypeError('module_specs() providers must return ModuleSpec objects')
            if spec.name in names:raise ValueError('duplicate module description: '+spec.name)
            names.add(spec.name);specs.append(spec)
    return tuple(specs)
