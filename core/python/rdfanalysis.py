"""Typed RDFAnalyzerCore frontend with Numba kernels and native graph execution."""
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Mapping, Optional, Tuple
from types import MappingProxyType
import argparse
import json
import itertools
import re
import time
import copy
import math
import hashlib
import types
import sys
import warnings
from rdfsettings import InputSettings, SkimSettings
from rdfdeclarations import Stage, HistogramGroup, Observable, RegularAxis, VariableAxis
from rdfdeclarations import NDHistogramSettings, NDCategoryAxis, NDHistogramSpec, NDHistogramGroup
from rdfcolumns import KernelOutputs, KernelField, CollectionRef, FieldMetadata, named_kernel_arguments, projection_arguments
from rdfmodules import ActionClaim, ModuleSpec, AnalysisModule, ModuleSequence, discover_module_specs
from rdfvalidation import SettingsSpec, discover_settings_specs

_numba_names = itertools.count()
_numba_cache = {}
_numba_bridge_cache = {}
_numba_compilation_attempts = 0
_NATIVE_API_VERSION = 4
_NATIVE_FEATURE_METHODS = (
    ('runtime-threading', ('_threading_snapshot',)),
    ('bool-column-filter', ('filter_column',)),
    ('histogram-batch', ('book_histograms',)),
    ('variable-histograms', ('book_histogram_variable',)),
    ('mixed-histogram-batch', ('book_histograms_mixed',)),
    ('correction-batch', ('applyCorrections','applyCorrectionsVec')),
    ('packed-nd-histograms', ('_book_nd_histograms',)),
    ('packed-nd-per-slot', ('_nd_collection_layouts',)),
)


def _native_features(target):
    """Optional operation availability; class/instance lookup only, no graph work."""
    return [name for name,methods in _NATIVE_FEATURE_METHODS
            if all(hasattr(target,method) for method in methods)]


def _numba_bridge_snapshot(compiled):
    """Classify the registered wrapper without importing Numba or exposing code.

    ROOT may mutate the same Python callable on its next declaration. Store only
    immutable primitive metadata for each cache signature, alongside its owners.
    """
    try:
        import ctypes
        bridge = getattr(compiled, '__numba_cfunc__', None)
        bridge_class = None if bridge is None else type(bridge).__module__ + '.' + type(bridge).__qualname__
        native_type = getattr(sys.modules.get('numba.core.ccallback'), 'CFunc', None)
        if isinstance(native_type, type) and isinstance(bridge, native_type):
            kind = 'native-cfunc'
        elif isinstance(bridge, getattr(ctypes, '_CFuncPtr', ())):
            kind = 'ctypes-callback'
        else:
            kind = 'unknown'
        return kind, bridge_class
    except Exception:
        # Optional diagnostics must not break the default permissive path on
        # an unfamiliar decorator object. Explicit strict mode rejects unknown.
        return 'unknown', None


def _kernel_fingerprint(function, version):
    """Track compile-time state without attempting to unload native JIT code."""
    import numpy as np
    seen=set()
    def token(value,names=None):
        if isinstance(value,np.ndarray):
            if value.dtype.hasobject:raise ValueError('Numba captures cannot contain object arrays')
            return ('array',value.dtype.str,value.shape,hashlib.sha256(value.tobytes()).hexdigest())
        if isinstance(value,np.generic):return ('numpy_scalar',value.dtype.str,value.tobytes())
        if isinstance(value,np.dtype):return ('dtype',value.str)
        if value is None or isinstance(value,(str,bytes,bool,int,float,complex)):
            return (type(value).__name__,repr(value))
        identity=(id(value),tuple(sorted(set(names or ()))) if isinstance(value,types.ModuleType) else None)
        if identity in seen:return ('reference',identity)
        seen.add(identity)
        if isinstance(value,(list,tuple)):return (type(value).__name__,tuple(token(item) for item in value))
        if isinstance(value,frozenset):return ('frozenset',tuple(sorted((token(item) for item in value),key=repr)))
        if isinstance(value,dict):return ('dict',tuple(sorted(((token(key),token(item)) for key,item in value.items()),key=repr)))
        if isinstance(value,types.CodeType):
            # marshal encodes interpreter-owned string interning/reference flags.
            # Numba can change those flags without changing code semantics,
            # causing false misses. Read immutable code fields explicitly.
            return ('code',value.co_argcount,value.co_posonlyargcount,value.co_kwonlyargcount,
                    value.co_nlocals,value.co_stacksize,value.co_flags,value.co_code,
                    token(value.co_consts),value.co_names,value.co_varnames,value.co_freevars,
                    value.co_cellvars,getattr(value,'co_exceptiontable',b''))
        if value is Ellipsis:return ('ellipsis',)
        if isinstance(value,types.FunctionType):
            if value is not function and (value.__module__ or '').startswith(('numpy','numba','math')):
                return ('library_function',value.__module__,value.__qualname__)
            code=value.__code__
            closure=tuple(token(cell.cell_contents,code.co_names) for cell in (value.__closure__ or ()))
            globals_used=tuple((name,token(value.__globals__[name],code.co_names)) for name in sorted(set(code.co_names)) if name in value.__globals__)
            return ('function',token(code),token(value.__defaults__),token(value.__kwdefaults__),closure,globals_used)
        if isinstance(value,types.ModuleType):
            # Library functions have their own compiled/intrinsic semantics.
            # Track directly referenced constants and names, not whole modules.
            attributes=[]
            for name in sorted(set(names if names is not None else function.__code__.co_names)):
                item=value.__dict__.get(name)
                if item is not None:attributes.append((name,token(item,names)))
            return ('module',value.__name__,getattr(value,'__version__',None),tuple(attributes))
        if isinstance(value,(type,types.BuiltinFunctionType,np.ufunc)):
            return ('callable',getattr(value,'__module__',type(value).__module__),getattr(value,'__qualname__',getattr(value,'__name__',type(value).__name__)))
        if callable(value) and getattr(value,'__module__','').startswith(('numpy','numba','math')):
            return ('library_callable',value.__module__,getattr(value,'__name__',type(value).__name__))
        if version is not None:return ('explicit_version_object',type(value).__module__,type(value).__qualname__,id(value))
        raise ValueError(f'cannot fingerprint captured {type(value).__name__}; pass cache_version and update it when captured state changes')
    return hashlib.sha256(repr((version,token(function))).encode()).hexdigest()


@dataclass(frozen=True)
class AnalysisConfig:
    files: Tuple[str, ...]
    output: str
    tree: str = "Events"
    threads: int = 1
    systematics: bool = True
    skim: bool = True
    persist_histograms: bool = False
    save_columns: Optional[Tuple[str, ...]] = None
    options: Mapping[str, str] = field(default_factory=dict)
    entry_range: Optional[Tuple[int, int]] = None
    range_mode: str = 'filter'
    input_settings: InputSettings = field(default_factory=InputSettings)
    skim_settings: SkimSettings = field(default_factory=SkimSettings)

    def __post_init__(self):
        object.__setattr__(self,'input_settings',InputSettings.from_mapping(self.input_settings))
        object.__setattr__(self,'skim_settings',SkimSettings.from_mapping(self.skim_settings))
        if not isinstance(self.files, (list, tuple)) or not self.files:
            raise ValueError("files must be a nonempty sequence of paths or URLs")
        if any(not isinstance(p, str) or not p.strip() or "," in p for p in self.files):
            raise ValueError("files must contain nonempty paths without commas")
        object.__setattr__(self, "files", tuple(self.files))
        if not isinstance(self.output, str) or not self.output.strip():
            raise ValueError("output must be a nonempty path")
        if not isinstance(self.tree, str) or not self.tree.strip():
            raise ValueError("tree must be a nonempty name")
        if type(self.threads) is not int or self.threads == 0 or self.threads < -1:
            raise ValueError("threads must be -1 or a positive integer")
        for key in ("systematics", "skim", "persist_histograms"):
            if type(getattr(self, key)) is not bool:
                raise ValueError(f"{key} must be a boolean")
        if self.save_columns is not None:
            if not isinstance(self.save_columns, (list, tuple)) or not self.save_columns:
                raise ValueError("save_columns must be a nonempty sequence or None")
            if any(not isinstance(c, str) or not c.strip() or "\n" in c
                   for c in self.save_columns):
                raise ValueError("save_columns must contain nonempty column names")
            object.__setattr__(self, "save_columns", tuple(self.save_columns))
        reserved = {"fileList", "saveFile", "saveTree", "treeList", "threads",
                    "enableSystematics", "enableSkim", "saveConfig", "persistPythonHistograms"}
        if self.range_mode not in ('filter','source'):
            raise ValueError('range_mode must be filter or source')
        if self.entry_range is None:
            if self.range_mode!='filter':raise ValueError('range_mode requires an explicit entry_range')
        else:
            bounds=self.entry_range
            if not isinstance(bounds,(tuple,list)) or len(bounds)!=2 or any(type(value) is not int for value in bounds):
                raise ValueError('entry_range must contain two integer half-open bounds')
            if not 0<=bounds[0]<bounds[1]<2**63:raise ValueError('entry_range requires 0 <= first < last <= signed64-bit maximum')
            if self.threads!=1:raise ValueError('entry_range requires threads=1')
            if self.range_mode=='filter' and bounds[1]>=2**32:
                raise ValueError('filter ranges have unsigned32-bit bounds; choose source on a supported ROOT version')
            object.__setattr__(self,'entry_range',tuple(bounds))
            reserved.update(('firstEntry','lastEntry','entryRangeMode'))
        if not isinstance(self.options, Mapping):
            raise ValueError("options must be a mapping of native configuration strings")
        reserved.update(self.input_settings.native_options())
        reserved.update(self.skim_settings.native_options())
        if self.input_settings.cache_size_mb is not None:
            reserved.update(('rootTTreeCacheSize','ROOT_TTREECACHE_SIZE'))
        if reserved.intersection(self.options):
            raise ValueError("options cannot override typed fields: " +
                             ", ".join(sorted(reserved.intersection(self.options))))
        if any(not isinstance(k, str) or not isinstance(v, str)
               for k, v in self.options.items()):
            raise ValueError("options keys and values must be strings")
        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))

    @classmethod
    def from_file(cls, path):
        from native_config_io import load_yaml_document
        path = Path(path).resolve()
        data = load_yaml_document(path, typed=True)
        if not isinstance(data, dict):
            raise ValueError(f"{path}: analysis configuration must be a mapping")
        unknown = data.keys() - cls.__dataclass_fields__.keys()
        if unknown:
            detail = f"{path}: unknown analysis fields: " + ", ".join(sorted(map(str, unknown)))
            if unknown.intersection({"fileList", "treeList", "saveFile", "saveTree"}):
                detail += ". To open a native worker configuration directly, use rdfanalyzer.Analyzer(str(path))."
            raise ValueError(detail)
        def resolve(value):
            return value if "://" in value or Path(value).is_absolute() else str(path.parent / value)
        try:
            for name,kind in (('input_settings',InputSettings),('skim_settings',SkimSettings)):
                if name in data:data[name]=kind.from_mapping(data[name],base=path.parent)
            config = cls(**data)
            return cls(**{**data, "files": tuple(map(resolve, config.files)),
                          "output": resolve(config.output)})
        except (TypeError,ValueError) as error:
            raise ValueError(f'{path}: {error}') from error

    def native_config(self):
        metadata = str(Path(self.output).with_suffix(".metadata.root"))
        ranges={} if self.entry_range is None else {'firstEntry':str(self.entry_range[0]),
            'lastEntry':str(self.entry_range[1]),'entryRangeMode':self.range_mode}
        return {"metaFile": metadata, **self.options, **ranges, "fileList": ",".join(self.files),
                "saveFile": self.output, "treeList": self.tree,
                "saveTree": self.tree, "threads": str(self.threads),
                "enableSystematics": "true" if self.systematics else "false",
                "persistPythonHistograms": "true" if self.persist_histograms else "false",
                "enableSkim": "true" if self.skim else "false",
                **self.input_settings.native_options(), **self.skim_settings.native_options()}

    @classmethod
    def schema(cls):
        """Reference for optional typed groups, from their validation definitions."""
        return {'schema':'rdf-settings-schema-v1','input_settings':InputSettings.schema(),
                'skim_settings':SkimSettings.schema(),
                'advanced_options':'opaque native string mapping; may not conflict with explicitly supplied typed settings',
                'extensions':'extension-owned facades/schemas; no mandatory experiment imports',
                'asset_scope':'only explicitly supplied local input-settings configuration files; not input ROOT files or transitive payloads'}

    def describe(self):
        """Describe run settings without importing native modules or opening input.

        This is a configuration description, not a constructed-graph inventory.
        Use Analysis.inspect() after graph construction for available columns.
        """
        from rdfworkflow import WorkflowDescription
        return WorkflowDescription("python-numba", ("configuration","local-config-assets"), {
            "files":list(self.files), "tree":self.tree, "output":self.output,
            "threads":self.threads, "systematics":self.systematics, "skim":self.skim,
            "persist_histograms":self.persist_histograms, "entry_range":self.entry_range,
            "range_mode":self.range_mode, "native_option_names":sorted(self.options),
            "input_settings":self.input_settings.values(), "skim_settings":self.skim_settings.values(),
            "native_settings":self.native_config(),
            "setting_origins":{key:'typed' if key in self.input_settings.native_options() or key in self.skim_settings.native_options()
                               else 'legacy' if key in self.options else 'frontend'
                               for key in self.native_config()},
            "asset_validation_scope":self.schema()['asset_scope'],
            "input_schema_preflight":"explicit check_schema(requirements); every supplied file, original columns only; never automatic",
            "graph":"not constructed; configuration-only description"})

    def validate(self, level="configuration"):
        """Report supported checks explicitly; construction validates settings."""
        from rdfworkflow import ValidationIssue,ValidationReport
        issues=[]
        if level not in ('configuration','assets'):
            issues.append(ValidationIssue('unsupported_level',
                'AnalysisConfig.validate supports configuration and local config assets; use check_schema(requirements) for explicit per-file metadata. Graph/compile validation requires graph construction.','level'))
        elif level=='assets':
            for item in self.input_settings.__dataclass_fields__.values():
                value=getattr(self.input_settings,item.name)
                if item.metadata.get('asset') and value is not None and not Path(value).is_file():
                    issues.append(ValidationIssue('missing_asset',f'Local configuration file is missing or not a regular file: {value}',
                                                  'input_settings.'+item.name))
        return ValidationReport(level,('configuration','assets'),tuple(issues))

    def check_schema(self, requirements, *, reader=None):
        """Explicitly acquire every input's requested schema before construction.

        This checks original input columns; native aliases, optional branches,
        friends and derived columns remain the analysis/provider's responsibility.
        Ordinary validate/describe/construction never calls this automatically.
        """
        from rdfschema import check_input_schemas
        return check_input_schemas(self.files, requirements, tree=self.tree, reader=reader)


class Analysis:
    """Own the native analyzer and its generated config for the lazy lifecycle.

    Use ``define_numba`` for Python/NumPy kernels, or ``define`` for C++
    expressions. Both construct native RDF nodes; ROOT may select a ctypes
    bridge if native CFunc compilation is unavailable. Explicit
    ``require_native`` and ``numba_bridges`` inspect/control that wrapper policy,
    without certifying every function reachable from the kernel. ``native``
    exposes existing plugin/histogram APIs.
    """
    def __init__(self, config: AnalysisConfig, *, record_graph=True):
        if not isinstance(config, AnalysisConfig):
            raise TypeError("config must be an AnalysisConfig")
        if type(record_graph) is not bool:
            raise TypeError("record_graph must be a boolean")
        config.validate('assets').raise_if_invalid()
        from native_config_io import write_config_yaml
        import rdfanalyzer
        if getattr(rdfanalyzer,'python_authoring_api_version',0)<_NATIVE_API_VERSION:
            raise RuntimeError('Native rdfanalyzer is older than this Python frontend; rebuild/install both from the same build')
        self.config = config
        self._native_functions = []
        self._numba_bridges = []
        self._session = None
        self._weights = {}
        self._regions = {}
        self._region_base_columns = None
        self._action_names = {'counts':set(),'sums':set(),'histograms':set()}
        self._filter_names = set()
        self._stages = []
        self._kernel_outputs = {}
        self._modules = {}
        self._module_active = None
        self._module_failure = None
        self._stage_failure = None
        self._run_status = None
        self._nd_groups = {}
        self._nd_failure = None
        self._run_wall_s = None
        self._compilation_stats = {"numba_compile_s": 0.0, "numba_kernels_compiled": 0,
                                   "numba_cache_hits": 0}
        self._directory = TemporaryDirectory(prefix="rdfanalysis_")
        directory = Path(self._directory.name)
        settings = config.native_config()
        if config.save_columns is not None:
            columns = directory / "columns.yaml"
            write_config_yaml(list(config.save_columns), columns)
            settings["saveConfig"] = str(columns)
        path = directory / "analysis.yaml"
        write_config_yaml(dict(sorted(settings.items())), path)
        if hasattr(rdfanalyzer.Analyzer,'_graph_description'):
            self.native = rdfanalyzer.Analyzer(str(path),record_graph=record_graph)
        else:
            self.native = rdfanalyzer.Analyzer(str(path))

    def define(self, name, expression, columns=()):
        self.native.define(name, expression, list(columns))
        return self

    def define_constant_vector(self, name, values):
        """Copy a 1D native-endian NumPy array at booking. All events share its read-only values.

        Supported dtypes are float32/64, int32/64 and uint32/64. A strided
        array is copied into contiguous storage. The native graph owns the copy.
        """
        if not hasattr(self.native, "define_constant_vector"):
            raise RuntimeError("Rebuild rdfanalyzer to use graph-owned constant vectors")
        self.native.define_constant_vector(name, values)
        return self

    def define_nominal(self, name, expression, columns=()):
        self.native.define_nominal(name, expression, list(columns))
        return self

    def define_numba(self, name, function, columns, *, return_type=None, nominal=False, cache_version=None, require_native=False):
        """Compile a Python kernel with Numba and propagate input variations.

        RVec inputs appear as NumPy arrays. Specify return_type for vector
        outputs, for example ``RVec<double>``. Keep compiled owners alive until
        lazy execution finishes; no raw address is exposed to the user.
        ``require_native=True`` rejects unknown/ctypes bridges before defining
        the RDF column. Classification concerns the wrapper, not its whole call graph.
        """
        if type(require_native) is not bool:
            raise TypeError('require_native must be a boolean')
        import ROOT
        if not callable(function):
            raise TypeError("function must be a Python callable supported by Numba")
        if cache_version is not None and type(cache_version) not in (str,int):
            raise TypeError('cache_version must be a string, integer or None')
        if hasattr(function,'py_func'):
            raise TypeError('Pass the original Python function (or dispatcher.py_func) to define_numba; ROOT owns the JIT wrapper and cannot accept an already-jitted dispatcher')
        if self.has_column(name):
            raise ValueError(f"Define: column '{name}' already exists")
        columns = list(columns)
        input_types = []
        for column in columns:
            try:
                input_types.append(self.native.column_type(column))
            except Exception as error:
                raise ValueError(f"Numba column '{name}': cannot resolve input '{column}'") from error
        aliases = {"Float_t": "float", "Double_t": "double", "Int_t": "int",
                   "UInt_t": "unsigned int", "Bool_t": "bool", "Long64_t": "long",
                   "ULong64_t": "unsigned long"}
        for i, type_name in enumerate(input_types):
            input_types[i] = re.sub(r"\b(" + "|".join(aliases) + r")\b",
                                    lambda match: aliases[match.group()], type_name)
        key = (id(function), _kernel_fingerprint(function,cache_version), tuple(input_types), return_type)
        if key not in _numba_cache:
            global _numba_compilation_attempts
            if self._session is not None:
                self._session._before_compilation()
            _numba_compilation_attempts += 1
            function_name = f"__rdf_numba_kernel_{next(_numba_names)}"
            started = time.perf_counter()
            try:
                compiled = ROOT.Numba.Declare(input_types, return_type, name=function_name)(function)
            except Exception as error:
                detail = (" Group related arguments into a vector; long signatures can exceed the ROOT/Numba bridge's supported call form."
                          if len(columns) > 30 else "")
                raise RuntimeError(f"Numba compilation failed for column '{name}' ({function.__name__}), "
                                   f"inputs {dict(zip(columns, input_types))}, return {return_type}.{detail}") from error
            self._compilation_stats["numba_compile_s"] += time.perf_counter() - started
            self._compilation_stats["numba_kernels_compiled"] += 1
            # ROOT annotates the function with native owners. Preserve each
            # signature's owners even when another declaration updates it.
            _numba_cache[key] = (function_name, compiled, tuple(compiled.__dict__.values()))
            _numba_bridge_cache[key] = _numba_bridge_snapshot(compiled)
        else:
            self._compilation_stats["numba_cache_hits"] += 1
        function_name, compiled, owners = _numba_cache[key]
        self._native_functions.append((compiled, owners))
        # Never re-read a cached callable's mutable decorator attributes: they
        # may now describe another signature. Missing old metadata stays unknown.
        kind, bridge_class = _numba_bridge_cache.get(key, ('unknown', None))
        record = dict(column=name, input_types=list(input_types), return_type=return_type,
                      kind=kind, bridge_class=bridge_class, require_native=require_native,
                      definition_registered=False)
        if not hasattr(self, '_numba_bridges'):
            self._numba_bridges = []
        self._numba_bridges.append(record)
        if require_native and kind != 'native-cfunc':
            raise RuntimeError(f"Numba column '{name}': require_native=True requires a known Numba CFunc; "
                               f"observed {kind} ({bridge_class}). Inspect analysis.numba_bridges; "
                               "require_native=False explicitly permits the existing fallback behavior.")
        if kind != 'native-cfunc':
            warnings.warn(
                f"Numba column '{name}': native CFunc is not confirmed; observed {kind} ({bridge_class}). "
                "A per-event Python callback may be used. Set require_native=True to reject this bridge "
                "before graph registration; inspect analysis.numba_bridges for details.",
                RuntimeWarning, stacklevel=2)
        self.native._define_compiled(name, "Numba::" + function_name, columns, nominal)
        record['definition_registered'] = True
        return self

    @property
    def runtime_threading(self):
        """Observe ROOT's pool and this dataframe's slots without running events.

        Requested threads remain configuration intent. Older native modules
        report unavailable observations explicitly rather than guessing them.
        ROOT's process-global pool can differ from a dataframe's booked slots.
        """
        result = dict(requested_threads=self.config.threads, observed=False,
                      implicit_mt_enabled=None, pool_size=None, dataframe_slots=None)
        getter = getattr(self.native, '_threading_snapshot', None)
        if getter is None:
            return result
        snapshot = getter()
        if (not isinstance(snapshot, dict)
                or type(snapshot.get('implicit_mt_enabled')) is not bool
                or type(snapshot.get('pool_size')) is not int
                or snapshot['pool_size'] < 0
                or type(snapshot.get('dataframe_slots')) is not int
                or snapshot['dataframe_slots'] < 1):
            raise RuntimeError('native threading snapshot has an invalid contract')
        result.update(observed=True,
                      **{key:snapshot[key] for key in ('implicit_mt_enabled','pool_size','dataframe_slots')})
        return result

    @property
    def numba_bridges(self):
        """Detached setup-time wrapper classifications; no addresses or source bodies."""
        return copy.deepcopy(getattr(self, '_numba_bridges', []))

    @property
    def compilation_stats(self):
        return dict(self._compilation_stats)

    def define_numba_outputs(self, outputs, function, columns, *, return_type, cache_version=None, require_native=False):
        """Name an existing homogeneous kernel layout without wrapping its callable."""
        column = named_kernel_arguments(outputs, return_type)
        self.define_numba(column, function, columns, return_type=return_type,
                          nominal=outputs.nominal, cache_version=cache_version,
                          **({'require_native': require_native} if require_native is not False else {}))
        self._kernel_outputs[column] = dict(outputs.describe(), return_type=return_type)
        return self

    def project(self, name, source, index=None, *, nominal=None):
        """Select an RVec element with native bounds checks and propagation."""
        source, index, nominal = projection_arguments(source, index, nominal)
        self.native.project(name, source, index, nominal)
        return self

    def filter(self, name, expression, columns=()):
        self.native.filter(name, expression, list(columns))
        self._filter_names.add(name)
        return self

    def filter_column(self, name, column):
        """Filter an existing scalar bool with native systematic masks.

        This opt-in path avoids a redundant expression alias. Define the bool
        first; ordinary expression filters and interleaving remain available.
        """
        if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z0-9_]+',name,re.ASCII):
            raise ValueError('filter name must be nonempty ASCII letters, digits or underscores')
        if not isinstance(column,str) or not column.strip() or any(char in column for char in ('\0','\n','\r')):
            raise ValueError('filter column must be a nonempty name without NUL/newlines')
        operation=getattr(self.native,'filter_column',None)
        if operation is None:
            raise RuntimeError('filter_column requires a native module built with bool-column filter support; rebuild rdfanalyzer')
        operation(name,column)
        self._filter_names.add(name)
        return self

    def count(self, name):
        self.native.book_count(name)
        self._action_names['counts'].add(name)
        return self

    def nd_histograms(self, group, *, max_histograms=1000):
        """Book category-packed observables at this exact native graph position.

        Requires an explicitly registered canonical histogramManager plugin.
        Contents stay in the native metadata output, separate from TH1 payloads.
        Structural checks perform no I/O; native failures require a fresh Analysis.
        """
        if not isinstance(group, NDHistogramGroup): raise TypeError('nd_histograms requires NDHistogramGroup')
        requests, categories = group.native_descriptors(max_histograms=max_histograms)
        operation = getattr(self.native, '_book_nd_histograms', None)
        if operation is None:
            raise RuntimeError('nd_histograms requires native packed ND support; rebuild rdfanalyzer')
        layout = getattr(getattr(self, 'config', None), 'options', {}).get('histogramCollectionLayout', 'fixed')
        if layout not in ('fixed', 'per-slot'):
            raise ValueError('histogramCollectionLayout must be fixed or per-slot')
        if layout == 'per-slot':
            supported = getattr(self.native, '_nd_collection_layouts', None)
            if supported is None or layout not in supported():
                raise RuntimeError('per-slot ND collections require a matching native build; rebuild rdfanalyzer')
        if self._run_status is not None: raise RuntimeError('Book ND histograms before run()')
        if self._nd_failure: raise RuntimeError('ND lowering failed; start a fresh Analysis')
        if group.name in self._nd_groups: raise ValueError('duplicate ND group: '+group.name)
        booked = {item['native_key'] for previous in self._nd_groups.values() for item in previous['histograms']}
        if any(item[0]+'_' in booked for item in requests): raise ValueError('duplicate ND histogram destination')
        for column in dict.fromkeys([r[1] for r in requests] + [r[3] for r in requests] +
                                    [c[0] for c in categories if c[0] != 'zero__']):
            if not self.has_column(column): raise ValueError('unknown ND input column: '+column)
        try:
            native = operation(requests, categories, group.variations == 'nominal')
        except Exception as error:
            self._nd_failure = str(error)
            raise
        description=group.describe()
        self._nd_groups[group.name] = dict(description, native=dict(native),
            configured_storage_options={key:value for key,value in self.config.options.items()
                if key in ('histogramBackend','histogramBoostPrecision','histogramBoostSparseBackend','histogramDenseLimitMiB','histogramShareCollectionTransport','histogramCollectionLayout')},
            logical_cells_including_flows=description['logical_cells_per_systematic_bin'] * (len(native['systematic_labels'])+2))
        return self

    def histogram(self, name, variable, bins, low, high, *, weight="", selection="",
                  variation="Nominal", region=None):
        if region is not None:
            if selection:
                raise ValueError("use region or selection for a histogram, not both")
            if region not in self._regions:
                raise ValueError(f"unknown region '{region}'; declare it before booking")
            selection=self._regions[region]['selection']
        self.native.book_histogram(name, variable, bins, low, high, weight, selection, variation)
        self._action_names['histograms'].add(name)
        self._action_names['counts'].add(name)
        return self

    def histogram_variable(self, name, variable, edges, *, weight="", selection="",
                           variation="Nominal", region=None):
        """Book a lazy 1D histogram with finite, strictly increasing bin edges."""
        axis = VariableAxis(edges)
        operation = getattr(self.native, 'book_histogram_variable', None)
        if operation is None:
            raise RuntimeError('histogram_variable requires native variable histogram support; rebuild rdfanalyzer')
        if region is not None:
            if selection:
                raise ValueError('use region or selection for a histogram, not both')
            if region not in self._regions:
                raise ValueError(f"unknown region '{region}'; declare it before booking")
            selection = self._regions[region]['selection']
        operation(name, variable, axis.edges, weight, selection, variation)
        self._action_names['histograms'].add(name)
        self._action_names['counts'].add(name)
        return self

    def histogram_batch(self, histograms, *, max_histograms=1000):
        """Explicit bounded booking at the current node, sharing equal selections.

        Each mapping uses histogram() or histogram_variable() arguments. Axis
        kinds can be mixed. All named counts remain present.
        The cache is confined to this call: later definitions/filters cannot reuse
        an earlier node. Structural failures are checked first; native failures
        after booking starts are nontransactional and require a fresh Analysis.
        """
        if type(max_histograms) is not int or max_histograms < 1:
            raise ValueError('max_histograms must be a positive integer')
        if not isinstance(histograms, (list, tuple)) or not histograms:
            raise ValueError('histograms must be a nonempty list or tuple of mappings')
        if len(histograms) > max_histograms:
            raise ValueError('histogram batch exceeds max_histograms')
        mixed = any(isinstance(item, Mapping) and 'edges' in item for item in histograms)
        operation = getattr(self.native, 'book_histograms_mixed' if mixed else 'book_histograms', None)
        if operation is None:
            if mixed:
                raise RuntimeError('histogram_batch with edges requires native mixed histogram batch support; rebuild rdfanalyzer')
            raise RuntimeError('histogram_batch requires a native module built with histogram batch support; rebuild rdfanalyzer')
        names = set(self._action_names['counts']) | set(self._action_names['histograms'])
        optional = {'weight', 'selection', 'variation', 'region'}
        requests = []
        for index, values in enumerate(histograms):
            if not isinstance(values, Mapping):
                raise TypeError(f'histograms[{index}] must be a mapping')
            required = {'name', 'variable', 'edges'} if 'edges' in values else {'name', 'variable', 'bins', 'low', 'high'}
            if required - values.keys() or values.keys() - required - optional:
                raise ValueError(f'histograms[{index}] requires {sorted(required)} and only {sorted(optional)} optional keys')
            item = dict(weight='', selection='', variation='Nominal', region=None)
            item.update(values)
            for key in ('name', 'variable', 'variation'):
                if not isinstance(item[key], str) or not item[key].strip() or any(char in item[key] for char in ('\0', '\n', '\r')):
                    raise ValueError(f'histograms[{index}].{key} must be a nonempty name without NUL/newlines')
            for key in ('weight', 'selection'):
                if not isinstance(item[key], str) or any(char in item[key] for char in ('\0', '\n', '\r')):
                    raise ValueError(f'histograms[{index}].{key} must be a column string without NUL/newlines')
            if item['name'] in names:
                raise ValueError('Duplicate histogram/count '+item['name'])
            names.add(item['name'])
            if 'edges' in item:
                item['edges'] = VariableAxis(item['edges']).edges
                item.update(bins=0, low=0., high=0.)
            else:
                if type(item['bins']) is not int or item['bins'] < 1:
                    raise ValueError('Histogram bins must be a positive integer')
                if any(type(item[key]) not in (int, float) or not math.isfinite(item[key]) for key in ('low', 'high')) or item['low'] >= item['high']:
                    raise ValueError('Histogram bounds must be finite and increasing')
                item['edges'] = ()
            if item['region'] is not None:
                if item['selection']:
                    raise ValueError('use region or selection for a histogram, not both')
                if not isinstance(item['region'], str) or item['region'] not in self._regions:
                    raise ValueError('unknown histogram region: '+str(item['region']))
                item['selection'] = self._regions[item['region']]['selection']
            keys = ('name', 'variable', 'bins', 'low', 'high', 'edges', 'weight', 'selection', 'variation') if mixed else ('name', 'variable', 'bins', 'low', 'high', 'weight', 'selection', 'variation')
            requests.append(tuple(item[key] for key in keys))
        operation(requests)
        for request in requests:
            self._action_names['histograms'].add(request[0])
            self._action_names['counts'].add(request[0])
        return self

    def correction_batch(self, role, correction, string_arguments, *, outputs,
                         inputs=(), propagate_systematics=(), vector=False):
        """Apply registered native correction variants with one local input pack.

        Numeric inputs default to the registered features; output names are
        explicit. Empty propagation flags preserve native all-propagating behavior.
        False disables automatic input-variation expansion for that output.
        Already-varied input names remain explicit. Scalar precision
        and vector Float_t output follow CorrectionManager unchanged. This neither
        registers nuisance labels nor installs/configures a manager automatically.
        Native plugin definitions remain opaque in the declaration-level recorder.
        """
        for key, value in (('role',role),('correction',correction)):
            if not isinstance(value,str) or not value.strip() or any(char in value for char in ('\0','\n','\r')):
                raise ValueError(key+' must be a nonempty name without NUL/newlines')
        if type(vector) is not bool:raise TypeError('vector must be a boolean')
        if not isinstance(string_arguments,(list,tuple)) or not string_arguments:
            raise ValueError('string_arguments must be a nonempty list/tuple of string-argument lists')
        arguments=[]
        for values in string_arguments:
            if not isinstance(values,(list,tuple)) or any(not isinstance(value,str) for value in values):
                raise TypeError('each correction variant requires a list/tuple of strings')
            arguments.append(list(values))
        def columns(values,location):
            if not isinstance(values,(list,tuple)):raise TypeError(location+' must be a list/tuple of column names')
            for value in values:
                if not isinstance(value,str) or not value.strip() or any(char in value for char in ('\0','\n','\r')):
                    raise ValueError(location+' must contain nonempty column names without NUL/newlines')
            return list(values)
        inputs=columns(inputs,'inputs');outputs=columns(outputs,'outputs')
        if len(outputs)!=len(arguments) or len(set(outputs))!=len(outputs):
            raise ValueError('outputs must give one unique explicit name per correction variant')
        if not isinstance(propagate_systematics,(list,tuple)) or any(type(flag) is not bool for flag in propagate_systematics):
            raise TypeError('propagate_systematics must be a list/tuple of booleans')
        if propagate_systematics and len(propagate_systematics)!=len(arguments):
            raise ValueError('propagate_systematics must be empty or have one flag per variant')
        operation=getattr(self.native,'applyCorrectionsVec' if vector else 'applyCorrections',None)
        if operation is None:
            raise RuntimeError('correction_batch requires a native module built with correction batch support; rebuild rdfanalyzer')
        for output in outputs:
            if self.has_column(output):
                raise ValueError('correction output already exists: '+output+'; use a new output name when changing inputs')
        operation(role,correction,arguments,inputs,outputs,list(propagate_systematics))
        return self

    def weights(self, components, *, normalizations=None, variations=None, output="event_weight"):
        """Use native WeightManager products/replacements and register outputs.

        components maps component names to scalar float/double columns.
        variations maps a nuisance family to (component, up_column, down_column).
        Existing object-dependent component shifts propagate. A nuisance that
        simultaneously changes object inputs and replaces a weight requires
        explicit combined final weights; ambiguous overlaps fail before setup.
        """
        def identifier(value):
            return isinstance(value,str) and re.fullmatch(r"[A-Za-z_]\w*",value,re.ASCII)
        if self._weights:raise ValueError("declare one event-weight product per Analysis; include its components and nuisances in one weights() call")
        if not identifier(output):raise ValueError("weight output must be an identifier")
        normalizations={} if normalizations is None else normalizations
        variations={} if variations is None else variations
        if any(not isinstance(value,Mapping) for value in (components,normalizations,variations)):
            raise TypeError("components, normalizations and variations must be mappings")
        if any(not identifier(name) or not isinstance(column,str) or not column.strip() for name,column in components.items()):
            raise ValueError("weight components require identifiers and input column names")
        if any(not identifier(name) or type(value) not in (int,float) or not math.isfinite(value) for name,value in normalizations.items()):
            raise ValueError("normalizations require identifiers and finite numbers")
        if not math.isfinite(math.prod(normalizations.values())):
            raise ValueError("combined normalization must be finite")
        records=[]
        for family,record in variations.items():
            if (not identifier(family) or family.endswith(('Up','Down')) or not isinstance(record,(tuple,list))
                    or len(record)!=3 or record[0] not in components
                    or any(not isinstance(column,str) or not column.strip() for column in record)):
                raise ValueError("each weight nuisance requires (registered_component, up_column, down_column)")
            records.append((family,*record))
        if variations and not self.config.systematics:
            raise ValueError("weight variations require systematics=True")
        result=dict(self.native._configure_weights(output,list(components.items()),list(normalizations.items()),records))
        self._weights[output]={'components':dict(components),'normalizations':dict(normalizations),
                               'variations':dict(variations),'columns':result}
        return dict(result)

    def region(self, name, column, *, parent=""):
        """Declare a native RegionManager hierarchy and a propagated selection.

        Define every region's boolean input before declaring the first region:
        RegionManager captures its base node at the first declaration.
        """
        if not isinstance(name,str) or not re.fullmatch(r"[A-Za-z_]\w*",name,re.ASCII):
            raise ValueError("region name must be an identifier")
        if name in self._regions:raise ValueError("duplicate region: "+name)
        if parent and parent not in self._regions:raise ValueError("declare parent region first: "+parent)
        if self._region_base_columns is not None and column not in self._region_base_columns:
            raise ValueError('region predicate was not present at the first native region declaration; define all predicates before the first region: '+column)
        if self.native.column_type(column) not in ('bool','Bool_t'):
            raise ValueError('region predicate must be scalar bool: '+column)
        selection="__rdf_python_region_"+name
        if self.has_column(selection):raise ValueError("region helper column already exists: "+selection)
        base=frozenset(self.columns) if self._region_base_columns is None else self._region_base_columns
        chain=list(self.native._declare_region(name,column,parent))
        self._region_base_columns=base
        self.define(selection,' && '.join('('+value+')' for value in chain),chain)
        self._regions[name]={'column':column,'parent':parent,'chain':chain,'selection':selection}
        return self

    def sum(self, name, column):
        """Book a lazy scalar-double sum at the current graph node (nominal only)."""
        self.native.book_sum(name, column)
        self._action_names['sums'].add(name)
        return self

    def apply(self, stage):
        """Explicitly lower an optional phased recipe; never process events."""
        if not isinstance(stage,Stage):raise TypeError('apply requires a Stage')
        return stage.apply(self)

    def use(self, *modules):
        """Compose optional modules once during setup, in exactly this order."""
        return ModuleSequence(modules).apply(self)

    @property
    def sums(self):
        return self.native.sums

    @property
    def histograms(self):
        return self.native.histograms

    @property
    def counts(self):
        return self.native.counts

    def register_variation(self, variable, label, column):
        self.native.register_variation(variable, label, column)
        return self

    def register_variation_pair(self, variable, family, up, down):
        self.native.register_variation_pair(variable, family, up, down)
        return self

    @property
    def columns(self):
        return self.native.columns

    def has_column(self, name):
        """Check one column without exporting the full inventory when supported."""
        method=getattr(self.native,'_has_column',None)
        return bool(method(name)) if method is not None else name in self.columns

    @property
    def variation_labels(self):
        return self.native.variation_labels

    @property
    def selection_masks(self):
        return self.native.selection_masks

    def run(self):
        if getattr(self, '_nd_failure', None):
            raise RuntimeError('ND lowering failed; start a fresh Analysis: '+self._nd_failure)
        if self._module_active:
            raise RuntimeError('Module builders construct the graph; run() is only allowed after module application')
        if self._module_failure:
            raise RuntimeError('A module failed during setup; start a fresh Analysis: '+self._module_failure)
        if self._stage_failure:
            raise RuntimeError('A stage failed during native lowering; start a fresh Analysis: '+self._stage_failure)
        if self._run_status is not None:
            raise RuntimeError('Analysis execution has already been attempted; start a fresh Analysis')
        self._run_status = 'running'
        started = time.perf_counter()
        try:
            self.native.run()
        except BaseException:
            self._run_status = 'failed'
            raise
        finally:
            self._run_wall_s = time.perf_counter()-started
        self._run_status = 'succeeded'
        # All lazy consumers have completed; generated configuration files no
        # longer need to remain on disk. A second native run is rejected.
        self._directory.cleanup()
        return self

    def result(self):
        """Snapshot completed in-memory actions without running or reading files.

        ``run()`` still returns this Analysis. A failed/unrun analysis has no
        successful snapshot; execution errors retain their original exceptions.
        """
        if self._run_status != 'succeeded':
            raise RuntimeError('Results require a successful Analysis.run(); no event processing was triggered')
        from rdfresults import RunResult
        settings = self.config.native_config()
        outputs = {'metadata': settings['metaFile']} if settings['metaFile'] else {}
        if self.config.skim:
            outputs['skim'] = self.config.output
        return RunResult('python-numba', 'succeeded',
            'native Analysis.run returned; booked in-memory summaries, no persisted-output or physics validation',
            counts=self.counts, histograms=self.histograms, sums=self.sums,
            timings={'execute_s': self._run_wall_s}, outputs=outputs,
            details={'compilation': self.compilation_stats, 'nd_histograms':copy.deepcopy(getattr(self, '_nd_groups', {})), 'threading':self.runtime_threading}, event_loops=self.native.event_loops)

    def inspect(self):
        """Describe the constructed graph without triggering an event loop."""
        return {"columns": list(self.columns), "variations": list(self.variation_labels),
                "selection_masks": dict(self.selection_masks), "compilation": self.compilation_stats,
                "weights":copy.deepcopy(self._weights),"regions":copy.deepcopy(self._regions),
                "stages":copy.deepcopy(self._stages),"stage_failure":self._stage_failure,
                "kernel_outputs":copy.deepcopy(self._kernel_outputs),
                "nd_histograms":copy.deepcopy(getattr(self, '_nd_groups', {})), "nd_failure":getattr(self, '_nd_failure', None),
                "modules":copy.deepcopy(self._modules),"module_failure":self._module_failure}

    def describe(self, *, include_types=False):
        """Return the common description envelope without executing the graph.

        Tracked operations retain construction-time dependencies and action
        stages. Native/plugin internals remain opaque. Type lookup is explicit
        because it can add setup work on a large graph.
        """
        from rdfworkflow import WorkflowDescription
        # Check the native running guard before touching the current inventory.
        # The GIL remains held throughout this read-only setup inspection.
        graph=self.native._graph_description() if hasattr(self.native,'_graph_description') else None
        inventory=self.inspect()
        if include_types:
            inventory['column_types']={name:self.native.column_type(name) for name in inventory['columns']}
        details={
            'configuration':self.config.describe().as_dict()['details'],
            'threading':self.runtime_threading,
            'inventory':inventory,
            'graph_completeness':'partial; initial configuration, services and untracked native/plugin operations are opaque'}
        capabilities=['configuration','graph-inventory']
        capabilities.extend(_native_features(self.native))
        if graph is not None:
            details['graph']=graph
            capabilities.append('authoring-graph')
        else:
            details['graph_completeness']='inventory only; rebuild the native module for tracked dependency/stage explanations'
        return WorkflowDescription('python-numba',tuple(capabilities),details)

    def explain(self, name=None, *, variation=None):
        """Explain tracked definitions/actions and upstream work without running.

        ``variation`` chooses a recorded branch for definitions and filters.
        Actions always retain their booked variation and selection stage.
        Names may select multiple kinds (for example a column and histogram).
        The versioned machine-readable records are in ``describe()``.
        """
        details=self.describe().as_dict()['details']
        if 'graph' not in details:
            raise RuntimeError(details['graph_completeness'])
        if not details['graph']['enabled']:
            raise RuntimeError('Graph recording is disabled; construct Analysis with record_graph=True to explain authoring operations')
        if variation is not None and variation not in details['inventory']['variations']:
            raise ValueError(f"Unknown variation {variation!r}; available: {details['inventory']['variations']}")
        operations=details['graph']['operations']
        selected=[i for i,op in enumerate(operations) if name is None or op['name']==name
                  or any(item['output']==name for item in op['materializations'])]
        if name is not None and not selected:
            raise ValueError(f"No tracked operation for {name!r}; input columns and opaque native/plugin work have no authoring record")

        def branch(op, label):
            label=op['attributes'].get('variation',label)
            output=op['variations'].get(label)
            definition=next((item for item in op['materializations'] if item['output']==output),None)
            return label,output,definition['inputs'] if definition else op['inputs']

        # Follow actual recorded materializations, not names guessed from a
        # suffix. Only earlier operations can supply a dependency at this node.
        producers={}
        for index,op in enumerate(operations):
            for item in op['materializations']:
                producers.setdefault(item['output'],[]).append(index)
        wanted={};pending=[]
        for index in selected:
            op=operations[index]
            label=variation or 'Nominal'
            if variation is None and name!=op['name']:
                label=next((key for key,value in sorted(op['variations'].items()) if value==name),label)
            pending.append((index,label))
        while pending:
            index,label=pending.pop()
            op=operations[index];label,_,inputs=branch(op,label)
            labels=wanted.setdefault(index,set())
            if label in labels:continue
            labels.add(label)
            if op['kind'] in ('histogram','sum','count'):
                for earlier in range(index):
                    if operations[earlier]['kind']=='filter':
                        for selected_label in (op['selection_masks'] if op['kind']=='count' else [label]):
                            pending.append((earlier,selected_label))
            for column in inputs:
                for earlier in reversed(producers.get(column,())):
                    if earlier>=index:continue
                    producer=operations[earlier]
                    producer_label=label if producer['variations'].get(label)==column else (
                        'Nominal' if producer['variations'].get('Nominal')==column else next(
                            (key for key,value in sorted(producer['variations'].items()) if value==column),'Nominal'))
                    pending.append((earlier,producer_label))
                    break
        lines=['Tracked authoring graph (partial; native/plugin internals are opaque).',
               'Stages count successful binding filters; actions retain their booking stage.']
        for index in sorted(wanted):
            op=operations[index]
            lines.append(f"[{index}] stage {op['stage']} {op['kind']} {op['name']} ({op['policy']})")
            for selected_label in sorted(wanted[index]):
                label,output,inputs=branch(op,selected_label)
                if op['materializations']:
                    if label not in op['variations']:
                        lines.append(f"  {label}: not registered at construction; no retroactive propagation is implied")
                    elif op['policy']=='nominal-only':
                        lines.append(f"  {label}: reuse {output}; propagation explicitly disabled")
                    elif label!='Nominal' and output==op['name']:
                        lines.append(f"  {label}: reuse {output}; recorded inputs do not require another output")
                    else:
                        lines.append(f"  {label}: {'nominal' if label=='Nominal' else 'recompute'} {output}")
                if inputs:lines.append('  inputs: '+', '.join(inputs))
            if op['kind']=='variation-registration':
                lines.append('  registered: '+', '.join(f'{key}={value}' for key,value in sorted(op['variations'].items())))
            masks=op['selection_masks']
            if op['kind']=='count':
                lines.append('  counts rows retained by the variation union; no nominal mask is applied')
            if masks:
                lines.append('  masks: '+', '.join(f'{key}={value or "all rows"}' for key,value in sorted(masks.items())))
            if op['kind'] in ('histogram','sum'):
                lines.append(f'  booked variation: {label}')
                if variation is not None and variation!=label:
                    lines.append(f'  requested {variation} does not change this booked action')
        return '\n'.join(lines)


class WorkerRecycleRequired(RuntimeError):
    """The configured worker budget is exhausted; start a fresh process."""


@dataclass(frozen=True)
class AnalysisResult:
    """Detached results: retaining this object does not retain a native graph."""
    counts: dict
    histograms: dict
    sums: dict
    compilation: dict
    timing: dict
    event_loops: int
    output: Optional[str]
    metadata: str
    nd_histograms: dict = field(default_factory=dict)

    def as_dict(self):
        return copy.deepcopy(self.__dict__)

    def to_run_result(self):
        """Adapt the existing detached session result without reopening outputs."""
        from rdfresults import RunResult
        outputs = {'metadata': self.metadata} if self.metadata else {}
        if self.output is not None:
            outputs['skim'] = self.output
        return RunResult('python-numba-session', 'succeeded',
            'session job returned from native Analysis.run; no persisted-output or physics validation',
            counts=self.counts, histograms=self.histograms, sums=self.sums,
            timings=self.timing, outputs=outputs, details={'compilation': self.compilation, 'nd_histograms':self.nd_histograms},
            event_loops=self.event_loops)


class AnalysisSession:
    """Run independent jobs serially in a worker with bounded compilation reuse.

    ROOT cannot unload generated JIT code safely. Budgets request process
    recycling instead of pretending to evict that native memory. This shares
    the existing process-local Numba registrations, not event results or files.
    """
    def __init__(self, *, max_jobs=16, max_numba_signatures=64):
        if any(type(v) is not int or v < 1 for v in (max_jobs, max_numba_signatures)):
            raise ValueError("session budgets must be positive integers")
        self.max_jobs, self.max_numba_signatures = max_jobs, max_numba_signatures
        self._jobs = 0
        self._running = False
        self._closed = False
        self._paths = set()

    @property
    def cache_info(self):
        return {"registered_signatures": len(_numba_cache),
                "compilation_attempts": _numba_compilation_attempts,
                "jobs_started": self._jobs, "max_jobs": self.max_jobs,
                "max_numba_signatures": self.max_numba_signatures}

    def _before_compilation(self):
        if _numba_compilation_attempts >= self.max_numba_signatures:
            raise WorkerRecycleRequired("Numba compilation budget reached; recycle this worker process")

    def run(self, config, builder, *args, **kwargs):
        if self._closed or self._running:
            raise RuntimeError("session is closed or already running a job")
        if self._jobs >= self.max_jobs:
            raise WorkerRecycleRequired("Job budget reached; recycle this worker process")
        if not isinstance(config, AnalysisConfig) or not callable(builder):
            raise TypeError("run requires an AnalysisConfig and graph-builder callable")
        settings = config.native_config()
        outputs = [settings["metaFile"]] + ([config.output] if config.skim else [])
        paths = {str(Path(path).resolve()) for path in outputs}
        if len(paths) != len(outputs) or paths.intersection(self._paths):
            raise ValueError("Each session job requires distinct skim/metadata output paths")
        self._paths.update(paths)
        self._jobs += 1
        self._running = True
        started = time.perf_counter()
        try:
            analysis = Analysis(config)
            analysis._session = self
            builder(analysis, *args, **kwargs)
            ready = time.perf_counter()
            analysis.run()
            finished = time.perf_counter()
            return AnalysisResult(counts=dict(analysis.counts), histograms=dict(analysis.histograms),
                sums=dict(analysis.sums), compilation=analysis.compilation_stats,
                timing={"setup_s": ready-started, "execute_s": finished-ready, "total_s": finished-started},
                event_loops=analysis.native.event_loops, output=config.output if config.skim else None,
                metadata=settings["metaFile"], nd_histograms=copy.deepcopy(analysis._nd_groups))
        finally:
            self._running = False

    def close(self):
        if self._running:
            raise RuntimeError("Cannot close a session while a job is running")
        self._closed = True

    def __enter__(self):
        if self._closed:
            raise RuntimeError("session is closed")
        return self

    def __exit__(self, *exc):
        self.close()


def diagnose_runtime(*, numba=False, cms=False, require_capabilities=()):
    """Check this interpreter's native/ROOT/Numba path without opening user data.

    Run as the ``doctor`` CLI in a fresh process, in the same environment as
    the analysis. This is an execution probe, not just package discovery.
    """
    from collections.abc import Sequence
    allowed=tuple(name for name,_ in _NATIVE_FEATURE_METHODS)
    if isinstance(require_capabilities,(str,bytes)) or not isinstance(require_capabilities,Sequence):
        raise TypeError('require_capabilities must be a sequence of capability names')
    required=tuple(require_capabilities)
    if any(type(name) is not str or name not in allowed for name in required):
        raise ValueError('require_capabilities must select from: '+', '.join(allowed))
    required=tuple(dict.fromkeys(required))
    import importlib
    import platform
    import sys
    import sysconfig
    import site
    report={"passed":True,"python":sys.executable,"python_version":platform.python_version(),
            "soabi":sysconfig.get_config_var('SOABI'),"user_site_enabled":site.ENABLE_USER_SITE,
            "user_site":site.getusersitepackages(),"required_capabilities":list(required),"checks":[]}
    modules={}
    def check(name,function):
        try:
            detail=function()
            report['checks'].append({'name':name,'passed':True,'detail':detail})
        except Exception as error:
            report['passed']=False
            report['checks'].append({'name':name,'passed':False,'detail':str(error)})
    def load(name):
        module=importlib.import_module(name);modules[name]=module
        return {'path':getattr(module,'__file__',None),'version':getattr(module,'__version__',None)}
    for name in ('yaml','numpy','ROOT','rdfanalyzer'):check(name,lambda name=name:load(name))
    source_entry_ranges = None
    if 'ROOT' in modules:
        root = modules['ROOT'].gROOT
        report['root_version'] = root.GetVersion()
        root_version = root.GetVersionInt()
        source_entry_ranges = 64000 <= root_version < 64100 or root_version == 64199
    if 'rdfanalyzer' in modules:
        native=modules['rdfanalyzer']
        def native_api():
            if getattr(native,'python_authoring_api_version',0)<_NATIVE_API_VERSION:
                raise RuntimeError('Native authoring API version is older than this frontend; rebuild/install rdfanalyzer and rdfanalysis together')
            needed=('define','_define_compiled','project','book_histogram','register_variation','_configure_weights','_declare_region')
            missing=[name for name in needed if not hasattr(native.Analyzer,name)]
            if missing:raise RuntimeError('Native module is older than this frontend; rebuild rdfanalyzer: '+', '.join(missing))
            return {'has_cms':bool(getattr(native,'has_cms',False)),
                    'native_api_version':native.python_authoring_api_version,'frontend_required_api':_NATIVE_API_VERSION,
                    'optional_capabilities':_native_features(native.Analyzer),
                    'source_entry_ranges':source_entry_ranges}
        check('native authoring API',native_api)
        if required:
            def feature_check():
                available=_native_features(native.Analyzer)
                missing=[name for name in required if name not in available]
                if missing:raise RuntimeError('Loaded rdfanalyzer lacks requested capabilities: '+', '.join(missing)+
                    '; rebuild/install matching native bindings or select a compatible module path')
                return {'required':list(required),'available':available}
            check('required native capabilities',feature_check)
        if cms:
            def cms_api():
                if not getattr(native,'has_cms',False) or not hasattr(native.Analyzer,'configure_extension'):
                    raise RuntimeError('CMS Python authoring is unavailable; enable the CMS extension and rebuild rdfanalyzer')
                return load('rdfanalyzer_cms')
            check('CMS authoring',cms_api)
    if numba:
        for name in ('numba','cffi'):check(name,lambda name=name:load(name))
        if {'ROOT','numba','cffi'}<=modules.keys():
            def jit_probe():
                root=modules['ROOT']
                def square(value):return value*value
                name=f'__rdf_runtime_probe_{next(_numba_names)}'
                owner=root.Numba.Declare(['double'],'double',name=name)(square)
                value=root.RDataFrame(3).Define('probe_input','double(rdfentry_)').Define('probe_output',f'Numba::{name}(probe_input)').Sum('probe_output').GetValue()
                if value!=5.:raise RuntimeError('Numba bridge returned an incorrect probe value')
                return {'sum':value,'events':3,'owner_alive':owner is not None}
            check('ROOT/Numba execution bridge',jit_probe)
    if not report['passed']:
        report['remedy']='Source the ROOT/build environment, use its matching Python ABI, install optional authoring dependencies, rebuild rdfanalyzer, and set the installed or build Python module path.'
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    validate=commands.add_parser('validate',help='validate settings without opening inputs or importing ROOT')
    validate.add_argument('config',help='typed analysis YAML')
    validate.add_argument('--level',choices=('configuration','assets'),default='configuration')
    commands.add_parser('schema',help='print optional typed setting reference without importing YAML or native modules')
    explain=commands.add_parser('explain',help='describe run settings without constructing a native graph')
    explain.add_argument('config',help='typed analysis YAML')
    doctor=commands.add_parser('doctor',help='probe this interpreter/native/ROOT environment without user data')
    doctor.add_argument('--numba',action='store_true',help='compile and execute a tiny NumPy/Numba-compatible kernel')
    doctor.add_argument('--cms',action='store_true',help='require the installed CMS authoring facade')
    doctor.add_argument('--require-capability',action='append',default=[],
        choices=tuple(name for name,_ in _NATIVE_FEATURE_METHODS),
        help='require an optional loaded-module feature; repeat for multiple features')
    args = parser.parse_args(argv)
    if args.command=='schema':
        print(json.dumps(AnalysisConfig.schema(),indent=2,sort_keys=True))
        return 0
    if args.command=='doctor':
        report=diagnose_runtime(numba=args.numba,cms=args.cms,require_capabilities=args.require_capability)
        print(json.dumps(report,indent=2))
        return 0 if report['passed'] else 1
    try:
        config = AnalysisConfig.from_file(args.config)
    except (ValueError, TypeError, OSError) as exc:
        parser.error(str(exc))
    if args.command=='validate' and args.level=='assets':
        report=config.validate('assets')
        print(json.dumps(report.as_dict(),indent=2,sort_keys=True))
        return 0 if report.passed else 1
    report=config.describe().as_dict() if args.command=='explain' else config.native_config()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
