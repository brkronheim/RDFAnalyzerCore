"""Optional phased declarations lowered once to the existing Analysis methods.

No event callbacks, C++ physics wrappers, native imports or automatic execution.
Definitions and filters retain their interleaved declaration order. Registration,
region and action boundaries remain explicit.
Use separate stages for actions on opposite sides of a filter.
"""
from dataclasses import dataclass
import math
import re
import struct
from typing import Union
from rdfcolumns import KernelOutputs, named_kernel_arguments, projection_arguments


from _rdftext import nonempty as _text


def _identifier(value, location):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z_]\w*',value,re.ASCII):
        raise ValueError(f'{location}: expected an identifier')
    return value


def _path(value, location):
    _text(value,location)
    if ';' in value or any(part in ('','.','..') for part in value.split('/')):
        raise ValueError(f'{location}: expected a relative ROOT path without empty/dot components or cycle delimiters')
    return value


def _sequence(value, location, *, empty=False):
    if not isinstance(value,(list,tuple)) or (not empty and not value):
        raise ValueError(f'{location}: expected a {"possibly empty" if empty else "nonempty"} list or tuple')
    return tuple(value)


@dataclass(frozen=True)
class RegularAxis:
    bins: int
    low: float
    high: float

    def __post_init__(self):
        if type(self.bins) is not int or self.bins<=0:
            raise ValueError('RegularAxis.bins: expected a positive integer')
        if any(type(value) not in (int,float) or not math.isfinite(value) for value in (self.low,self.high)) or self.low>=self.high:
            raise ValueError('RegularAxis: expected finite increasing bounds')


@dataclass(frozen=True)
class VariableAxis:
    edges: tuple

    def __post_init__(self):
        edges=_sequence(self.edges,'VariableAxis.edges')
        if len(edges)<2 or any(type(value) not in (int,float) for value in edges):
            raise ValueError('VariableAxis.edges: expected at least two finite numeric edges')
        try:edges=tuple(float(value) for value in edges)
        except OverflowError as error:raise ValueError('VariableAxis.edges: expected finite double edges') from error
        if any(not math.isfinite(value) for value in edges):
            raise ValueError('VariableAxis.edges: expected finite double edges')
        if any(low>=high for low,high in zip(edges,edges[1:])):
            raise ValueError('VariableAxis.edges: expected strictly increasing edges')
        object.__setattr__(self,'edges',edges)


@dataclass(frozen=True)
class Observable:
    name: str
    column: str
    axis: Union[RegularAxis, VariableAxis]

    def __post_init__(self):
        _identifier(self.name,'Observable.name');_text(self.column,'Observable.column')
        if not isinstance(self.axis,(RegularAxis,VariableAxis)):raise TypeError('Observable.axis must be a RegularAxis or VariableAxis')


def _native_float_bounds(axis):
    bounds = (axis.low, axis.high) if isinstance(axis, RegularAxis) else (axis.edges[0], axis.edges[-1])
    try:
        low, high = (struct.unpack('f', struct.pack('f', value))[0] for value in bounds)
    except (OverflowError, struct.error) as error:
        raise ValueError('ND observable bounds must fit native float bounds') from error
    if not math.isfinite(low) or not math.isfinite(high) or low >= high:
        raise ValueError('ND observable bounds must remain finite and increasing as native floats')
    return low, high


@dataclass(frozen=True)
class NDHistogramSettings:
    backend: str = 'boost'
    precision: str = 'float'
    sparse_backend: str = 'map'
    dense_limit_mib: int = 0
    collection_layout: str = 'fixed'

    def __post_init__(self):
        if self.backend not in ('root', 'boost'): raise ValueError('backend must be root or boost')
        if self.precision not in ('float', 'double'): raise ValueError('precision must be float or double')
        if self.sparse_backend not in ('map', 'flat'): raise ValueError('sparse_backend must be map or flat')
        if self.collection_layout not in ('fixed', 'per-slot'):
            raise ValueError('collection_layout must be fixed or per-slot')
        if type(self.dense_limit_mib) is not int or not 0 <= self.dense_limit_mib <= 2**31-1:
            raise ValueError('dense_limit_mib must be a nonnegative native integer')

    def native_options(self):
        return dict(histogramBackend=self.backend, histogramBoostPrecision=self.precision,
                    histogramBoostSparseBackend=self.sparse_backend,
                    histogramDenseLimitMiB=str(self.dense_limit_mib),
                    histogramCollectionLayout=self.collection_layout)


@dataclass(frozen=True)
class NDCategoryAxis:
    column: str
    axis: RegularAxis
    labels: tuple

    def __post_init__(self):
        _text(self.column, 'NDCategoryAxis.column')
        if not isinstance(self.axis, RegularAxis): raise TypeError('ND categories require a RegularAxis')
        if self.axis.bins > 2**31-1: raise ValueError('category bins exceed native integer range')
        _native_float_bounds(self.axis)
        labels = _sequence(self.labels, 'NDCategoryAxis.labels')
        for label in labels: _text(label, 'NDCategoryAxis.labels')
        if len(labels) != self.axis.bins or len(set(labels)) != len(labels):
            raise ValueError('category labels must be unique, with one label per bin')
        object.__setattr__(self, 'labels', labels)

    def native_descriptor(self):
        low, high = _native_float_bounds(self.axis)
        return (self.column, self.axis.bins, low, high, self.labels)


@dataclass(frozen=True)
class NDHistogramSpec:
    name: str
    column: str
    axis: Union[RegularAxis, VariableAxis]
    weight: str
    label: str = ''
    force_sparse: bool = False

    def __post_init__(self):
        _identifier(self.name, 'NDHistogramSpec.name'); _text(self.column, 'NDHistogramSpec.column')
        _text(self.weight, 'NDHistogramSpec.weight')
        if not isinstance(self.label, str) or any(c in self.label for c in '\0\n\r'):
            raise ValueError('NDHistogramSpec.label must be text without NUL/newlines')
        if not isinstance(self.axis, (RegularAxis, VariableAxis)):
            raise TypeError('ND observable requires RegularAxis or VariableAxis')
        bins = self.axis.bins if isinstance(self.axis, RegularAxis) else len(self.axis.edges)-1
        if bins > 2**31-1: raise ValueError('observable bins exceed native integer range')
        _native_float_bounds(self.axis)
        if type(self.force_sparse) is not bool: raise TypeError('force_sparse must be boolean')

    def native_descriptor(self):
        low, high = _native_float_bounds(self.axis)
        edges = self.axis.edges if isinstance(self.axis, VariableAxis) else ()
        bins = len(edges)-1 if edges else self.axis.bins
        return (self.name, self.column, self.label, self.weight, bins, low, high, edges, self.force_sparse)


@dataclass(frozen=True)
class NDHistogramGroup:
    name: str
    histograms: tuple
    channel: object = None
    control: object = None
    sample: object = None
    variations: str = 'all'

    def __post_init__(self):
        _identifier(self.name, 'NDHistogramGroup.name')
        histograms = _sequence(self.histograms, 'NDHistogramGroup.histograms')
        if any(not isinstance(item, NDHistogramSpec) for item in histograms):
            raise TypeError('histograms must contain NDHistogramSpec objects')
        if len({item.name for item in histograms}) != len(histograms):
            raise ValueError('duplicate ND histogram names')
        for axis in (self.channel, self.control, self.sample):
            if axis is not None and not isinstance(axis, NDCategoryAxis):
                raise TypeError('categories must be NDCategoryAxis or None')
        if self.variations not in ('all', 'nominal'):
            raise ValueError('ND variations supports only all or nominal')
        object.__setattr__(self, 'histograms', histograms)

    def native_descriptors(self, *, max_histograms=1000):
        if type(max_histograms) is not int or max_histograms < 1: raise ValueError('max_histograms must be positive')
        if len(self.histograms) > max_histograms: raise ValueError('ND histogram group exceeds histogram budget')
        categories = tuple(axis.native_descriptor() if axis is not None else
                           ('zero__', 1, 0., 1., ('Default',))
                           for axis in (self.channel, self.control, self.sample))
        return tuple(item.native_descriptor() for item in self.histograms), categories

    def describe(self):
        requests, categories = self.native_descriptors(max_histograms=max(1, len(self.histograms)))
        return dict(name=self.name, variations=self.variations,
                    axis_roles=['channel', 'control', 'sample', 'systematic', 'observable'],
                    categories=[dict(column=c[0], bins=c[1], low=c[2], high=c[3], labels=list(c[4])) for c in categories],
                    histograms=[dict(name=r[0], native_key=r[0]+'_', column=r[1], label=r[2], weight=r[3],
                                     bins=r[4], low=r[5], high=r[6], edges=list(r[7]), force_sparse=r[8]) for r in requests],
                    logical_cells_per_systematic_bin=sum((r[4]+2) * math.prod(c[1]+2 for c in categories) for r in requests),
                    storage='native configured backend/precision/dense eligibility; no allocation in description')


@dataclass(frozen=True)
class HistogramGroup:
    name: str
    observables: tuple
    regions: tuple=(None,)
    variations: object=('Nominal',)
    families: tuple=()
    weight: str=''

    def __post_init__(self):
        _path(self.name,'HistogramGroup.name')
        object.__setattr__(self,'observables',_sequence(self.observables,'HistogramGroup.observables'))
        if any(not isinstance(item,Observable) for item in self.observables):raise TypeError('observables must contain Observable objects')
        if len({item.name for item in self.observables})!=len(self.observables):raise ValueError('duplicate observable names')
        object.__setattr__(self,'regions',_sequence(self.regions,'HistogramGroup.regions'))
        for region in self.regions:
            if region is not None:_identifier(region,'HistogramGroup.regions')
        if len(set(self.regions))!=len(self.regions):raise ValueError('duplicate region references')
        if self.variations!='all':
            object.__setattr__(self,'variations',_sequence(self.variations,'HistogramGroup.variations',empty=True))
            for label in self.variations:_path(label,'HistogramGroup.variations')
        object.__setattr__(self,'families',_sequence(self.families,'HistogramGroup.families',empty=True))
        for family in self.families:_text(family,'HistogramGroup.families')
        if not self.variations and not self.families:raise ValueError('select at least one variation or family')
        if not isinstance(self.weight,str):raise TypeError('HistogramGroup.weight must be a column string')

    def resolve(self, labels, *, budget, strict=True):
        selected=list(labels) if self.variations=='all' else list(self.variations)
        for family in self.families:selected.extend((family+'Up',family+'Down'))
        selected=list(dict.fromkeys(selected))
        if strict and set(selected)-set(labels):
            raise ValueError(f'{self.name}: unknown variations: '+', '.join(sorted(set(selected)-set(labels))))
        size=len(self.observables)*len(self.regions)*len(selected)
        if size>budget:raise ValueError(f'{self.name}: expands to {size} histograms, exceeding remaining stage budget {budget}')
        result=[]
        for region in self.regions:
            for label in selected:
                _path(label,'variation output path')
                for observable in self.observables:
                    axis=observable.axis
                    item=dict(name=f'{self.name}/{region or "inclusive"}/{label}/{observable.name}',
                        variable=observable.column,region=region,variation=label,weight=self.weight)
                    if isinstance(axis,VariableAxis):item['edges']=axis.edges
                    else:item.update(bins=axis.bins,low=axis.low,high=axis.high)
                    result.append(item)
        return result


class Stage:
    """Reusable declaration recipe with explicit phases and bounded expansion.

    Building/previewing a recipe never compiles a kernel or processes events.
    apply() is not transactional after native lowering starts: failures poison
    frontend run() to prevent accidentally executing an incomplete analysis.
    """
    def __init__(self, name, *, max_histograms=1000, record_sources=False):
        self._name=_identifier(name,'Stage.name')
        if type(max_histograms) is not int or max_histograms<1:raise ValueError('max_histograms must be a positive integer')
        if type(record_sources) is not bool:raise TypeError('record_sources must be a boolean')
        self._max_histograms=max_histograms
        self._steps=[];self._phase=0;self._sealed=False
        self._record_sources=record_sources;self._sources={};self._source_keys={}

    @property
    def name(self):return self._name

    @property
    def max_histograms(self):return self._max_histograms

    def _add(self, phase, method, *args, **kwargs):
        if self._sealed:raise RuntimeError('Stage recipe is sealed after lowering starts; construct another recipe to edit it')
        if phase<self._phase:
            raise ValueError('Stage phases must be registrations, interleaved definitions/filters, regions, actions; use another stage for a different booking node')
        if self._record_sources:
            import sys
            # Keep primitive values only. Retaining a frame would retain the
            # caller's analysis, arrays and arbitrary locals across jobs.
            frame=sys._getframe(2)
            try:
                index=len(self._steps)
                self._sources[index]=dict(file=frame.f_code.co_filename,
                    line=frame.f_lineno,function=frame.f_code.co_name)
            finally:del frame
            if phase>=3:
                first=args[0]
                key=first.name if method in ('histograms','nd_histograms') else (
                    first['name'] if method in ('histogram','histogram_variable') else first)
                self._source_keys[(method,key)]=index
        self._phase=phase;self._steps.append((phase,method,args,kwargs));return self

    def _action_source(self, method, values):
        if not self._record_sources:return None
        def group_key(name):
            matches=(key for key in self._source_keys if key[0]=='histograms' and name.startswith(key[1]+'/'))
            return max(matches,key=lambda key:len(key[1]),default=None)
        if method=='histogram_batch':
            key=group_key(values[0]['name'])
        elif method in ('histogram','histogram_variable'):
            name=values['name'];key=(method,name)
            if key not in self._source_keys:key=group_key(name)
        else:key=(method,values[0].name if method=='nd_histograms' else values[0])
        return self._sources.get(self._source_keys.get(key))

    @staticmethod
    def _source_hint(site):
        return '' if site is None else f" [declared at {site['file']}:{site['line']} in {site['function']}]"

    def register_variation(self, variable, label, column):
        for value in (variable,label,column):_text(value,'variation registration')
        return self._add(0,'register_variation',variable,label,column)

    def register_variation_pair(self, variable, family, up, down):
        for value in (variable,family,up,down):_text(value,'variation pair')
        return self._add(0,'register_variation_pair',variable,family,up,down)

    def define(self, name, expression, columns=()):
        _identifier(name,'definition.name');_text(expression,'definition.expression')
        return self._add(1,'define',name,expression,_sequence(columns,'columns',empty=True))

    def define_nominal(self, name, expression, columns=()):
        _identifier(name,'definition.name');_text(expression,'definition.expression')
        return self._add(1,'define_nominal',name,expression,_sequence(columns,'columns',empty=True))

    def define_numba(self, name, function, columns, **kwargs):
        _identifier(name,'definition.name')
        if not callable(function):raise TypeError('define_numba requires a Python callable')
        if set(kwargs)-{'return_type','nominal','cache_version','require_native'}:raise ValueError('unknown define_numba options')
        if 'nominal' in kwargs and type(kwargs['nominal']) is not bool:raise ValueError('nominal must be a boolean')
        if 'require_native' in kwargs and type(kwargs['require_native']) is not bool:raise ValueError('require_native must be a boolean')
        if kwargs.get('cache_version') is not None and type(kwargs['cache_version']) not in (str,int):
            raise ValueError('cache_version must be a string, integer or None')
        return self._add(1,'define_numba',name,function,_sequence(columns,'columns',empty=True),**kwargs)

    def define_numba_outputs(self, outputs, function, columns, *, return_type, cache_version=None, require_native=False):
        named_kernel_arguments(outputs,return_type)
        if not callable(function):raise TypeError('define_numba_outputs requires a Python callable')
        if type(require_native) is not bool:raise ValueError('require_native must be a boolean')
        if cache_version is not None and type(cache_version) not in (str,int):
            raise ValueError('cache_version must be a string, integer or None')
        return self._add(1,'define_numba_outputs',outputs,function,_sequence(columns,'columns',empty=True),
                         return_type=return_type,cache_version=cache_version,
                         **({'require_native': True} if require_native else {}))

    def project(self, name, source, index=None, *, nominal=None):
        source,index,nominal=projection_arguments(source,index,nominal)
        _identifier(name,'projection.name');_text(source,'projection.source')
        if type(index) is not int or index<0:raise ValueError('projection index must be a nonnegative integer')
        if type(nominal) is not bool:raise ValueError('projection nominal must be a boolean')
        return self._add(1,'project',name,source,index,nominal=nominal)

    def filter(self, name, expression, columns=()):
        _text(name,'filter.name');_identifier('pass_'+name,'filter.name');_text(expression,'filter.expression')
        return self._add(1,'filter',name,expression,_sequence(columns,'columns',empty=True))

    def filter_column(self, name, column):
        """Use an existing scalar bool at this exact interleaved stage position."""
        _text(name,'filter.name');_identifier('pass_'+name,'filter.name');_text(column,'filter.column')
        return self._add(1,'filter_column',name,column)

    def region(self, name, column, *, parent=''):
        _identifier(name,'region.name');_text(column,'region.column')
        if parent:_identifier(parent,'region.parent')
        return self._add(3,'region',name,column,parent=parent)

    def count(self, name):
        _text(name,'count.name');return self._add(4,'count',name)

    def sum(self, name, column):
        _text(name,'sum.name');_text(column,'sum.column');return self._add(4,'sum',name,column)

    def histograms(self, group, *, batch=None):
        if not isinstance(group,HistogramGroup):raise TypeError('histograms requires a HistogramGroup')
        if batch is not None and type(batch) is not bool:raise TypeError('batch must be a boolean or None')
        return self._add(4,'histograms',group,batch=batch)

    def nd_histograms(self, group):
        if not isinstance(group, NDHistogramGroup): raise TypeError('nd_histograms requires NDHistogramGroup')
        return self._add(4,'nd_histograms',group)

    def histogram(self, name, variable, bins, low, high, *, weight='', selection='', variation='Nominal', region=None):
        _path(name,'histogram.name');_text(variable,'histogram.variable');_text(variation,'histogram.variation')
        RegularAxis(bins,low,high)
        if not isinstance(weight,str) or not isinstance(selection,str):raise ValueError('weight and selection must be column strings')
        if region is not None:
            _identifier(region,'histogram.region')
            if selection:raise ValueError('use region or selection, not both')
        return self._add(4,'histogram',dict(name=name,variable=variable,bins=bins,low=low,high=high,
                                         weight=weight,selection=selection,variation=variation,region=region))

    def histogram_variable(self, name, variable, edges, *, weight='', selection='', variation='Nominal', region=None):
        _path(name,'histogram.name');_text(variable,'histogram.variable');_text(variation,'histogram.variation')
        axis=VariableAxis(edges)
        if not isinstance(weight,str) or not isinstance(selection,str):raise ValueError('weight and selection must be column strings')
        if region is not None:
            _identifier(region,'histogram.region')
            if selection:raise ValueError('use region or selection, not both')
        return self._add(4,'histogram_variable',dict(name=name,variable=variable,edges=axis.edges,
                                         weight=weight,selection=selection,variation=variation,region=region))

    def describe(self):
        from dataclasses import asdict
        from rdfworkflow import WorkflowDescription
        def value(item):
            if isinstance(item,HistogramGroup):return asdict(item)
            if isinstance(item,NDHistogramGroup):return item.describe()
            if isinstance(item,KernelOutputs):return item.describe()
            if callable(item):return {'callable':getattr(item,'__qualname__',type(item).__name__),
                                      'module':getattr(item,'__module__',None)}
            return item
        return WorkflowDescription('authoring-declarations',('configuration',),{
            'name':self.name,'max_histograms':self.max_histograms,'sealed':self._sealed,
            'steps':[{'phase':phase,'method':method,'args':[value(item) for item in args],'kwargs':kwargs,
                      **({'source':dict(self._sources[index])} if index in self._sources else {})}
                     for index,(phase,method,args,kwargs) in enumerate(self._steps)],
            'execution':'not run; definitions compile only during explicit apply()',
            'variation_expansion':'Nominal by default; all/families resolve against registered labels before booking'})

    def _plan(self, analysis, *, strict, lowered=False):
        if getattr(analysis,'_stage_failure',None):raise RuntimeError('A stage failed during native lowering; start a fresh Analysis')
        if any(stage['name']==self.name for stage in analysis._stages):raise ValueError('duplicate stage name: '+self.name)
        point_lookup=getattr(getattr(analysis,'native',None),'_has_column',None)
        columns=set() if point_lookup else set(analysis.columns)
        def exists(name):return name in columns or bool(point_lookup and point_lookup(name))
        regions=dict(analysis._regions);pending={};labels=list(analysis.variation_labels)
        filters=set(analysis._filter_names)
        for phase,method,args,kwargs in self._steps:
            if phase==0:
                labels.extend([args[1]] if method=='register_variation' else [args[1]+'Up',args[1]+'Down'])
            elif phase==1:
                # Explicit dependencies must exist at this declaration position.
                # Empty expression-column lists remain opaque to structural checks.
                if not lowered:
                    required = (() if method=='filter_column' else (args[1],) if method=='project' else args[2])
                    missing = [column for column in required if not exists(column)]
                    if missing:
                        output = args[0].column if method=='define_numba_outputs' else args[0]
                        raise ValueError(f'{method} {output}: unknown explicit input columns at this declaration position: '
                                         + ', '.join(missing) + '; define them earlier or correct the column names')
                if method in ('filter','filter_column'):
                    if not lowered and args[0] in filters:raise ValueError('duplicate filter: '+args[0])
                    if method=='filter_column' and not exists(args[1]):
                        raise ValueError(f'filter {args[0]}: unknown predicate {args[1]} at this declaration position')
                    filters.add(args[0])
                    columns.add('pass_'+args[0])
                else:
                    name=args[0].column if method=='define_numba_outputs' else args[0]
                    if not lowered and exists(name):raise ValueError('duplicate definition: '+name)
                    columns.add(name)
            elif phase==3:
                name,column=args
                if name in regions or name in pending:raise ValueError('duplicate region: '+name)
                if not exists(column):raise ValueError(f'region {name}: unknown predicate {column}')
                base=analysis._region_base_columns
                if base is not None and column not in base:
                    raise ValueError(f'region {name}: {column} was not present at the first native region declaration; define all predicates before the first region')
                helper='__rdf_python_region_'+name
                if exists(helper):raise ValueError('region helper column already exists: '+helper)
                columns.add(helper);pending[name]=(column,kwargs['parent'])
        order=[]
        while pending:
            ready=[name for name,(_,parent) in pending.items() if not parent or parent in regions]
            if not ready:
                raise ValueError('region parents are missing or cyclic: '+', '.join(pending))
            for name in ready:
                column,parent=pending.pop(name);regions[name]={'column':column,'parent':parent}
                order.append((name,column,parent))
        labels=list(dict.fromkeys(labels));resolved=[];counts=set(analysis._action_names['counts']);sums=set(analysis._action_names['sums'])
        histograms=set(analysis._action_names['histograms']);budget=self.max_histograms
        nd_groups=getattr(analysis,'_nd_groups',{})
        nd_names=set(nd_groups)
        nd_keys={item['native_key'] for group in nd_groups.values() for item in group['histograms']}
        directories={name[:index] for name in histograms for index,char in enumerate(name) if char=='/'}
        for phase,method,args,kwargs in self._steps:
            if phase!=4:continue
            if method=='nd_histograms':
                group=args[0]
                if budget<1: raise ValueError('ND histogram expansion exceeds remaining stage budget')
                requests,categories=group.native_descriptors(max_histograms=budget)
                if group.name in nd_names: raise ValueError('duplicate ND group: '+group.name)
                keys={row[0]+'_' for row in requests}
                if keys.intersection(nd_keys): raise ValueError('duplicate ND histogram destination')
                for column in [row[1] for row in requests]+[row[3] for row in requests]+[c[0] for c in categories if c[0]!='zero__']:
                    if not exists(column): raise ValueError('unknown ND input column at action position: '+column)
                nd_names.add(group.name);nd_keys.update(keys);budget-=len(requests)
                resolved.append((method,(group,)));continue
            if method in ('histograms','histogram','histogram_variable'):
                if method=='histograms':
                    group=args[0]
                    unknown={name for name in group.regions if name is not None and name not in regions}
                    if unknown:raise ValueError(f'{group.name}: unknown regions: '+', '.join(sorted(unknown)))
                    items=group.resolve(labels,budget=budget,strict=strict)
                else:
                    item=args[0]
                    if budget<1:raise ValueError('histogram expansion exceeds stage budget')
                    if item['region'] is not None and item['region'] not in regions:raise ValueError('unknown region: '+item['region'])
                    if strict and item['variation'] not in labels:raise ValueError('unknown variation: '+item['variation'])
                    items=[dict(item)]
                budget-=len(items)
                for item in items:
                    name=item['name']
                    if name in counts:raise ValueError('duplicate histogram/count destination: '+name)
                    parents={name[:index] for index,char in enumerate(name) if char=='/'}
                    if name in directories or parents.intersection(histograms):
                        raise ValueError('histogram/directory path collision: '+name)
                    counts.add(name);histograms.add(name);directories.update(parents)
                batch=kwargs.get('batch')
                if batch is None:
                    # The group is one explicit action position. Never carry a
                    # selected-node cache across groups, definitions or filters.
                    # Old modules retain ordinary native booking; preview and
                    # successful stage descriptions expose the chosen action.
                    operation='book_histograms_mixed' if any('edges' in item for item in items) else 'book_histograms'
                    batch=len(items)>1 and callable(getattr(getattr(analysis,'native',None),operation,None))
                if batch and items:resolved.append(('histogram_batch',items))
                else:resolved.extend(('histogram_variable' if 'edges' in item else 'histogram',item) for item in items)
            else:
                names=counts if method=='count' else sums
                if args[0] in names:raise ValueError(f'duplicate {method} destination: '+args[0])
                names.add(args[0]);resolved.append((method,args))
        return order,resolved

    def preview(self, analysis):
        """Provisional expansion; no definitions are compiled or booked.

        Native expression discovery can add labels during lowering. The final
        expansion is checked again against the same budget before any booking.
        """
        from rdfworkflow import WorkflowDescription
        order,actions=self._plan(analysis,strict=False)
        return WorkflowDescription('authoring-declarations',('provisional-plan',),{
            'name':self.name,'region_order':[name for name,_,_ in order],
            'actions':[{'kind':kind,'arguments':values[0].describe() if kind=='nd_histograms' else values} for kind,values in actions],
            'completeness':'provisional; native-discovered labels and types resolve during apply(), before action booking'})

    def validate(self, analysis, *, strict=False):
        """Report structural planning errors without lowering or execution.

        Strict mode requires requested variation labels to exist already. This
        does not compile expressions/kernels, inspect event-level array lengths,
        validate native types or infer opaque plugin dependencies.
        """
        from rdfworkflow import ValidationIssue,ValidationReport
        if type(strict) is not bool:raise TypeError('strict must be a boolean')
        try:self._plan(analysis,strict=strict)
        except (ValueError,TypeError,RuntimeError) as error:
            return ValidationReport('declarations',('declarations',),(
                ValidationIssue('stage.invalid_declarations',str(error),self.name),))
        return ValidationReport('declarations',('declarations',))

    def apply(self, analysis):
        # Structural failures leave the analyzer untouched and the recipe editable.
        self._plan(analysis,strict=False)
        self._sealed=True;operation='start';site=None
        try:
            for index,(phase,method,args,kwargs) in enumerate(self._steps):
                if phase<=2:
                    operation=f'{method} {args[0]}'
                    site=self._sources.get(index)
                    getattr(analysis,method)(*args,**kwargs)
            operation='resolve actions';site=None
            order,actions=self._plan(analysis,strict=True,lowered=True)
            for name,column,parent in order:
                operation=f'region {name}'
                site=self._sources.get(self._source_keys.get(('region',name)))
                if analysis.native.column_type(column) not in ('bool','Bool_t'):
                    raise ValueError(f'region {name}: predicate {column} must be scalar bool')
            for name,column,parent in order:
                operation=f'region {name}'
                site=self._sources.get(self._source_keys.get(('region',name)))
                analysis.region(name,column,parent=parent)
            for method,values in actions:
                label=(values['name'] if method in ('histogram','histogram_variable') else
                       values[0].name if method=='nd_histograms' else
                       f'{len(values)} histograms' if method=='histogram_batch' else values[0])
                operation=f'{method} {label}'
                site=self._action_source(method,values)
                if method in ('histogram','histogram_variable'):getattr(analysis,method)(**values)
                elif method=='histogram_batch':analysis.histogram_batch(values,max_histograms=self.max_histograms)
                elif method=='nd_histograms':analysis.nd_histograms(values[0],max_histograms=self.max_histograms)
                else:getattr(analysis,method)(*values)
            analysis._stages.append({'name':self.name,'histograms':sum(len(values) if method=='histogram_batch' else len(values[0].histograms) if method=='nd_histograms' else method in ('histogram','histogram_variable') for method,values in actions),
                'region_order':[name for name,_,_ in order],
                'actions':[{'kind':method,'arguments':values[0].describe() if method=='nd_histograms' else values} for method,values in actions],
                **({'declaration_sources':{str(index):dict(source) for index,source in self._sources.items()}}
                   if self._record_sources else {})})
            return analysis
        except Exception as error:
            hint=self._source_hint(site)
            analysis._stage_failure=f'{self.name}: {operation}{hint}: {error}'
            raise RuntimeError(f'Stage {self.name!r} failed at {operation}{hint}: {error}. Native lowering is not transactional; start a fresh Analysis.') from error
