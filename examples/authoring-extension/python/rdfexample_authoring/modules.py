"""Example analysis-owned policy, outside the framework's native dispatch."""
from rdfmodules import AnalysisModule, ModuleSpec

_SCHEMA={'type':'object','additionalProperties':False,
         'required':['input','threshold','output'],
         'properties':{key:{'type':'string'} for key in ('input','threshold','output')}}


def sum_above_threshold(values,threshold):
    # This original Python function is passed directly to ROOT/Numba, never
    # wrapped in a new function for each graph/job. Empty collections sum to 0.
    total=0.
    for value in values:
        if value>threshold:total+=value
    return total


def _validate(settings):
    if set(settings)!=set(_SCHEMA['required']):raise ValueError('HT settings require exactly input, threshold and output')
    if any(not isinstance(value,str) or not value.strip() for value in settings.values()):
        raise ValueError('HT settings must name nonempty dataframe columns')


def _build(analysis,settings):
    analysis.define_numba(settings['output'],sum_above_threshold,
                          [settings['input'],settings['threshold']],return_type='double')


def make_ht_module(*,input_column='Jet_pt',threshold_column='jet_pt_min',output_column='jet_ht',name='example.ht'):
    spec=ModuleSpec(name,requires=(input_column,threshold_column),produces=(output_column,),
                    settings_schema=_SCHEMA,capabilities=('numba','systematic-propagation'),
                    documentation='Sum input elements above an explicit threshold column; empty input returns zero.')
    return AnalysisModule(spec,_build,settings={'input':input_column,'threshold':threshold_column,'output':output_column},
                          validate_settings=_validate)


def module_specs():
    """The generic optional service loads descriptions without building graphs."""
    return (make_ht_module().spec,)
