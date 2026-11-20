"""Declared composition boundaries and optional discovery, without ROOT."""
import gc
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
import weakref

from rdfanalysis import Analysis,ActionClaim,AnalysisModule,ModuleSpec,ModuleSequence

REPO=Path(__file__).resolve().parents[3]


class Inventory:
    use=Analysis.use
    run=Analysis.run
    def __init__(self):
        self.fields={'pt','cut'};self.queries=[];self.events=[]
        self._modules={};self._module_active=None;self._module_failure=None;self._stage_failure=None
        self._action_names={'counts':set(),'sums':set(),'histograms':set()}
        self.native=SimpleNamespace(event_loops=0,run=lambda:self.events.append('run'))
        self._directory=SimpleNamespace(cleanup=lambda:None)
    def has_column(self,name):self.queries.append(name);return name in self.fields


def module(name,**kwargs):
    return AnalysisModule(ModuleSpec(name,**kwargs),lambda analysis,settings:None)


class ModuleContractTests(unittest.TestCase):
    def test_compiled_analysis_metadata_stays_independent_of_the_native_python_stack(self):
        env=dict(os.environ,PYTHONPATH=str(REPO/'core/python'))
        code="""
import json,sys
from rdfworkflow import CppAnalysisSpec
from rdfmodules import ModuleSpec
run=CppAnalysisSpec('/tmp/example-not-built')
module=ModuleSpec('external.compiled_ml',implementation='compiled-cpp',outputs_at='native-lifecycle',
                  requires=('Jet_pt',),produces=('score',),capabilities=('inference',))
json.dumps({'run':run.describe().as_dict(),'module':module.describe().as_dict()})
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
print('clean')
"""
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,capture_output=True,text=True,check=True)
        self.assertEqual(result.stdout.strip(),'clean')

    def test_settings_schema_descriptions_are_frozen_detached_and_provider_validated(self):
        source={'nested':{'thresholds':[1,2]}};schema={'properties':{'nested':{'type':'object'}}}
        calls=[]
        def validate(settings):calls.append(settings);settings['nested']['thresholds'].append(3)
        item=AnalysisModule(ModuleSpec('external.ht',settings_schema=schema),lambda analysis,settings:None,
                            settings=source,validate_settings=validate)
        source['nested']['thresholds'].append(100);schema['properties'].clear()
        described=item.describe().as_dict()['details'];json.dumps(described)
        self.assertEqual(described['settings'],{'nested':{'thresholds':[1,2]}})
        self.assertEqual(described['settings_schema']['properties'],{'nested':{'type':'object'}})
        described['settings']['nested']['thresholds'].append(200)
        self.assertEqual(item.describe().as_dict()['details']['settings']['nested']['thresholds'],[1,2])
        self.assertEqual(len(calls),1)
        with self.assertRaises(TypeError):item.settings['nested']['thresholds']=[]
        with self.assertRaisesRegex(TypeError,'finite JSON'):AnalysisModule(ModuleSpec('bad'),lambda a,s:None,{'bad':float('nan')})

    def test_batch_preflight_rejects_all_declared_conflicts_before_any_builder(self):
        cases=[(module('same'),module('same')),
               (module('a',produces=('out',)),module('b',produces=('out',))),
               (module('a',resources=('correction.jet',)),module('b',resources=('correction.jet',))),
               (module('a',actions=(ActionClaim('count','selected'),)),module('b',actions=(ActionClaim('histogram','selected'),))),
               (module('a',actions=(ActionClaim('sum','total'),)),module('b',actions=(ActionClaim('sum','total'),))),
               (module('a',actions=(ActionClaim('histogram','plots'),)),module('b',actions=(ActionClaim('histogram','plots/pt'),))),
               (module('a',actions=(ActionClaim('histogram','plots/pt'),)),module('b',actions=(ActionClaim('histogram','plots'),)))]
        for items in cases:
            with self.subTest(items=[item.spec.name for item in items]):
                analysis=Inventory();calls=[]
                first=AnalysisModule(items[0].spec,lambda a,s:calls.append('called'))
                with self.assertRaises(ValueError):ModuleSequence((first,items[1])).apply(analysis)
                self.assertEqual(calls,[]);self.assertEqual(analysis._modules,{})
                self.assertIsNone(analysis._module_failure)

    def test_supplied_order_dependencies_and_future_columns_are_never_hoisted(self):
        analysis=Inventory();calls=[]
        def produce(a,s):calls.append('define/filter');a.fields.add('out')
        def consume(a,s):calls.append('downstream define/filter')
        first=AnalysisModule(ModuleSpec('first',requires=('pt',),produces=('out',)),produce)
        second=AnalysisModule(ModuleSpec('second',requires=('out',),dependencies=('first',)),consume)
        sequence=ModuleSequence((first,second));sequence.preview(analysis)
        self.assertFalse(calls);self.assertEqual(analysis.fields,{'pt','cut'})
        self.assertEqual(analysis.queries,['pt','out'])
        sequence.apply(analysis);self.assertEqual(calls,['define/filter','downstream define/filter'])
        fresh=Inventory()
        with self.assertRaisesRegex(ValueError,'dependencies must precede'):fresh.use(second,first)
        self.assertEqual(fresh._modules,{})

    def test_deferred_native_outputs_reserve_names_without_becoming_setup_inputs(self):
        deferred=module('native',produces=('future',),outputs_at='native-lifecycle',implementation='native-manager',
                        actions=(ActionClaim('histogram','native/pt'),))
        analysis=Inventory();analysis.use(deferred)
        self.assertNotIn('future',analysis.fields)
        with self.assertRaisesRegex(ValueError,'unavailable during setup'):analysis.use(module('consumer',requires=('future',)))
        with self.assertRaisesRegex(ValueError,'reserved'):analysis.use(module('overwrite',produces=('future',)))
        with self.assertRaisesRegex(ValueError,'path collision'):
            analysis.use(module('path',actions=(ActionClaim('histogram','native/pt/sub'),)))

    def test_missing_claimed_output_or_action_poison_run_after_setup_started(self):
        for claim in (dict(produces=('out',)),dict(actions=(ActionClaim('count','out'),))):
            analysis=Inventory()
            with self.assertRaisesRegex(RuntimeError,'advertised'):analysis.use(module('bad',**claim))
            with self.assertRaisesRegex(RuntimeError,'fresh Analysis'):analysis.run()
            self.assertFalse(analysis.events)
            with self.assertRaisesRegex(RuntimeError,'Previous module'):analysis.use(module('later'))

    def test_builder_cannot_run_events_or_nest_module_application(self):
        for builder in (lambda a,s:a.run(),lambda a,s:a.use(module('nested'))):
            analysis=Inventory()
            with self.assertRaisesRegex(RuntimeError,'Module .*failed'):analysis.use(AnalysisModule(ModuleSpec('bad'),builder))
            self.assertFalse(analysis.events);self.assertIsNone(analysis._module_active)
        analysis=Inventory();analysis.native.event_loops=1
        with self.assertRaisesRegex(RuntimeError,'before event execution'):analysis.use(module('late'))

    def test_reuse_does_not_mutate_settings_or_retain_setup_builder_in_analysis(self):
        class Builder:
            def __call__(self,a,s):s['nested'].append(2)
        builder=Builder();owner=weakref.ref(builder)
        item=AnalysisModule(ModuleSpec('reuse'),builder,{'nested':[1]})
        first=Inventory();second=Inventory();first.use(item);second.use(item)
        self.assertEqual(first._modules,second._modules)
        self.assertEqual(first._modules['reuse']['settings'],{'nested':[1]})
        del item,builder;gc.collect();self.assertIsNone(owner())

    def test_external_service_discovery_is_explicit_stdlib_only_and_disableable(self):
        provider=REPO/'examples/authoring-extension'
        env=dict(os.environ,PYTHONPATH=str(REPO/'core/python'),RDF_EXTENSION_PATHS=str(provider),
                 RDF_BUILD_EXTENSIONS='ON',RDF_ENABLE_EXAMPLE_AUTHORING='ON')
        code="""
import json,sys
from rdfanalysis import discover_module_specs
assert 'rdfexample_authoring' not in sys.modules
specs=discover_module_specs()
print(json.dumps([spec.describe().as_dict()['details']['name'] for spec in specs]))
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
"""
        enabled=subprocess.run([sys.executable,'-S','-c',code],env=env,capture_output=True,text=True,check=True)
        self.assertEqual(json.loads(enabled.stdout),['example.ht'])
        disabled=subprocess.run([sys.executable,'-S','-c',code],env={**env,'RDF_ENABLE_EXAMPLE_AUTHORING':'OFF'},
                                capture_output=True,text=True,check=True)
        self.assertEqual(json.loads(disabled.stdout),[])

    def test_enabled_provider_errors_are_not_silently_treated_as_absence(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);package=directory/'python/rdf_broken';package.mkdir(parents=True)
            (directory/'rdf-extension.json').write_text(json.dumps(dict(name='broken',core_api_version=1,
                python_package='rdf_broken',python_services={'authoring_modules':'provider'})))
            (package/'__init__.py').write_text('')
            (package/'provider.py').write_text("def module_specs():\n    raise RuntimeError('broken provider')\n")
            env=dict(os.environ,PYTHONPATH=str(REPO/'core/python'),RDF_EXTENSION_PATHS=str(directory),RDF_BUILD_EXTENSIONS='ON')
            result=subprocess.run([sys.executable,'-S','-c','from rdfmodules import discover_module_specs; discover_module_specs()'],
                                  env=env,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0);self.assertIn('broken provider',result.stderr)

    @unittest.skipUnless(shutil.which('cmake'),'CMake unavailable')
    def test_external_provider_uses_the_existing_cmake_extension_registry(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'source';source.mkdir()
            (source/'CMakeLists.txt').write_text(f'''cmake_minimum_required(VERSION 3.16)
project(ModuleProvider NONE)
include("{REPO}/cmake/SetupExtensions.cmake")
rdf_discover_extensions()
rdf_add_extensions()
if(NOT TARGET ExampleAuthoring)
    message(FATAL_ERROR "provider target missing")
endif()
''')
            subprocess.run(['cmake','-S',str(source),'-B',str(directory/'build'),
                            '-DRDF_EXTENSION_PATHS='+str(REPO/'examples/authoring-extension'),
                            '-DRDF_REQUIRED_EXTENSIONS=example_authoring'],check=True,capture_output=True,text=True)


if __name__=='__main__':unittest.main()
