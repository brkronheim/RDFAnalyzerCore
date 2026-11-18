"""Stdlib wrapper-classification/cache tests; no ROOT, Numba or event execution."""
import ctypes
import gc
import json
from pathlib import Path
import sys
import types
import unittest
import weakref
import warnings
from unittest import mock

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'python'))
import rdfanalysis
from rdfcolumns import KernelOutputs
from rdfdeclarations import Stage


class NativeCFunc:
    pass


class Native:
    def __init__(self):self.input_type='Float_t';self.names={'x'};self.calls=[]
    def _has_column(self,name):return name in self.names
    def column_type(self,name):return self.input_type
    def _define_compiled(self,*args):self.calls.append(args);self.names.add(args[0])


def analysis():
    result=object.__new__(rdfanalysis.Analysis);result.native=Native();result._session=None
    result._native_functions=[];result._kernel_outputs={};result._numba_bridges=[]
    result._compilation_stats=dict(numba_compile_s=0.,numba_kernels_compiled=0,numba_cache_hits=0)
    return result


class BridgeContracts(unittest.TestCase):
    def setUp(self):
        self.cache=rdfanalysis._numba_cache;self.bridges=rdfanalysis._numba_bridge_cache
        rdfanalysis._numba_cache={};rdfanalysis._numba_bridge_cache={}
        self.addCleanup(setattr,rdfanalysis,'_numba_cache',self.cache)
        self.addCleanup(setattr,rdfanalysis,'_numba_bridge_cache',self.bridges)
        self.classes=mock.patch.dict(sys.modules,{'numba.core.ccallback':types.SimpleNamespace(CFunc=NativeCFunc)})
        self.classes.start();self.addCleanup(self.classes.stop)
        self.fingerprint=mock.patch.object(rdfanalysis,'_kernel_fingerprint',return_value='fixed-source')
        self.fingerprint.start();self.addCleanup(self.fingerprint.stop)

    def root(self,bridges):
        values=list(bridges);calls=[]
        def declare(inputs,returns,name):
            def compile(function):
                calls.append((tuple(inputs),returns,name));function.__numba_cfunc__=values.pop(0)
                function.__py_wrapper__='not exposed';function.__cpp_wrapper__='not exposed'
                return function
            return compile
        patch=mock.patch.dict(sys.modules,{'ROOT':types.SimpleNamespace(Numba=types.SimpleNamespace(Declare=declare))})
        patch.start();self.addCleanup(patch.stop);return calls

    @staticmethod
    def fallback():return ctypes.CFUNCTYPE(ctypes.c_double,ctypes.c_double)(lambda value:value)

    def test_known_native_strict_and_detached_report(self):
        calls=self.root([NativeCFunc()]);a=analysis()
        self.assertIs(a.define_numba('value',lambda x:x,['x'],return_type='double',require_native=True),a)
        report=a.numba_bridges;self.assertEqual(report[0]['kind'],'native-cfunc');self.assertTrue(report[0]['definition_registered'])
        report[0]['input_types'].append('changed');report[0]['column']='changed'
        self.assertEqual(a.numba_bridges[0]['input_types'],['float']);self.assertEqual(a.numba_bridges[0]['column'],'value')
        self.assertEqual(set(a.compilation_stats),{'numba_compile_s','numba_kernels_compiled','numba_cache_hits'})
        self.assertNotIn('not exposed',json.dumps(a.numba_bridges));self.assertEqual(len(calls),1)

    def test_default_fallback_is_visible_without_an_expert_option(self):
        self.root([self.fallback()]);a=analysis()
        with self.assertWarnsRegex(RuntimeWarning,'per-event Python callback.*require_native=True'):
            a.define_numba('value',lambda x:x,['x'],return_type='double')
        self.assertEqual(len(a.native.calls),1)
        self.assertEqual(a.numba_bridges[0]['kind'],'ctypes-callback')

    def test_confirmed_native_default_is_quiet(self):
        self.root([NativeCFunc()]);a=analysis()
        with warnings.catch_warnings(record=True)as observed:
            warnings.simplefilter('always')
            a.define_numba('value',lambda x:x,['x'],return_type='double')
        self.assertEqual(observed,[])

    def test_unknown_default_is_visible_and_strict_still_rejects(self):
        self.root([types.SimpleNamespace(address=12345)]);a=analysis()
        with self.assertWarnsRegex(RuntimeWarning,'observed unknown'):
            a.define_numba('value',lambda x:x,['x'],return_type='double')
        self.assertTrue(a.numba_bridges[0]['definition_registered'])

    def test_default_fallback_preserves_existing_definition(self):
        self.root([self.fallback()]);a=analysis()
        with self.assertWarns(RuntimeWarning):a.define_numba('value',lambda x:x,['x'],return_type='double')
        self.assertEqual(a.numba_bridges[0]['kind'],'ctypes-callback');self.assertEqual(len(a.native.calls),1)

    def test_strict_fallback_rejects_before_graph_and_can_retry_default(self):
        self.root([self.fallback()]);a=analysis()
        def kernel(x):return x
        with self.assertRaisesRegex(RuntimeError,'require_native=True.*ctypes-callback'):a.define_numba('value',kernel,['x'],require_native=True)
        self.assertEqual(a.native.calls,[]);self.assertFalse(a.numba_bridges[0]['definition_registered'])
        with self.assertWarns(RuntimeWarning):a.define_numba('value',kernel,['x'])
        self.assertEqual(len(a.native.calls),1);self.assertEqual(a.compilation_stats['numba_cache_hits'],1)

    def test_unknown_address_like_object_does_not_pass_strict(self):
        self.root([types.SimpleNamespace(address=12345)]);a=analysis()
        with self.assertRaisesRegex(RuntimeError,'unknown'):a.define_numba('value',lambda x:x,['x'],require_native=True)
        self.assertEqual(a.native.calls,[])

    def test_multi_signature_cache_hit_uses_original_snapshot(self):
        native=NativeCFunc();reference=weakref.ref(native);self.root([native,self.fallback()]);a=analysis()
        def kernel(x):return x
        a.define_numba('first',kernel,['x'],require_native=True);first_name=a.native.calls[-1][1]
        a.native.input_type='Double_t'
        with self.assertWarns(RuntimeWarning):a.define_numba('second',kernel,['x'])
        del native;gc.collect();self.assertIsNotNone(reference())
        a.native.input_type='Float_t';a.define_numba('again',kernel,['x'],require_native=True)
        self.assertEqual(a.native.calls[-1][1],first_name)
        self.assertEqual([row['kind']for row in a.numba_bridges],['native-cfunc','ctypes-callback','native-cfunc'])
        self.assertEqual(a.compilation_stats['numba_kernels_compiled'],2);self.assertEqual(a.compilation_stats['numba_cache_hits'],1)

    def test_old_fallback_not_reclassified_after_native_redeclaration(self):
        self.root([self.fallback(),NativeCFunc()]);a=analysis()
        def kernel(x):return x
        with self.assertWarns(RuntimeWarning):a.define_numba('first',kernel,['x'])
        a.native.input_type='Double_t';a.define_numba('second',kernel,['x'],require_native=True)
        a.native.input_type='Float_t'
        with self.assertRaisesRegex(RuntimeError,'ctypes-callback'):a.define_numba('rejected',kernel,['x'],require_native=True)
        self.assertEqual(len(a.native.calls),2)

    def test_missing_legacy_cache_metadata_stays_unknown(self):
        self.root([NativeCFunc()]);a=analysis()
        def kernel(x):return x
        a.define_numba('first',kernel,['x']);rdfanalysis._numba_bridge_cache.clear()
        with self.assertRaisesRegex(RuntimeError,'unknown'):a.define_numba('again',kernel,['x'],require_native=True)
        self.assertEqual(len(a.native.calls),1)

    def test_invalid_boolean_fails_before_root_import(self):
        a=analysis()
        with mock.patch.dict(sys.modules,{'ROOT':None}):
            for invalid in (1,0,'true',None):
                with self.subTest(value=invalid),self.assertRaisesRegex(TypeError,'boolean'):
                    a.define_numba('value',lambda x:x,['x'],require_native=invalid)

    def test_named_outputs_forward_strict_and_failure_leaves_layout_unset(self):
        self.root([self.fallback()]);a=analysis();outputs=KernelOutputs('summary',('value',))
        with self.assertRaisesRegex(RuntimeError,'ctypes-callback'):
            a.define_numba_outputs(outputs,lambda x:x,['x'],return_type='RVec<double>',require_native=True)
        self.assertEqual(a._kernel_outputs,{});self.assertEqual(a.native.calls,[])

    def test_stage_preserves_default_description_and_validates_without_root(self):
        def kernel(x):return x
        output=KernelOutputs('summary',('value',))
        default=Stage('same').define_numba_outputs(output,kernel,['x'],return_type='RVec<double>')
        explicit_false=Stage('same').define_numba_outputs(output,kernel,['x'],return_type='RVec<double>',require_native=False)
        self.assertEqual(default.describe().as_dict(),explicit_false.describe().as_dict())
        strict=Stage('strict').define_numba_outputs(output,kernel,['x'],return_type='RVec<double>',require_native=True)
        self.assertTrue(strict.describe().as_dict()['details']['steps'][0]['kwargs']['require_native'])
        with self.assertRaisesRegex(ValueError,'boolean'):Stage('bad').define_numba('x',kernel,['x'],require_native=1)
        with self.assertRaisesRegex(ValueError,'boolean'):Stage('bad').define_numba_outputs(output,kernel,['x'],return_type='RVec<double>',require_native=1)

    def test_empty_report_has_no_runtime_dependency(self):
        self.assertEqual(object.__new__(rdfanalysis.Analysis).numba_bridges,[])

    def test_unfamiliar_diagnostic_attribute_is_unknown_not_default_failure(self):
        class Registration:
            @property
            def __numba_cfunc__(self):raise RuntimeError('unavailable wrapper metadata')
        self.assertEqual(rdfanalysis._numba_bridge_snapshot(Registration()),('unknown',None))

    def test_default_description_excludes_optional_diagnostics(self):
        a=analysis();a.config=rdfanalysis.AnalysisConfig(files=['missing.root'],output='unused.root',skim=False)
        a.native.columns=['x'];a.native.variation_labels=['Nominal'];a.native.selection_masks={}
        for name in ('_weights','_regions','_modules'):setattr(a,name,{})
        a._stages=[];a._stage_failure=None;a._module_failure=None
        before=a.describe().as_dict()
        a._numba_bridges.append(dict(column='diagnostic-only',kind='unknown'))
        self.assertEqual(a.describe().as_dict(),before)


if __name__=='__main__':unittest.main()
