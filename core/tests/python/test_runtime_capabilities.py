"""Optional native features are reported/required without constructing a graph."""
import contextlib
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import rdfanalysis


class RuntimeCapabilityTests(unittest.TestCase):
    def probe(self,methods=(),required=()):
        class Native:
            def __init__(self,*args,**kwargs):raise AssertionError('must not construct an analysis')
        for name in ('define','_define_compiled','project','book_histogram','register_variation','_configure_weights','_declare_region',*methods):
            setattr(Native,name,lambda:None)
        modules={name:SimpleNamespace(__file__='supplied/'+name,__version__='test') for name in ('yaml','numpy')}
        modules['rdfanalyzer']=SimpleNamespace(Analyzer=Native,python_authoring_api_version=4,has_cms=False)
        modules['ROOT']=SimpleNamespace(gROOT=SimpleNamespace(GetVersion=lambda:'6.40.04',GetVersionInt=lambda:64004))
        with patch('importlib.import_module',side_effect=lambda name:modules[name]):
            return rdfanalysis.diagnose_runtime(require_capabilities=required)

    def test_old_api_four_module_is_valid_without_optional_requirements(self):
        report=self.probe()
        self.assertTrue(report['passed'])
        detail=next(c['detail'] for c in report['checks'] if c['name']=='native authoring API')
        self.assertEqual(detail['optional_capabilities'],[])

    def test_missing_optional_operation_fails_only_when_explicitly_required(self):
        report=self.probe(['applyCorrections'],['correction-batch'])
        self.assertFalse(report['passed'])
        check=next(c for c in report['checks'] if c['name']=='required native capabilities')
        self.assertIn('correction-batch',check['detail']);self.assertIn('rebuild',check['detail'])
        report=self.probe(['applyCorrections','applyCorrectionsVec','book_histograms_mixed'],
                          ['correction-batch','mixed-histogram-batch','correction-batch'])
        self.assertTrue(report['passed'])
        self.assertEqual(report['required_capabilities'],['correction-batch','mixed-histogram-batch'])

    def test_bad_requests_fail_before_importing_the_native_environment(self):
        with patch('importlib.import_module',side_effect=AssertionError('must not import')):
            for value in ('correction-batch',None,[object()],['unknown']):
                with self.subTest(value=value),self.assertRaises((TypeError,ValueError)):
                    rdfanalysis.diagnose_runtime(require_capabilities=value)

    def test_cli_passes_explicit_feature_requirements_and_exit_status(self):
        for passed in (False,True):
            output=io.StringIO()
            with patch('rdfanalysis.diagnose_runtime',return_value={'passed':passed}) as probe,contextlib.redirect_stdout(output):
                result=rdfanalysis.main(['doctor','--require-capability','histogram-batch','--require-capability','correction-batch'])
            self.assertEqual(result,0 if passed else 1)
            probe.assert_called_once_with(numba=False,cms=False,require_capabilities=['histogram-batch','correction-batch'])
            self.assertEqual(json.loads(output.getvalue()),{'passed':passed})


if __name__=='__main__':unittest.main()
