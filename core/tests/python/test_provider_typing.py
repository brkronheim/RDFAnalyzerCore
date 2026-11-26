"""Provider/CMS signatures and systematic-inference planning, no native runtime."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from _stub_contracts import assert_stub_contract

REPO = Path(__file__).resolve().parents[3]
CMS = REPO/'extensions/cms'
HAS_CMS = (CMS/'cmake/InstallPython.cmake').is_file()


class ProviderSignatureTests(unittest.TestCase):
    def test_provider_and_module_signature_contracts(self):
        for name in ('rdfmodules', 'rdfvalidation'):
            with self.subTest(module=name): assert_stub_contract(self, name)

    @unittest.skipUnless(HAS_CMS, 'optional CMS checkout')
    def test_extension_owned_signature_contract(self):
        sys.path.insert(0, str(CMS/'python'))
        try:
            assert_stub_contract(self, 'rdfanalyzer_cms.analysis')
        finally:
            sys.path.remove(str(CMS/'python'))


@unittest.skipUnless(shutil.which('cmake'), 'language-free source install')
class InstalledProviderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='rdf provider signatures ')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.directory = Path(cls.temporary.name)
        cls.prefixes = {}
        for cms in ((False, True) if HAS_CMS else (False,)):
            source = cls.directory/('cms' if cms else 'core'); source.mkdir()
            document = ('cmake_minimum_required(VERSION 3.19)\nproject(ProviderFiles LANGUAGES NONE)\n'
                'include("'+(REPO/'cmake/InstallPythonAuthoring.cmake').as_posix()+'")\n'
                'rdf_install_python_authoring(COMPONENT Python)\n')
            if cms:
                document += 'include("'+(CMS/'cmake/InstallPython.cmake').as_posix()+'")\nrdf_install_cms_python()\n'
            (source/'CMakeLists.txt').write_text(document)
            build = source/'build'; prefix = source/'prefix'; cls.prefixes[cms] = prefix
            env = dict(os.environ, CC='/missing-compiler', CXX='/missing-compiler')
            subprocess.run(['cmake', '-S', str(source), '-B', str(build)], env=env,
                capture_output=True, text=True, check=True)
            subprocess.run(['cmake', '--install', str(build), '--prefix', str(prefix), '--component', 'Python'],
                env=env, capture_output=True, text=True, check=True)

    def child(self, code, *, cms=False):
        result = subprocess.run([sys.executable, '-S', '-c', code], cwd=self.directory,
            env=dict(os.environ, PYTHONPATH=str(self.prefixes[cms]/'python')),
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_core_provider_install_remains_independent_of_cms(self):
        for name in ('rdfmodules', 'rdfvalidation'):
            self.assertEqual((self.prefixes[False]/'python'/(name+'.pyi')).read_bytes(),
                (REPO/'core/python'/(name+'.pyi')).read_bytes())
        self.child('''
import importlib.util,json,sys
from rdfmodules import AnalysisModule,ModuleSpec
from rdfvalidation import SettingsSpec
settings=SettingsSpec('custom',{},lambda values:None)
assert settings.validate({}).passed
module=AnalysisModule(ModuleSpec('custom'),lambda analysis,values:None,{},validate_settings=lambda values:None)
json.dumps(module.describe().as_dict())
assert importlib.util.find_spec('rdfanalyzer_cms') is None
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
''')

    @unittest.skipUnless(HAS_CMS, 'optional CMS checkout')
    def test_installed_cms_recipe_is_typed_and_configuration_only(self):
        package = self.prefixes[True]/'python/rdfanalyzer_cms'
        for name in ('analysis.pyi', '__init__.pyi', 'py.typed'):
            self.assertEqual((package/name).read_bytes(), (CMS/'python/rdfanalyzer_cms'/name).read_bytes())
        self.child('''
import json,sys
from pathlib import Path
from unittest.mock import patch
import rdfanalyzer_cms
from rdfanalyzer_cms import CmsCorrections
assert Path(rdfanalyzer_cms.__file__).parent.parent.resolve()==Path(sys.path[1]).resolve()
with patch.object(Path,'is_file',side_effect=AssertionError('configuration must not inspect assets')):
 recipe=CmsCorrections('22pre',jes='total',registry='/not-inspected.yaml')
 assert recipe.validate().passed
 json.dumps(recipe.schema())
 module=recipe.as_module()
 assert module.validate().passed
 assert module.spec.resources==('cms.object_corrections',)
 assert module.spec.implementation=='native-manager'
try:CmsCorrections.from_mapping({'era':'22pre','jes':'typo'})
except ValueError:pass
else:raise AssertionError('invalid choice passed')
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
''', cms=True)

    def test_default_inference_description_is_installed_and_imports_no_runtime(self):
        path = self.prefixes[False]/'share/rdfanalyzer/examples/systematics-inference/analysis.py'
        self.child('''
import contextlib,io,json,runpy,sys
from pathlib import Path
functions=runpy.run_path(EXAMPLE);output=io.StringIO()
with contextlib.redirect_stdout(output):assert functions['main']([])==0
document=json.loads(output.getvalue())
assert document['contract']['expected_counts']=={'input':3,'selected_union':3}
assert not Path('systematics.root').exists()
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''.replace('EXAMPLE', repr(str(path))))

    def test_graph_uses_real_native_manager_and_independent_variation_slices(self):
        path = self.prefixes[False]/'share/rdfanalyzer/examples/systematics-inference/analysis.py'
        self.child('''
import runpy,sys
functions=runpy.run_path(EXAMPLE)
class Recorder:
 def __init__(self):self.calls=[];self.native=self
 def __getattr__(self,name):
  assert name!='run','builder must not execute'
  def record(*args,**kwargs):self.calls.append((name,args,kwargs));return self
  return record
a=Recorder();assert functions['build'](a) is a
names=[name for name,args,kwargs in a.calls]
assert names.index('register_variation_pair')<names.index('weights')<names.index('filter')<names.index('applyAllBDTs')
assert a.calls[names.index('AddPlugin')][1]==('bdt','BDTManager')
features=[args for name,args,kwargs in a.calls if name=='define_numba' and args[0]=='ml_feature']
assert features[0][1] is functions['feature']
assert names.index('filter')<next(i for i,(name,args,kwargs) in enumerate(a.calls) if name=='define_numba' and args[0]=='ml_feature')
weight=[kwargs['variations'] for name,args,kwargs in a.calls if name=='weights'][0]
assert weight=={'weight':('generator','weight_up','weight_down')}
hist=[(args,kwargs) for name,args,kwargs in a.calls if name=='histogram']
assert len(hist)==10
assert {kwargs['variation'] for args,kwargs in hist}==set(functions['LABELS'])
assert len([1 for args,kwargs in hist if kwargs.get('weight')=='event_weight'])==5
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''.replace('EXAMPLE', repr(str(path))))

    def test_signed_weight_oracle_and_shift_migrations(self):
        path = self.prefixes[False]/'share/rdfanalyzer/examples/systematics-inference/analysis.py'
        self.child('''
import runpy
functions=runpy.run_path(EXAMPLE);oracle=functions['contract']()
for label,counts,weights,variances in zip(functions['LABELS'],oracle['per_label_selected'],
 oracle['score_sumw_inner'],oracle['score_sumw2_inner']):
 observed=[0.,0.];observed2=[0.,0.];accepted=0
 for x,w in zip((.45,.55,.85),(1.,-2.,3.)):
  value=functions['shift_up'](x) if label=='shapeUp' else functions['shift_down'](x) if label=='shapeDown' else x
  if value<=.5:continue
  accepted+=1
  scale=2 if label=='weightUp' else .5 if label=='weightDown' else 1
  index=int(value>=.6);observed[index]+=scale*w;observed2[index]+=(scale*w)**2
 assert accepted==counts and observed==weights and observed2==variances
'''.replace('EXAMPLE', repr(str(path))))


if __name__ == '__main__': unittest.main()
