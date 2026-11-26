"""Real build/source/library identity contracts for optional compiled manifests."""
from collections import Counter
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import rdfworkflow
from rdfworkflow import CMakeBuildSpec,CppAnalysisSpec,ExecutionError
from run_bundle import content_digest,derived_artifact,file_digest,verify_artifact


class BuildIdentityTests(unittest.TestCase):
    def test_configuration_is_stdlib_only_and_does_not_read_declared_files(self):
        code='''
import sys
from unittest.mock import patch
from rdfworkflow import CMakeBuildSpec,CppAnalysisSpec
with patch('rdfworkflow.file_digest',side_effect=AssertionError('unexpected read')):
    b=CMakeBuildSpec('missing','analysis',inputs={'source':'source.cc'},products={'analysis':'analysis'})
    s=CppAnalysisSpec('missing',build_spec=b,build_manifest='missing-build.json')
    assert s.validate().passed
    assert s.describe().as_dict()['details']['build']['inputs']['source'].endswith('source.cc')
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python'))
        subprocess.run([sys.executable,'-S','-c',code],env=env,check=True,capture_output=True,text=True)

    def test_manifest_settings_are_frozen_and_json_paths_have_explicit_bases(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);path=directory/'spec.json'
            path.write_text(json.dumps({'executable':'build/analysis','working_directory':'work',
                'build_manifest':'../build-log/build_manifest.json','build_spec':{
                    'build_directory':'build','target':'analysis','inputs':{'source':'../source.cc'},
                    'products':{'analysis':'analysis'}}}))
            spec=CppAnalysisSpec.from_file(path)
            self.assertEqual(spec.build_manifest,str(directory/'build-log/build_manifest.json'))
            self.assertEqual(spec.build_spec.inputs['source'],str(directory/'source.cc'))
            self.assertEqual(spec.build_spec.products['analysis'],str(directory/'build/analysis'))
            with self.assertRaises(TypeError):spec.build_spec.inputs['source']='changed'
            for fields in ({'inputs':{'source':'source.cc'}}, {'products':{'analysis':'analysis'}},
                           {'inputs':{'source':'same'},'products':{'analysis':'same'}}):
                with self.subTest(fields=fields),self.assertRaises(ValueError):CMakeBuildSpec(temporary,'analysis',**fields)
            with self.assertRaisesRegex(ValueError,'overwrite'):
                CppAnalysisSpec(sys.executable,working_directory=temporary,build_manifest='receipt.json',outputs={'bad':'receipt.json'})

    def receipt(self,directory):
        source=directory/'source.cc';source.write_text('source')
        config={'declared_inputs':{'source':{'path':str(source),'sha256':file_digest(source)}},
                'tools':{'tool':{'path':sys.executable,'sha256':file_digest(sys.executable)}},
                'products':{'analysis':str(Path(sys.executable).resolve())}}
        return derived_artifact(kind='compiled-analysis-build',parents={},configuration=config,
                                files=[Path(sys.executable).resolve()])

    def test_invalid_or_unrelated_manifests_fail_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);receipt=directory/'build.json'
            original=self.receipt(directory)
            variants=[[],{}, {**original,'artifact_id':'wrong'}]
            product=next(iter(original['files']))
            noncanonical=str(Path(product).parent)+'/../'+Path(product).parent.name+'/'+Path(product).name
            for change in ({'kind':'other'}, {'configuration':[]},
                           {'configuration':{**original['configuration'],'products':{}}},
                           {'configuration':{**original['configuration'],'declared_inputs':{}}},
                           {'configuration':{**original['configuration'],'products':{
                               **original['configuration']['products'],'extra':noncanonical}},
                            'files':{**original['files'],noncanonical:original['files'][product]}}):
                record={**original,**change};record.pop('artifact_id',None)
                variants.append({**record,'artifact_id':content_digest(record)})
            for record in variants:
                receipt.write_text(json.dumps(record))
                spec=CppAnalysisSpec(sys.executable,build_manifest=str(receipt))
                with self.subTest(record=record):
                    self.assertFalse(spec.validate('assets').passed)
                    with self.assertRaises(ValueError):spec.prepare()
            receipt.write_text(json.dumps(original))
            unrelated=directory/'other';unrelated.write_text('not this build');unrelated.chmod(0o700)
            self.assertFalse(CppAnalysisSpec(str(unrelated),build_manifest=str(receipt)).validate('assets').passed)
            # A legal library name must not hide executable membership checks.
            shadow=CppAnalysisSpec(str(unrelated),libraries={'executable':sys.executable},build_manifest=str(receipt))
            self.assertFalse(shadow.validate('assets').passed)
            with self.assertRaisesRegex(ValueError,'not a product'):shadow.prepare()
            self.assertFalse(CppAnalysisSpec(sys.executable,libraries={'other':str(unrelated)},build_manifest=str(receipt)).validate('assets').passed)

    def test_preparation_hashes_aliased_products_once_and_retained_manifest_is_checked(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);receipt=directory/'build.json';receipt.write_text(json.dumps(self.receipt(directory)))
            spec=CppAnalysisSpec(sys.executable,('-c','pass'),build_manifest=str(receipt),libraries={'alias':sys.executable})
            calls=[]
            def digest(path):calls.append(str(Path(path).resolve()));return file_digest(path)
            with patch('rdfworkflow.file_digest',side_effect=digest):prepared=spec.prepare()
            self.assertTrue(calls);self.assertTrue(all(n==1 for n in Counter(calls).values()))
            receipt.write_text(receipt.read_text()+'\n')
            with self.assertRaisesRegex(ValueError,'artifact changed'):prepared.run(directory/'should-not-launch')
            self.assertFalse((directory/'should-not-launch').exists())

    @unittest.skipUnless(shutil.which('cmake') and hasattr(os,'mkfifo'),'POSIX CMake/FIFO fixture')
    def test_nonregular_build_input_is_rejected_before_hashing_or_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);(directory/'CMakeCache.txt').write_text('configured fixture')
            pipe=directory/'input.pipe';os.mkfifo(pipe)
            builder=CMakeBuildSpec(temporary,'analysis',inputs={'source':str(pipe)},products={'analysis':'analysis'})
            def safe_digest(path):
                if Path(path)==pipe:raise AssertionError('nonregular input reached hashing')
                return file_digest(path)
            with patch('rdfworkflow.file_digest',side_effect=safe_digest),self.assertRaisesRegex(ValueError,'not a regular file'):
                builder.build(directory/'logs')
            self.assertFalse((directory/'logs').exists())

    @unittest.skipUnless(shutil.which('cmake'),'CMake fixture')
    def test_products_cannot_replace_their_manifest_or_controller_logs(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);(directory/'CMakeCache.txt').write_text('configured fixture')
            source=directory/'source.cc';source.write_text('source')
            for name in ('run.json','progress.json','stdout.log','stderr.log','build_manifest.json'):
                builder=CMakeBuildSpec(temporary,'analysis',inputs={'source':str(source)},
                    products={'product':str(directory/'logs'/name)})
                with self.subTest(name=name),self.assertRaisesRegex(ValueError,'controller-owned'):
                    builder.build(directory/'logs')
                self.assertFalse((directory/'logs').exists())

    @unittest.skipUnless(shutil.which('cmake') and shutil.which('c++') and sys.platform.startswith('linux'),'Linux CMake/C++ fixture')
    def test_linked_library_rebuild_and_source_free_prebuilt_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'source';source.mkdir();build=directory/'build'
            (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.16)\nproject(BuildIdentity LANGUAGES CXX)\nadd_library(physics SHARED physics.cc)\nadd_executable(analysis main.cc)\ntarget_link_libraries(analysis PRIVATE physics)\n')
            (source/'physics.cc').write_text('int answer(){return 42;}\n')
            (source/'main.cc').write_text('#include <iostream>\nint answer();int main(){std::cout<<answer();}\n')
            subprocess.run(['cmake','-S',str(source),'-B',str(build)],check=True,capture_output=True)
            builder=CMakeBuildSpec(str(build),'analysis',parallel=1,
                inputs={p.stem.replace('.','_'):str(p) for p in source.iterdir()},
                products={'analysis':str(build/'analysis'),'physics':str(build/'libphysics.so')})
            self.assertTrue(builder.build(directory/'build-one').success)
            first_manifest=directory/'build-one/build_manifest.json';identity=verify_artifact(first_manifest)
            def spec(manifest):return CppAnalysisSpec(str(build/'analysis'),libraries={'physics':str(build/'libphysics.so')},build_manifest=str(manifest))
            first=spec(first_manifest);self.assertTrue(first.validate('build-inputs').passed)
            prepared=first.prepare();prepared.run(directory/'first-run')
            self.assertEqual((directory/'first-run/stdout.log').read_text(),'42')
            self.assertEqual(json.loads((directory/'first-run/run.json').read_text())['artifacts']['build_identity'],identity['artifact_id'])
            (source/'physics.cc').write_text('int answer(){return 43;}\n')
            self.assertFalse(first.validate('build-inputs').passed)
            # Recorded prebuilt products are still valid; a source edit is not a rebuild.
            first.run(directory/'prebuilt-old');self.assertEqual((directory/'prebuilt-old/stdout.log').read_text(),'42')
            builder.build(directory/'build-two')
            with self.assertRaisesRegex(ValueError,'artifact changed'):prepared.run(directory/'stale-prepared')
            with self.assertRaisesRegex(ValueError,'build product changed'):first.prepare()
            second=spec(directory/'build-two/build_manifest.json')
            self.assertTrue(second.validate('build-inputs').passed)
            shutil.rmtree(source)
            self.assertFalse(second.validate('build-inputs').passed)
            second.run(directory/'without-source');self.assertEqual((directory/'without-source/stdout.log').read_text(),'43')

    @unittest.skipUnless(shutil.which('cmake'),'CMake fixture')
    def test_zero_exit_missing_products_and_mutating_builds_cannot_publish_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'source';source.mkdir();build=directory/'build'
            (source/'before.txt').write_text('before');(source/'after.txt').write_text('after')
            (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.16)\nproject(BuildMutation NONE)\nadd_custom_target(missing)\nadd_custom_target(mutate COMMAND ${CMAKE_COMMAND} -E copy "${CMAKE_CURRENT_SOURCE_DIR}/after.txt" "${CMAKE_CURRENT_SOURCE_DIR}/before.txt" COMMAND ${CMAKE_COMMAND} -E touch "${CMAKE_CURRENT_BINARY_DIR}/product")\n')
            subprocess.run(['cmake','-S',str(source),'-B',str(build)],check=True,capture_output=True)
            for target in ('missing','mutate'):
                builder=CMakeBuildSpec(str(build),target,inputs={'source':str(source/'before.txt')},products={'product':'product'})
                result=builder.build(directory/target,check=False)
                self.assertFalse(result.success);self.assertEqual(result.returncode,0)
                self.assertEqual(result.issues[0].code,'build_identity_failed')
                self.assertFalse((directory/target/'build_manifest.json').exists())
                self.assertFalse(json.loads((directory/target/'run.json').read_text())['success'])


if __name__=='__main__':unittest.main()
