"""Compiler-free real component routing/notice contracts for shared ONNX payloads."""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO=Path(__file__).resolve().parents[3]


@unittest.skipUnless(shutil.which('cmake'),'CMake unavailable')
class OnnxRuntimeInstall(unittest.TestCase):
    def configure(self,root,notices=True):
        runtime=root/'runtime';(runtime/'lib').mkdir(parents=True)
        (runtime/'lib/libonnxruntime.so.1').write_bytes(b'file routing fixture; not an ELF runtime')
        (runtime/'lib/libonnxruntime.so').symlink_to('libonnxruntime.so.1')
        if notices:
            (runtime/'LICENSE').write_text('fixture license')
            (runtime/'ThirdPartyNotices.txt').write_text('fixture notices')
        source=root/'source';source.mkdir()
        (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.19)\nproject(ComponentRouting LANGUAGES NONE)\n'
            f'set(ONNXRUNTIME_ROOT_DIR "{runtime}")\nset(ONNXRUNTIME_LIB_DIR "{runtime}/lib")\n'
            f'include("{REPO}/cmake/InstallOnnxRuntime.cmake")\n'
            'rdf_install_onnx_runtime(COMPONENT Python)\nrdf_install_onnx_runtime(COMPONENT AnalyzerSDK REQUIRED_NOTICES)\n')
        subprocess.run(['cmake','-S',str(source),'-B',str(root/'build')],check=True,capture_output=True,text=True,timeout=30)

    def install(self,root,component,check=True):
        return subprocess.run(['cmake','--install',str(root/'build'),'--prefix',str(root/'prefix'),'--component',component],
            check=check,capture_output=True,text=True,timeout=30)

    def test_independent_components_share_one_prefix_relative_runtime(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);self.configure(root)
            self.install(root,'AnalyzerSDK')
            before=(root/'prefix/lib/libonnxruntime.so.1').read_bytes()
            self.install(root,'Python')
            self.assertEqual((root/'prefix/lib/libonnxruntime.so.1').read_bytes(),before)
            self.assertEqual(list((root/'prefix').rglob('libonnxruntime.so.1')),[root/'prefix/lib/libonnxruntime.so.1'])
            self.assertTrue((root/'prefix/lib/libonnxruntime.so').is_symlink())
            self.assertEqual((root/'prefix/share/rdfanalyzer/onnxruntime/LICENSE').read_text(),'fixture license')
            self.assertNotIn('CMAKE_CXX_COMPILER:',(root/'build/CMakeCache.txt').read_text())

    def test_build_local_link_stage_preserves_selected_and_provider_files(self):
        # Metadata-only configure: fixture files are never loaded as libraries.
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);runtime=root/'runtime';(runtime/'lib').mkdir(parents=True);(runtime/'include').mkdir()
            for name in ('libonnxruntime.so','libonnxruntime_providers_shared.so','libprovider.dylib'):
                (runtime/'lib'/name).write_bytes(b'file routing fixture; not an ELF runtime')
            source=root/'source';source.mkdir()
            (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.19)\nproject(LinkStage LANGUAGES NONE)\n'
                f'set(ONNXRUNTIME_ROOT_DIR "{runtime}")\ninclude("{REPO}/cmake/SetupOnnxRuntime.cmake")\n'
                'get_target_property(location OnnxRuntime::OnnxRuntime IMPORTED_LOCATION)\n'
                'file(WRITE "${CMAKE_BINARY_DIR}/location.txt" "${location}")\n')
            command=['cmake','-S',str(source),'-B',str(root/'build')]
            for repeat in range(2):
                subprocess.run(command,check=True,capture_output=True,text=True,timeout=30)
                location=Path((root/'build/location.txt').read_text())
                self.assertTrue(location.is_relative_to(root/'build'))
                self.assertEqual(location.resolve(),runtime/'lib/libonnxruntime.so')
                for name in ('libonnxruntime_providers_shared.so','libprovider.dylib'):
                    self.assertEqual((location.parent/name).resolve(),runtime/'lib'/name)
            self.assertNotIn('CMAKE_CXX_COMPILER:',(root/'build/CMakeCache.txt').read_text())

    def test_notice_policy_preserves_python_compatibility_and_sdk_requirement(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);self.configure(root,notices=False)
            self.install(root,'Python')
            failed=self.install(root,'AnalyzerSDK',check=False)
            self.assertNotEqual(failed.returncode,0)
            self.assertIn('LICENSE',failed.stderr)
            self.assertFalse((root/'prefix/share/rdfanalyzer/onnxruntime/LICENSE').exists())


if __name__=='__main__':unittest.main()
