"""Install and relocate the native helper closure without ROOT or a compiler."""
import os
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

REPO=Path(__file__).resolve().parents[3]


@unittest.skipUnless(shutil.which('cmake'),'CMake required')
class NativeAuthoringInstallTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary=tempfile.TemporaryDirectory(prefix='rdf native install ')
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root=Path(cls.temporary.name)
        cls.build=cls.root/'install build';cls.prefix=cls.root/'original prefix'
        cls.env=dict(os.environ,CC='/unavailable-compiler',CXX='/unavailable-compiler')
        cls.cmake=shutil.which('cmake')
        for command in ([cls.cmake,'-S',str(REPO/'core/native_authoring'),'-B',str(cls.build)],
                        [cls.cmake,'--install',str(cls.build),'--prefix',str(cls.prefix),'--component','NativeAuthoring']):
            subprocess.run(command,env=cls.env,check=True,capture_output=True,text=True)

    def test_install_has_complete_headers_and_no_runtime_or_compiler(self):
        cache=(self.build/'CMakeCache.txt').read_text()
        self.assertNotIn('CMAKE_CXX_COMPILER:',cache)
        self.assertNotIn('CMAKE_C_COMPILER:',cache)
        expected={'CommonSettings.h','CommonSettingsSchema.h','ValidationReport.h',
                  'NativePreflight.h','TagAndProbe.h','RunResult.h'}
        self.assertEqual({p.name for p in (self.prefix/'include/rdfanalyzer').iterdir()},expected)
        for name in expected:self.assertEqual((self.prefix/'include/rdfanalyzer'/name).read_bytes(),(REPO/'core/interface'/name).read_bytes())
        self.assertFalse(list(self.prefix.rglob('*.so')))
        self.assertFalse(list(self.prefix.rglob('*.a')))
        exported=(self.prefix/'lib/cmake/RDFNativeAuthoring/RDFNativeAuthoringTargets.cmake').read_text()
        self.assertNotIn(str(REPO),exported)
        self.assertIn('RDFAnalyzer::NativeAuthoring',exported)
        self.assertIn('cxx_std_17',exported)

    @unittest.skipUnless(shutil.which('g++') or shutil.which('clang++'),'C++ consumer compiler required')
    def test_relocated_package_builds_real_consumer_with_spaces(self):
        prefix=self.root/'relocated prefix';shutil.copytree(self.prefix,prefix)
        source=self.root/'consumer source';source.mkdir()
        (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.19)\nproject(NativeConsumer LANGUAGES CXX)\n'
            'find_package(RDFNativeAuthoring CONFIG REQUIRED)\nadd_executable(consumer main.cc)\n'
            'target_link_libraries(consumer PRIVATE RDFAnalyzer::NativeAuthoring)\n')
        (source/'main.cc').write_text('''#include <CommonSettings.h>
#include <NativePreflight.h>
#include <RunResult.h>
#include <iostream>
int main() {
  rdf::config::CommonSettings settings;
  settings.input.cacheEnabled = true;
  settings.input.cacheSizeMB = 32;
  if (settings.nativeOptions().at("treeCacheSizeMB") != "32") return 1;
  auto report = rdf::validation::checkSchema({{"event", "ULong64_t", "event"}}, {{"event", "ULong64_t"}});
  if (!report.passed()) return 2;
  std::cout << rdf::config::commonSettingsDescriptionJson() << '\\n';
  rdfauthoring::RunResult result("compiled-cpp", rdfauthoring::Status::Succeeded,
                               "installed C++ value serialization only");
  result.counts["input"] = 0;
  result.event_loops = 0;
  result.sums["weight"] = -2.5;
  std::cout << result.to_json() << '\\n';
}
''')
        build=self.root/'consumer build'
        for command in ([self.cmake,'-S',str(source),'-B',str(build),'-DCMAKE_PREFIX_PATH='+str(prefix),'-DCMAKE_EXPORT_COMPILE_COMMANDS=ON'],
                        [self.cmake,'--build',str(build),'--parallel','1']):
            result=subprocess.run(command,check=False,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        result=subprocess.run([str(build/'consumer')],check=True,capture_output=True,text=True)
        self.assertIn('rdf-workflow-description-v1',result.stdout)
        summary=json.loads(result.stdout.splitlines()[1])
        self.assertEqual(summary['schema'],'rdf-run-result-v1')
        self.assertEqual(summary['counts'],{'input':0})
        self.assertEqual(summary['event_loops'],0)
        self.assertEqual(summary['sums'],{'weight':-2.5})
        commands=(build/'compile_commands.json').read_text()
        self.assertNotIn(str(REPO/'core/interface'),commands)
        self.assertIn('relocated prefix/include/rdfanalyzer',commands)


if __name__=='__main__':unittest.main()
