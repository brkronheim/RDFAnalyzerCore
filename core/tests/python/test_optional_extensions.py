"""Build non-CMS extensions through the public registration contract."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]


def extension(source, name, api=1):
    source.mkdir(parents=True)
    (source / "rdf-extension.json").write_text(json.dumps({
        "name": name, "core_api_version": api, "configure": "Configure.cmake",
        "enable_variable": f"DEMO_{name.upper()}",
        "python_package": f"demo_{name}", "python_services": {f"service_{name}": "provider"},
        "environment": "env.sh",
    }))
    (source / "Configure.cmake").write_text(f'''
option(DEMO_{name.upper()} "Enable {name}" ON)
rdf_register_extension(NAME {name} ENABLED "${{DEMO_{name.upper()}}}" TARGET {name}
    PYTHON_PLUGIN_HEADER {name}.h PYTHON_PLUGIN_FACTORY {name}Factory
    PYTHON_CAPABILITY has_{name} TEST_DIRECTORY tests)
''')
    (source / "CMakeLists.txt").write_text(f'''
if(NOT RDF_EXTENSION_NAME STREQUAL "{name}" OR NOT RDF_EXTENSION_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    message(FATAL_ERROR "Incorrect per-extension build context")
endif()
add_library({name} STATIC {name}.cc)
target_include_directories({name} PUBLIC "${{CMAKE_CURRENT_SOURCE_DIR}}")
target_compile_definitions({name} PUBLIC HAVE_{name.upper()})
''')
    (source / f"{name}.cc").write_text(f"int {name}Value() {{ return 7; }}\n")
    (source / f"{name}.h").write_text(f'''#pragma once
#include <string>
class Analyzer;
inline bool {name}Factory(Analyzer &, const std::string &, const std::string &type) {{ return type == "{name}"; }}
''')
    (source / "tests").mkdir()
    (source / "tests/CMakeLists.txt").write_text(f'add_test(NAME {name}_test COMMAND ${{CMAKE_COMMAND}} -E true)\n')
    package = source / "python" / f"demo_{name}"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "provider.py").write_text(f'NAME = "{name}"\n')
    (source / "env.sh").write_text(f'export DEMO_{name.upper()}_ACTIVE=1\n')
    return source


def project(tmp_path):
    source = tmp_path / "project"
    source.mkdir()
    (source / "CMakeLists.txt").write_text(f'''
cmake_minimum_required(VERSION 3.19)
project(ExtensionContract LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
include("{ROOT}/cmake/SetupExtensions.cmake")
rdf_discover_extensions()
rdf_generate_extension_config()
add_library(core INTERFACE)
add_library(corePlugins INTERFACE)
rdf_add_extensions()
get_property(_targets GLOBAL PROPERTY RDF_EXTENSION_TARGETS)
add_library(coreAll INTERFACE)
target_link_libraries(coreAll INTERFACE ${{_targets}})
add_executable(probe probe.cc)
target_include_directories(probe PRIVATE "${{CMAKE_BINARY_DIR}}/include")
target_link_libraries(probe PRIVATE coreAll)
enable_testing()
rdf_add_extension_tests()
''')
    (source / "probe.cc").write_text('''
#include <ExtensionConfig.h>
#include <ExtensionPythonPlugins.h>
#include <iostream>
class Analyzer {};
#ifdef HAVE_ALPHA
int alphaValue();
#endif
#ifdef HAVE_BETA
int betaValue();
#endif
int main() {
  Analyzer analyzer;
  for (const auto &extension : rdf::extensionBuildInfo) {
    if (rdf::addExtensionPythonPlugin(analyzer, "", extension.name) != extension.enabled) return 1;
    std::cout << extension.name << ":" << extension.enabled << "\\n";
  }
#ifdef HAVE_ALPHA
  if (alphaValue() != 7) return 2;
#endif
#ifdef HAVE_BETA
  if (betaValue() != 7) return 3;
#endif
  return rdf::addExtensionPythonPlugin(analyzer, "", "__unknown__") ? 4 : 0;
}
''')
    return source


def run(*args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, **kwargs)


def configure(source, *settings):
    return run("cmake", "-S", str(source), "-B", str(source / "build"), *settings)


def build_and_probe(source):
    result = run("cmake", "--build", str(source / "build"), "-j", "2")
    assert result.returncode == 0, result.stdout + result.stderr
    result = run(str(source / "build/probe"))
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_multiple_non_cms_extensions_link_and_register(tmp_path):
    source = project(tmp_path)
    extension(source / "extensions/alpha", "alpha")
    extension(source / "extensions/beta", "beta")
    result = configure(source)
    assert result.returncode == 0, result.stderr
    assert set(build_and_probe(source).splitlines()) == {"alpha:1", "beta:1"}
    result = run("ctest", "--test-dir", str(source / "build"), "--output-on-failure")
    assert result.returncode == 0, result.stdout
    assert "0 tests failed out of 2" in result.stdout
    assert "RDF_ENABLE_CMS" not in (source / "build/CMakeCache.txt").read_text()


def test_optional_python_configurator_dispatch_and_disabled_errors(tmp_path):
    source=project(tmp_path)
    alpha=extension(source/'extensions/alpha','alpha')
    config=alpha/'Configure.cmake'
    config.write_text(config.read_text().replace('PYTHON_CAPABILITY',
        'PYTHON_ANALYZER_CONFIGURATOR alphaConfigure PYTHON_CAPABILITY'))
    header=alpha/'alpha.h'
    header.write_text(header.read_text()+'''
#include <unordered_map>
inline std::unordered_map<std::string,std::string> alphaConfigure(Analyzer &,
    const std::unordered_map<std::string,std::string>& settings) {return settings;}
''')
    probe=source/'probe.cc'
    extra='''
#ifdef HAVE_ALPHA
  if (rdf::configureExtensionPythonAnalyzer(analyzer,"alpha",{{"threshold","42"}}).at("threshold")!="42") return 5;
#else
  try {rdf::configureExtensionPythonAnalyzer(analyzer,"alpha",{});return 6;}
  catch (const std::invalid_argument&) {}
#endif
  try {rdf::configureExtensionPythonAnalyzer(analyzer,"unknown",{});return 7;}
  catch (const std::invalid_argument&) {}
'''
    probe.write_text(probe.read_text().replace('  return rdf::addExtensionPythonPlugin',extra+'  return rdf::addExtensionPythonPlugin'))
    for flag in ('ON','OFF'):
        result=configure(source,'-DDEMO_ALPHA='+flag)
        assert result.returncode==0,result.stderr
        assert build_and_probe(source).strip()==('alpha:1' if flag=='ON' else 'alpha:0')


def test_extension_owns_its_enable_flag(tmp_path):
    source = project(tmp_path)
    extension(source / "extensions/alpha", "alpha")
    extension(source / "extensions/beta", "beta")
    result = configure(source, "-DDEMO_ALPHA=OFF")
    assert result.returncode == 0, result.stderr
    assert set(build_and_probe(source).splitlines()) == {"alpha:0", "beta:1"}
    assert not (source / "build/extensions/alpha").exists()


def test_absent_extensions_define_no_options(tmp_path):
    source = project(tmp_path)
    result = configure(source)
    assert result.returncode == 0, result.stderr
    assert build_and_probe(source) == ""
    cache = (source / "build/CMakeCache.txt").read_text()
    assert "DEMO_ALPHA" not in cache and "RDF_ENABLE_CMS" not in cache
    result = configure(source, "-DRDF_REQUIRED_EXTENSIONS=alpha")
    assert result.returncode != 0 and "Required extension alpha" in result.stderr


def test_master_disable_does_not_load_extension_options(tmp_path):
    source = project(tmp_path)
    extension(source / "extensions/alpha", "alpha")
    result = configure(source, "-DRDF_BUILD_EXTENSIONS=OFF")
    assert result.returncode == 0, result.stderr
    assert build_and_probe(source) == ""
    assert "DEMO_ALPHA" not in (source / "build/CMakeCache.txt").read_text()


def test_incompatible_extension_is_an_error(tmp_path):
    source = project(tmp_path)
    extension(source / "extensions/alpha", "alpha", api=999)
    result = configure(source)
    assert result.returncode != 0 and "extension API 999" in result.stderr


def test_external_checkout_overrides_default(tmp_path):
    source = project(tmp_path)
    extension(source / "extensions/alpha", "alpha")
    external = extension(tmp_path / "external", "alpha")
    result = configure(source, f"-DRDF_EXTENSION_PATHS={external}")
    assert result.returncode == 0, result.stderr
    assert "Using explicit checkout" in result.stdout
    assert build_and_probe(source) == "alpha:1\n"


def test_duplicate_explicit_checkouts_fail(tmp_path):
    source = project(tmp_path)
    first = extension(tmp_path / "first", "alpha")
    second = extension(tmp_path / "second", "alpha")
    result = configure(source, f"-DRDF_EXTENSION_PATHS={first};{second}")
    assert result.returncode != 0 and "Duplicate extension alpha" in result.stderr


def test_duplicate_discovered_checkouts_fail(tmp_path):
    source = project(tmp_path)
    extension(source / "extensions/first", "alpha")
    extension(source / "extensions/second", "alpha")
    result = configure(source)
    assert result.returncode != 0 and "Duplicate extension alpha" in result.stderr


def test_analysis_dependencies_use_extension_ids(tmp_path):
    source = project(tmp_path)
    analyses = source / "analyses"
    analyses.mkdir()
    (analyses / "CMakeLists.txt").write_text((ROOT / "analyses/CMakeLists.txt").read_text())
    sample = analyses / "sample"
    sample.mkdir()
    (sample / "rdf-dependencies.cmake").write_text("set(RDF_ANALYSIS_REQUIRES alpha)\n")
    (sample / "CMakeLists.txt").write_text("add_library(sample INTERFACE)\n")
    with (source / "CMakeLists.txt").open("a") as output:
        output.write("add_subdirectory(analyses)\n")
    result = configure(source)
    assert result.returncode == 0 and "Skipping analysis sample" in result.stdout
    result = configure(source, "-DRDF_ANALYSES=sample")
    assert result.returncode != 0 and "requires missing or disabled extensions: alpha" in result.stderr
    extension(source / "extensions/alpha", "alpha")
    result = configure(source, "-DRDF_ANALYSES=sample")
    assert result.returncode == 0, result.stderr


def test_python_services_and_environment_use_the_same_manifest(tmp_path):
    source = extension(tmp_path / "alpha", "alpha")
    env = dict(os.environ, RDF_EXTENSION_PATHS=str(source), RDF_BUILD_EXTENSIONS="ON",
               PYTHONPATH=str(ROOT / "core/python"))
    code = '''
from optional_extensions import load_extension_service, environment_scripts
assert load_extension_service('service_alpha').NAME == 'alpha'
assert any(p.name == 'env.sh' for p in environment_scripts())
'''
    result = run(sys.executable, "-c", code, env=env)
    assert result.returncode == 0, result.stderr
    env["DEMO_ALPHA"] = "OFF"
    result = run(sys.executable, "-c", "from optional_extensions import load_extension_service; assert load_extension_service('service_alpha') is None", env=env)
    assert result.returncode == 0, result.stderr


def test_core_imports_without_extensions(tmp_path):
    env = dict(os.environ, RDF_BUILD_EXTENSIONS="OFF", RDF_EXTENSION_PATHS="",
               PYTHONPATH=os.pathsep.join([str(ROOT / "core/python"), str(ROOT / "core/python/law"), os.environ.get("PYTHONPATH", "")]))
    result = run(sys.executable, "-c", '''
import io_policy, submission_backend, validate_config
assert io_policy.GENERIC_REDIRECTOR_HOSTS == set()
assert io_policy.SITE_DOMAINS == {}
assert 'GoldenJsonManager' not in validate_config._KNOWN_PLUGINS
try:
    from rucio_discovery import query_rucio
except ImportError as error:
    assert 'disabled' in str(error)
else:
    raise AssertionError('CMS discovery imported while disabled')
''', env=env)
    assert result.returncode == 0, result.stderr


def test_download_preserves_existing_directories(tmp_path):
    destination = tmp_path / "existing"
    destination.mkdir()
    (destination / "user-work.txt").write_text("preserve me")
    result = run(sys.executable, str(ROOT / "scripts/fetch_cms.py"), "--destination", str(destination))
    assert result.returncode != 0
    assert "not a separate repository" in result.stderr, result.stderr
    assert (destination / "user-work.txt").read_text() == "preserve me"


def test_worker_scripts_embed_policy_without_extension_imports():
    from submission_backend import stage_inputs_block, xrootd_optimize_block
    for block in (stage_inputs_block(), xrootd_optimize_block()):
        assert "rdfanalyzer_cms" not in block
        assert "optional_extensions" not in block
