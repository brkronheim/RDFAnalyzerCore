#!/usr/bin/env python3
"""
Tests for cpp_wrapper module
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent))

from cpp_wrapper import (
    find_shared_libraries,
    stage_shared_libraries,
    setup_library_path,
    run_cpp_executable
)


class TestSharedLibraryDiscovery(unittest.TestCase):
    """Test shared library discovery"""
    
    @patch('cpp_wrapper.subprocess.run')
    @patch('cpp_wrapper.os.path.exists')
    def test_find_shared_libraries_basic(self, mock_exists, mock_run):
        """Test basic shared library discovery"""
        # Mock ldd output
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = """
        linux-vdso.so.1 =>  (0x00007fff)
        libpthread.so.0 => /lib64/libpthread.so.0 (0x00007f)
        libstdc++.so.6 => /usr/lib64/libstdc++.so.6 (0x00007f)
        libm.so.6 => /lib64/libm.so.6 (0x00007f)
        libgcc_s.so.1 => /lib64/libgcc_s.so.1 (0x00007f)
        libc.so.6 => /lib64/libc.so.6 (0x00007f)
        """
        mock_run.return_value = mock_result
        mock_exists.return_value = True  # Pretend all paths exist
        
        result = find_shared_libraries("/path/to/exe")
        
        # Should find library paths
        self.assertGreater(len(result), 0)
        self.assertTrue(any('/lib64/' in lib for lib in result))
        
    @patch('cpp_wrapper.subprocess.run')
    def test_find_shared_libraries_timeout(self, mock_run):
        """Test handling of ldd timeout"""
        mock_run.side_effect = Exception("Timeout")
        
        result = find_shared_libraries("/path/to/exe")
        
        # Should return empty list on error
        self.assertEqual(result, [])
        

class TestLibraryStaging(unittest.TestCase):
    """Test library staging functionality"""
    
    def test_setup_library_path(self):
        """Test LD_LIBRARY_PATH setup"""
        with tempfile.TemporaryDirectory() as tmpdir:
            lib_dir = Path(tmpdir) / "lib"
            lib_dir.mkdir()
            
            # Setup path
            setup_library_path(str(lib_dir))
            
            # Check environment variable was set
            ld_path = os.environ.get('LD_LIBRARY_PATH', '')
            self.assertIn(str(lib_dir), ld_path)
            
    def test_setup_library_path_nonexistent(self):
        """Test handling of nonexistent library directory"""
        # Should not crash
        setup_library_path("/nonexistent/path")


class TestCppExecution(unittest.TestCase):
    """Test C++ executable execution"""
    
    @patch('cpp_wrapper.subprocess.run')
    def test_run_cpp_executable_success(self, mock_run):
        """Test successful execution"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "Success"
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cpp_executable(
                "/path/to/exe",
                "config.txt",
                tmpdir
            )
            
            self.assertTrue(result['success'])
            self.assertEqual(result['returncode'], 0)
            
    @patch('cpp_wrapper.subprocess.run')
    def test_run_cpp_executable_failure(self, mock_run):
        """Test failed execution"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Error"
        mock_run.return_value = mock_result
        
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_cpp_executable(
                "/path/to/exe",
                "config.txt",
                tmpdir
            )
            
            self.assertFalse(result['success'])
            self.assertEqual(result['returncode'], 1)


def run_tests():
    """Run all tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestSharedLibraryDiscovery))
    suite.addTests(loader.loadTestsFromTestCase(TestLibraryStaging))
    suite.addTests(loader.loadTestsFromTestCase(TestCppExecution))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
