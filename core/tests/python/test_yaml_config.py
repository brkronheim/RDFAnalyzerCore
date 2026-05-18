"""Tests for YAML/text config read-write functions in submission_backend."""

import os
import tempfile
import pytest

from submission_backend import (
    read_config,
    write_config,
    get_config_extension,
    read_config_text,
    read_config_yaml,
)


def test_read_text_format():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
        f.write("# This is a comment\n")
        f.write("key1=value1\n")
        f.write("key2=value2 # inline comment\n")
        f.write("key3=value with spaces\n")
    try:
        result = read_config(fname)
        assert result['key1'] == 'value1'
        assert result['key2'] == 'value2'
        assert result['key3'] == 'value with spaces'
    finally:
        os.unlink(fname)


def test_read_yaml_format():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname = f.name
        f.write("key1: value1\n")
        f.write("key2: value2\n")
        f.write("key3: value with spaces\n")
    try:
        result = read_config(fname)
        assert result['key1'] == 'value1'
        assert result['key2'] == 'value2'
        assert result['key3'] == 'value with spaces'
    finally:
        os.unlink(fname)


def test_write_text_format_roundtrip():
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
    try:
        write_config(test_data, fname)
        result = read_config(fname)
        assert result == test_data
    finally:
        os.unlink(fname)


def test_write_yaml_format_roundtrip():
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname = f.name
    try:
        write_config(test_data, fname)
        result = read_config(fname)
        assert result == test_data
    finally:
        os.unlink(fname)


def test_get_config_extension_txt():
    assert get_config_extension('config.txt') == '.txt'
    assert get_config_extension('/path/to/config.txt') == '.txt'


def test_get_config_extension_yaml():
    assert get_config_extension('config.yaml') == '.yaml'
    assert get_config_extension('/path/to/config.yml') == '.yaml'
    assert get_config_extension('config.yml') == '.yaml'


def test_text_yaml_consistency():
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname_txt = f.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname_yaml = f.name
    try:
        write_config(test_data, fname_txt)
        write_config(test_data, fname_yaml)
        assert read_config(fname_txt) == read_config(fname_yaml)
    finally:
        os.unlink(fname_txt)
        os.unlink(fname_yaml)


if __name__ == '__main__':
    import sys
    sys.exit(pytest.main([__file__, '-v']))
