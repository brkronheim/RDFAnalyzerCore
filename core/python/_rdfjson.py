"""Private G19 candidate: immutable finite JSON snapshots, never event data.

The caller supplies its established diagnostics and key-subclass policy. This
module has no I/O, native imports, discovery, or serialization side effects.
"""
from collections.abc import Mapping
import math
from types import MappingProxyType


def freeze(value, *, exact_keys, key_error, value_error):
    def visit(item):
        if isinstance(item, Mapping):
            if any((type(key) is not str if exact_keys else not isinstance(key, str))
                   for key in item):
                raise TypeError(key_error)
            return MappingProxyType({key: visit(child) for key, child in item.items()})
        if isinstance(item, (list, tuple)):
            return tuple(visit(child) for child in item)
        if item is None or type(item) in (str, int, bool):
            return item
        if type(item) is float and math.isfinite(item):
            return item
        raise TypeError(value_error)
    return visit(value)


def plain(value):
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [plain(item) for item in value]
    return value
