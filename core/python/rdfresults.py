"""Detached, opt-in run summaries; standard library only, with no output I/O.

Success describes the stated completion scope. This envelope is not a framework
production-completion marker and does not certify physics or persisted outputs.
"""
from collections.abc import Mapping
from dataclasses import dataclass, field
import math
from _rdfjson import freeze as _json_freeze, plain as _json_plain
from typing import Optional


def _freeze(value):
    return _json_freeze(value, exact_keys=True, key_error='run-result keys must be strings', value_error='run results require finite JSON-compatible values, not native handles')


_plain = _json_plain


@dataclass(frozen=True)
class RunResult:
    """Immutable snapshot shared by explicit Python/session/command adapters.

    Absent summaries/identities mean unavailable, never zero or verified. Paths
    are references only: constructing/serializing a result never opens them.
    """
    backend: str
    status: str
    completion_scope: str
    counts: Mapping = field(default_factory=dict)
    histograms: Mapping = field(default_factory=dict)
    sums: Mapping = field(default_factory=dict)
    timings: Mapping = field(default_factory=dict)
    outputs: Mapping = field(default_factory=dict)
    provenance: Mapping = field(default_factory=dict)
    details: Mapping = field(default_factory=dict)
    issues: tuple = ()
    event_loops: Optional[int] = None

    def __post_init__(self):
        for name in ('backend', 'completion_scope'):
            if not isinstance(getattr(self, name), str) or not getattr(self, name).strip():
                raise ValueError(name + ' must be a nonempty string')
        if type(self.status) is not str or self.status not in ('succeeded', 'failed', 'timed_out', 'cancelled'):
            raise ValueError('unsupported run-result status: ' + str(self.status))
        if self.event_loops is not None and (type(self.event_loops) is not int or self.event_loops < 0):
            raise ValueError('event_loops must be a nonnegative integer or None')
        for name in ('counts', 'histograms', 'sums', 'timings', 'outputs', 'provenance', 'details'):
            value = getattr(self, name)
            if not isinstance(value, Mapping):
                raise TypeError(name + ' must be a mapping')
            object.__setattr__(self, name, _freeze(value))
        if not isinstance(self.issues, (list, tuple)) or any(not isinstance(item, Mapping) for item in self.issues):
            raise TypeError('issues must be a sequence of JSON mappings')
        object.__setattr__(self, 'issues', _freeze(self.issues))
        for name, value in self.counts.items():
            if type(value) is not int or value < 0:
                raise ValueError('counts.' + name + ' must be a nonnegative integer')
        for name, value in self.timings.items():
            if type(value) not in (int, float) or value < 0:
                raise ValueError('timings.' + name + ' must be nonnegative seconds')
        if any(type(path) is not str or not path for path in self.outputs.values()):
            raise ValueError('outputs must map names to nonempty path references')

    @property
    def success(self):
        return self.status == 'succeeded'

    @classmethod
    def from_dict(cls, document):
        """Validate a complete v1 envelope already acquired by its caller.

        This consumes supplied values only. It never opens paths or upgrades
        process success into physics validation. Unknown schemas/fields and a
        success flag inconsistent with status are rejected explicitly.
        """
        if not isinstance(document, Mapping):
            raise TypeError('run-result document must be a mapping')
        if any(type(key) is not str for key in document):
            raise TypeError('run-result document keys must be strings')
        if type(document.get('schema')) is not str or document['schema'] != 'rdf-run-result-v1':
            raise ValueError('unsupported run-result schema')
        fields = ('backend', 'status', 'completion_scope', 'counts', 'histograms',
                  'sums', 'timings', 'outputs', 'provenance', 'details', 'issues', 'event_loops')
        expected = set(fields) | {'schema', 'success'}
        missing, extra = expected - document.keys(), document.keys() - expected
        if missing or extra:
            raise ValueError('run-result fields differ: missing ' + repr(sorted(missing)) +
                             '; unknown ' + repr(sorted(extra)))
        if type(document['success']) is not bool:
            raise TypeError('run-result success must be a boolean')
        result = cls(**{name: document[name] for name in fields})
        if document['success'] is not result.success:
            raise ValueError('run-result success disagrees with status')
        return result

    def as_dict(self):
        return dict(schema='rdf-run-result-v1', backend=self.backend,
            status=self.status, success=self.success, completion_scope=self.completion_scope,
            **{name: _plain(getattr(self, name)) for name in
                ('counts', 'histograms', 'sums', 'timings', 'outputs', 'provenance', 'details', 'issues')},
            event_loops=self.event_loops)
