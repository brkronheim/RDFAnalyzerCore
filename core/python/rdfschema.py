"""Explicit per-file input schema checks, independent of analysis execution.

Import and supplied-metadata comparison use the standard library only. The ROOT
reader is imported and invoked only by an explicit read/check operation. This
does not resolve aliases, optional branches, friends, corrections or definitions.
"""
from collections.abc import Mapping
from dataclasses import dataclass
import os
from types import MappingProxyType

from rdfworkflow import ValidationIssue, ValidationReport


def _text(value, location, *, empty=False):
    if (not isinstance(value, str) or (not empty and not value.strip()) or
            any(char in value for char in '\0\n\r')):
        raise ValueError(f'{location}: expected a {"possibly empty" if empty else "nonempty"} string without NUL/newlines')
    return value


@dataclass(frozen=True)
class ColumnRequirement:
    name: str
    type: str = None
    location: str = ''

    def __post_init__(self):
        _text(self.name, 'ColumnRequirement.name')
        if self.type is not None:
            _text(self.type, 'ColumnRequirement.type')
        _text(self.location, 'ColumnRequirement.location', empty=True)


@dataclass(frozen=True)
class InputSchema:
    """Detached metadata for one input; type spellings belong to its reader."""
    source: str
    tree: str
    columns: object

    def __post_init__(self):
        _text(self.source, 'InputSchema.source')
        _text(self.tree, 'InputSchema.tree')
        if not isinstance(self.columns, Mapping):
            raise TypeError('InputSchema.columns must be a name-to-type mapping')
        columns = dict(self.columns)
        for name, dtype in columns.items():
            _text(name, 'InputSchema.columns.name')
            _text(dtype, f'InputSchema.columns[{name!r}]', empty=True)
        object.__setattr__(self, 'columns', MappingProxyType(columns))

    def as_dict(self):
        return dict(source=self.source, tree=self.tree, columns=dict(self.columns))


def _requirements(requirements):
    if isinstance(requirements, (str, bytes, Mapping)):
        raise TypeError('requirements must be a sequence of ColumnRequirement objects')
    requirements = tuple(requirements)
    names = set()
    for item in requirements:
        if not isinstance(item, ColumnRequirement):
            raise TypeError('requirements must contain ColumnRequirement objects')
        if item.name in names:
            raise ValueError('Duplicate required column: '+item.name)
        names.add(item.name)
    return requirements


def compare_schema(requirements, columns):
    """Compare supplied column metadata exactly; no imports, I/O or type coercion.

    A None requirement type checks presence only. An empty observed type is
    unknown, and fails a typed requirement. Extra columns are permitted.
    """
    requirements = _requirements(requirements)
    # Reuse the detached metadata value's validation, without acquiring inputs.
    columns = InputSchema('supplied metadata', 'supplied tree', columns).columns
    issues = []
    for index, item in enumerate(requirements):
        location = item.location or f'required[{index}]'
        if item.name not in columns:
            issues.append(ValidationIssue('schema.missing', 'Required column is missing: '+item.name, location))
        elif item.type is not None and item.type != columns[item.name]:
            issues.append(ValidationIssue('schema.type',
                f'Expected {item.type}, observed {columns[item.name] or "unknown type"}: {item.name}', location))
    return ValidationReport('schema', ('schema',), tuple(issues))


@dataclass(frozen=True)
class InputSchemaCheck:
    """Coverage and diagnostics, including inputs whose metadata could not open."""
    report: ValidationReport
    files: tuple
    schemas: tuple

    def __post_init__(self):
        if not isinstance(self.report, ValidationReport) or self.report.level != 'schema':
            raise TypeError('InputSchemaCheck.report must be a schema ValidationReport')
        if isinstance(self.files, (str, bytes)) or isinstance(self.schemas, (str, bytes)):
            raise TypeError('files and schemas must be sequences')
        files, schemas = tuple(self.files), tuple(self.schemas)
        if not files:
            raise ValueError('InputSchemaCheck requires at least one supplied file')
        for source in files:
            _text(source, 'InputSchemaCheck.files')
        if not all(isinstance(item, InputSchema) for item in schemas):
            raise TypeError('schemas must contain InputSchema objects')
        observed = tuple(item.source for item in schemas)
        if self.report.passed and observed != files:
            raise ValueError('A passing schema check requires metadata for every supplied file, in order')
        remaining = iter(files)
        if any(not any(source == candidate for candidate in remaining) for source in observed):
            raise ValueError('Schema observations must follow the supplied input order')
        object.__setattr__(self, 'files', files)
        object.__setattr__(self, 'schemas', schemas)

    @property
    def passed(self):
        return self.report.passed

    def as_dict(self):
        return dict(schema='rdf-input-schema-check-v1', validation=self.report.as_dict(),
            files=list(self.files), schemas=[item.as_dict() for item in self.schemas],
            scope='every supplied file, requested columns only; checker books no actions; '
                  'reader owns acquisition and event-loop verification; '
                  'no aliases, optional-column defaults, friends, graph compilation or later-read guarantee')

    def raise_if_invalid(self):
        self.report.raise_if_invalid()
        return self


def read_root_schema(source, tree, columns):
    """Open exactly one ROOT file and query requested RDF column types.

    Keeps TFile alive during all dataframe queries, and closes it on
    success/failure. Books no actions, changes no implicit MT settings and imports
    no analysis bindings. Explicit remote URLs use ROOT's configured I/O policy.
    This reader has no independent wall-clock timeout; use an isolated process
    if the storage backend requires a hard opening deadline.
    """
    source, tree = _text(source, 'source'), _text(tree, 'tree')
    if isinstance(columns, (str, bytes)):
        raise TypeError('columns must be a sequence of names')
    columns = tuple(_text(name, 'columns') for name in columns)
    import ROOT
    handle = None
    dataframe = None
    try:
        handle = ROOT.TFile.Open(source, 'READ')
        if not handle or handle.IsZombie():
            raise OSError('Cannot open ROOT input: '+source)
        obj = handle.Get(tree)
        if not obj or not obj.InheritsFrom('TTree'):
            raise OSError('Missing TTree '+tree+' in '+source)
        dataframe = ROOT.RDataFrame(obj)
        available = {str(name) for name in dataframe.GetColumnNames()}
        types = {name: str(dataframe.GetColumnType(name)) for name in columns if name in available}
        if dataframe.GetNRuns() != 0:
            # An execution invariant is a reader bug, not an unreadable input.
            # Abort immediately rather than attempting more files.
            raise AssertionError('Schema reader unexpectedly ran an event loop')
        return InputSchema(source, tree, types)
    except (RuntimeError, ValueError) as error:
        raise OSError(str(error)) from error
    finally:
        # Drop our dataframe owner before closing. An exception traceback can
        # retain a wrapper longer; no queries/actions may use it after this point.
        dataframe = None
        if handle:
            handle.Close()


def check_input_schemas(files, requirements, *, tree='Events', reader=None):
    """Check every supplied input separately, aggregating file and column errors.

    Files may come directly from an existing DatasetEntry.files or AnalysisConfig.
    No discovery, deduplication, glob expansion, manifest rewriting or caching.
    A custom reader(source, tree, names) returns InputSchema, and raises OSError
    for acquisition failures; programming/contract errors propagate.
    """
    requirements = _requirements(requirements)
    _text(tree, 'tree')
    if isinstance(files, (str, bytes, os.PathLike, Mapping)):
        raise TypeError('files must be a nonempty sequence of paths/URLs')
    files = tuple(_text(os.fspath(source), 'files') for source in files)
    if not files:
        raise ValueError('files must not be empty; no schema coverage would be checked')
    if reader is None:
        reader = read_root_schema
    if not callable(reader):
        raise TypeError('reader must be callable')
    names = tuple(item.name for item in requirements)
    schemas, issues = [], []
    for index, source in enumerate(files):
        location = f'files[{index}] ({source}):{tree}'
        try:
            metadata = reader(source, tree, names)
        except OSError as error:
            issues.append(ValidationIssue('schema.input', str(error), location))
            continue
        if not isinstance(metadata, InputSchema):
            raise TypeError('schema reader must return InputSchema')
        if metadata.source != source or metadata.tree != tree:
            raise ValueError('schema reader returned metadata for a different source/tree')
        schemas.append(metadata)
        report = compare_schema(requirements, metadata.columns)
        issues.extend(ValidationIssue(item.code, item.message, location+':'+item.location)
                      for item in report.issues)
    return InputSchemaCheck(ValidationReport('schema', ('schema',), tuple(issues)), files, tuple(schemas))


def main(argv=None):
    """Explicit ROOT metadata check using a JSON list of column requirements."""
    import argparse
    import json
    from pathlib import Path
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('requirements', type=Path, help='JSON list of {name,type?,location?}')
    parser.add_argument('files', nargs='+', help='explicit ROOT files/URLs; no discovery')
    parser.add_argument('--tree', default='Events')
    args = parser.parse_args(argv)
    try:
        values = json.loads(args.requirements.read_text())
        if not isinstance(values, list):
            raise ValueError('requirements must be a JSON list')
        requirements = [ColumnRequirement(**item) for item in values]
        result = check_input_schemas(args.files, requirements, tree=args.tree)
    except (OSError, ValueError, TypeError, ImportError) as error:
        report = ValidationReport('schema', ('schema',),
            (ValidationIssue('schema.setup', str(error), str(args.requirements)),))
        print(json.dumps(report.as_dict(), indent=2))
        return 2
    print(json.dumps(result.as_dict(), indent=2))
    return 0 if result.passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
