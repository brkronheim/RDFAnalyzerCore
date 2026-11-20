"""Construction-time names for existing columns and homogeneous kernel outputs.

These objects contain no event data and never add or reorder graph operations.
"""
from collections.abc import Mapping
from dataclasses import dataclass, field
import re
from types import MappingProxyType


def _name(value, location):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z_]\w*', value, re.ASCII):
        raise ValueError(f'{location}: expected an identifier')
    return value


def _column(value, location):
    if not isinstance(value, str) or not value.strip() or any(c in value for c in '\0\n\r'):
        raise ValueError(f'{location}: expected a nonempty column name without NUL/newlines')
    return value


@dataclass(frozen=True)
class KernelField:
    """An unmaterialized checked vector projection, not a dataframe column."""
    source: str
    index: int
    nominal: bool = False

    def __post_init__(self):
        _column(self.source, 'KernelField.source')
        if type(self.index) is not int or self.index < 0:
            raise ValueError('KernelField.index: expected a nonnegative integer')
        if type(self.nominal) is not bool:
            raise ValueError('KernelField.nominal: expected a boolean')


@dataclass(frozen=True)
class KernelOutputs:
    """Ordered names for a homogeneous RVec<float> or RVec<double> result.

    Names document the kernel's existing vector layout. They do not create
    projections or validate the vector length until a field is consumed.
    """
    column: str
    fields: tuple
    nominal: bool = False

    def __post_init__(self):
        _name(self.column, 'KernelOutputs.column')
        if not isinstance(self.fields, (list, tuple)) or not self.fields:
            raise ValueError('KernelOutputs.fields: expected a nonempty list or tuple')
        object.__setattr__(self, 'fields', tuple(self.fields))
        for field in self.fields:
            _name(field, 'KernelOutputs.fields')
        if len(set(self.fields)) != len(self.fields):
            raise ValueError('KernelOutputs.fields: duplicate field names')
        if type(self.nominal) is not bool:
            raise ValueError('KernelOutputs.nominal: expected a boolean')

    def __getitem__(self, name):
        try:
            index = self.fields.index(name)
        except ValueError:
            raise KeyError(f'{self.column}: unknown field {name!r}; available: {self.fields}') from None
        return KernelField(self.column, index, self.nominal)

    def describe(self):
        return dict(column=self.column, fields={name: i for i, name in enumerate(self.fields)},
                    nominal=self.nominal, layout='homogeneous-vector; checked on projection')


def named_kernel_arguments(outputs, return_type):
    if not isinstance(outputs, KernelOutputs):
        raise TypeError('define_numba_outputs requires KernelOutputs')
    if return_type not in ('RVec<float>', 'RVec<double>'):
        raise ValueError('named outputs require explicit RVec<float> or RVec<double>; use define_numba for other result types')
    return outputs.column


def projection_arguments(source, index, nominal):
    if nominal is not None and type(nominal) is not bool:
        raise ValueError('projection nominal must be a boolean or None')
    if isinstance(source, KernelField):
        if index is not None:
            raise ValueError('a named kernel field already specifies its index')
        if nominal is not None and nominal != source.nominal:
            raise ValueError('projection nominal policy conflicts with the named kernel field')
        source, index, nominal = source.source, source.index, source.nominal
    if nominal is None:
        nominal = False
    # Reuse the same validation for positional and named projections.
    field = KernelField(source, index, nominal)
    return field.source, field.index, field.nominal


@dataclass(frozen=True)
class FieldMetadata:
    """User-declared column type, unit and help; no inference or conversion.

    dtype is the reader/native spelling the author expects, not a Numba type
    prescription. Metadata never validates a column or changes a consumer.
    """
    dtype: object = None
    unit: object = None
    description: str = ''

    def __post_init__(self):
        for name in ('dtype', 'unit'):
            value = getattr(self, name)
            if value is not None:
                _column(value, 'FieldMetadata.'+name)
        if not isinstance(self.description, str) or '\0' in self.description:
            raise ValueError('FieldMetadata.description: expected text without NUL')

    def describe(self):
        return dict(dtype=self.dtype, unit=self.unit, description=self.description)


@dataclass(frozen=True)
class CollectionRef:
    """A field-name view with optional user-declared alignment/index metadata.

    Alignment is an assertion about ordering, never a runtime length check.
    index_column, when provided, maps elements into index_target's ordering.
    No selection, reordering, correction, alias column or copy is performed.
    """
    name: str
    fields: object
    alignment: object = None
    index_column: object = None
    index_target: object = None
    metadata: object = field(default_factory=dict)

    def __post_init__(self):
        _name(self.name, 'CollectionRef.name')
        if not hasattr(self.fields, 'items') or not self.fields:
            raise ValueError('CollectionRef.fields: expected a nonempty mapping')
        fields = dict(self.fields)
        for name, column in fields.items():
            _name(name, 'CollectionRef.fields'); _column(column, f'CollectionRef.fields.{name}')
        object.__setattr__(self, 'fields', MappingProxyType(fields))
        if self.alignment is not None:
            _column(self.alignment, 'CollectionRef.alignment')
        if (self.index_column is None) != (self.index_target is None):
            raise ValueError('index_column and index_target must be supplied together')
        if self.index_column is not None:
            _column(self.index_column, 'CollectionRef.index_column')
            _column(self.index_target, 'CollectionRef.index_target')
        if not isinstance(self.metadata, Mapping):
            raise TypeError('CollectionRef.metadata: expected a field-to-FieldMetadata mapping')
        metadata = dict(self.metadata)
        for name, item in metadata.items():
            if not isinstance(name, str) or name not in fields:
                raise ValueError(f'CollectionRef.metadata: unknown field {name!r} in {self.name}')
            if not isinstance(item, FieldMetadata):
                raise TypeError(f'CollectionRef.metadata.{name}: expected FieldMetadata')
        object.__setattr__(self, 'metadata', MappingProxyType(metadata))

    def __getitem__(self, field):
        try:
            return self.fields[field]
        except KeyError:
            raise KeyError(f'{self.name}: unknown field {field!r}; available: {tuple(self.fields)}') from None

    def require_aligned(self, other):
        if not isinstance(other, CollectionRef):
            raise TypeError('require_aligned requires a CollectionRef')
        if self.alignment is None or other.alignment is None:
            raise ValueError('collection alignment is unknown; declare it explicitly before comparing')
        if self.alignment != other.alignment:
            raise ValueError(f'collection alignment differs: {self.name} and {other.name}; explicit index mapping is required')
        return self

    def schema_requirements(self):
        """Describe explicit presence/exact-type checks without inspecting data.

        Types keep the author's reader-owned spelling. Aliased physical columns
        appear once in first-field order; all logical field locations remain in
        diagnostics. Units, alignment and index metadata are informational.
        """
        from rdfschema import ColumnRequirement

        requirements, typed_origins = {}, {}
        for field_name, column in self.fields.items():
            location = f'CollectionRef({self.name}).fields.{field_name}'
            metadata = self.metadata.get(field_name)
            dtype = metadata.dtype if metadata is not None else None
            previous = requirements.get(column)
            if previous is not None:
                if previous.type is not None and dtype is not None and previous.type != dtype:
                    raise ValueError(
                        f'{location}: conflicting exact types for column {column!r}: '
                        f'{typed_origins[column]} expects {previous.type!r}; '
                        f'{location} expects {dtype!r}')
                dtype = previous.type if previous.type is not None else dtype
                locations = previous.location + ', ' + location
            else:
                locations = location
            if dtype is not None and column not in typed_origins:
                typed_origins[column] = location
            requirements[column] = ColumnRequirement(column, dtype, locations)
        return tuple(requirements.values())

    def describe(self):
        description = dict(name=self.name, fields=dict(self.fields), alignment=self.alignment,
                           index_column=self.index_column, index_target=self.index_target,
                           validation='user-declared metadata; no runtime length or index validation')
        # Preserve existing descriptions when no annotations are supplied.
        if self.metadata:
            description['metadata'] = {name: item.describe() for name, item in self.metadata.items()}
        return description
