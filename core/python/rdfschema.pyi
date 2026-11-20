"""Explicit schema metadata helpers; ROOT remains an optional runtime reader."""
from dataclasses import dataclass
from os import PathLike
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple, Union
from rdfworkflow import ValidationReport

@dataclass(frozen=True)
class ColumnRequirement:
    name: str
    type: Optional[str] = ...
    location: str = ...

@dataclass(frozen=True)
class InputSchema:
    source: str
    tree: str
    columns: Mapping[str, str]
    def as_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class InputSchemaCheck:
    report: ValidationReport
    files: Tuple[str, ...]
    schemas: Tuple[InputSchema, ...]
    @property
    def passed(self) -> bool: ...
    def as_dict(self) -> dict[str, Any]: ...
    def raise_if_invalid(self) -> InputSchemaCheck: ...

def compare_schema(requirements: Sequence[ColumnRequirement], columns: Mapping[str, str]) -> ValidationReport: ...
def read_root_schema(source: str, tree: str, columns: Sequence[str]) -> InputSchema: ...
def check_input_schemas(files: Sequence[Union[str, PathLike[str]]], requirements: Sequence[ColumnRequirement], *,
    tree: str = ..., reader: Optional[Callable[[str, str, Tuple[str, ...]], InputSchema]] = ...) -> InputSchemaCheck: ...
def main(argv: Optional[list[str]] = ...) -> int: ...
