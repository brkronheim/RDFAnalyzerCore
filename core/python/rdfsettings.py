"""Optional typed native settings. Importing this module uses only the stdlib.

None means omit the native key, retaining native defaults or an explicit legacy
option. These definitions drive both runtime validation and the public schema.
Experiment-specific settings remain owned by their extensions.
"""
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Mapping, Optional


def _option(native, kind, description, **constraints):
    return field(default=None, metadata=dict(native_key=native, type=kind,
                                             description=description, **constraints))


class _Settings:
    def __post_init__(self):
        for item in fields(self):
            value=getattr(self,item.name)
            if value is None:continue
            spec=item.metadata;location=f'{type(self).__name__}.{item.name}'
            kind={'boolean':bool,'integer':int,'string':str}[spec['type']]
            if type(value) is not kind:
                raise ValueError(f'{location}: expected {spec["type"]} or None')
            if kind is str and (not value.strip() or any(char in value for char in ('\0','\n','\r'))):
                raise ValueError(f'{location}: expected a nonempty string without NUL/newlines')
            if 'minimum' in spec and value<spec['minimum'] or 'maximum' in spec and value>spec['maximum']:
                raise ValueError(f'{location}: expected {spec.get("minimum")} through {spec.get("maximum")}')
            if 'choices' in spec and value not in spec['choices']:
                raise ValueError(f'{location}: expected one of {spec["choices"]}')
            if spec.get('path'):
                if '://' in value:raise ValueError(f'{location}: expected a local path')
                object.__setattr__(self,item.name,str(Path(value).absolute()))

    @classmethod
    def from_mapping(cls, values, *, base=None):
        if isinstance(values,cls):return values
        if not isinstance(values,Mapping):raise ValueError(f'{cls.__name__}: expected a mapping or {cls.__name__}')
        names={item.name for item in fields(cls)}
        unknown=values.keys()-names
        if unknown:raise ValueError(f'{cls.__name__}: unknown fields: '+', '.join(sorted(map(str,unknown))))
        resolved=dict(values)
        if base is not None:
            for item in fields(cls):
                value=resolved.get(item.name)
                if item.metadata.get('path') and isinstance(value,str) and value and '://' not in value:
                    resolved[item.name]=str(Path(base)/value)
        return cls(**resolved)

    def native_options(self):
        return {item.metadata['native_key']:str(value).lower() if type(value) is bool else str(value)
                for item in fields(self) if (value:=getattr(self,item.name)) is not None}

    def values(self):
        return {item.name:getattr(self,item.name) for item in fields(self)}

    @classmethod
    def schema(cls):
        return {item.name:{**dict(item.metadata),'default':None,'nullable':True,
                           'unset_behavior':'omit native key; retain legacy option or framework default'}
                for item in fields(cls)}


@dataclass(frozen=True)
class InputSettings(_Settings):
    cache_enabled: Optional[bool]=_option('treeCacheEnabled','boolean','Enable the existing optimized input cache policy.')
    cache_size_mb: Optional[int]=_option('treeCacheSizeMB','integer','Requested cache MiB; requires cache_enabled=True.',minimum=0,maximum=512)
    cache_statistics: Optional[bool]=_option('treeCacheStats','boolean','Report existing input-cache statistics.')
    cache_verbosity: Optional[int]=_option('treeCacheVerbosity','integer','Input-cache diagnostic verbosity.',minimum=0,maximum=2)
    float_constants: Optional[str]=_option('floatConfig','string','Native floating-point constants configuration.',path=True,asset=True)
    integer_constants: Optional[str]=_option('intConfig','string','Native integer constants configuration.',path=True,asset=True)
    aliases: Optional[str]=_option('aliasConfig','string','Native input-column aliases configuration.',path=True,asset=True)
    optional_branches: Optional[str]=_option('optionalBranchesConfig','string','Native optional-branch configuration.',path=True,asset=True)
    friends: Optional[str]=_option('friendConfig','string','Native friend-tree configuration; referenced trees are not recursively validated.',path=True,asset=True)

    def __post_init__(self):
        super().__post_init__()
        if self.cache_size_mb is not None and self.cache_enabled is not True:
            raise ValueError('InputSettings.cache_size_mb: requires cache_enabled=True; otherwise native code ignores this size')


@dataclass(frozen=True)
class SkimSettings(_Settings):
    tree: Optional[str]=_option('saveTree','string','Skim output tree name; None uses AnalysisConfig.tree.')
    metadata_file: Optional[str]=_option('metaFile','string','Metadata output path; None retains the existing derived path or legacy option.',path=True)
    systematics: Optional[bool]=_option('skimSystematics','boolean','Include available systematic columns in the skim; independent of histogram variations.')
    compression_algorithm: Optional[str]=_option('skimCompressionAlgorithm','string','Lossless compression for skim output only.',choices=('zstd','lzma','zlib','lz4'))
    compression_level: Optional[int]=_option('skimCompressionLevel','integer','Skim compression level; zero disables compression.',minimum=0,maximum=9)
