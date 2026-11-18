"""Provider-owned settings help/validation with detached, shared reports.

No schema engine, experiment registry, native imports or implicit discovery.
Configuration checks and explicitly requested asset checks remain distinct.
"""
from collections.abc import Mapping
from dataclasses import dataclass
import math
from _rdfjson import freeze as _json_freeze, plain as _json_plain

def _freeze(value):
    return _json_freeze(value, exact_keys=False, key_error='settings/schema keys must be strings', value_error='settings/schema must contain finite JSON-compatible data')


_plain = _json_plain


def validate_provider_settings(validator,settings,*,location='',level='configuration'):
    """Accept legacy raise/None validators or enforce a returned ValidationReport.

    Provider issue locations are relative to the supplied settings mapping.
    Domain ValueError/TypeError become issues; other provider exceptions propagate.
    Boolean/dict/arbitrary return values are contract errors, never a success.
    """
    from rdfworkflow import ValidationIssue,ValidationReport
    if not callable(validator):raise TypeError('settings validator must be callable')
    def issue(message,code='provider.invalid_settings'):
        return ValidationReport(level,(level,),(ValidationIssue(code,message,location),))
    if not isinstance(settings,Mapping):return issue('Expected a settings mapping','settings.type')
    try:
        values=_plain(_freeze(settings))
        result=validator(values)
    except (ValueError,TypeError) as error:
        return issue(str(error))
    if result is None:return ValidationReport(level,(level,))
    if not isinstance(result,ValidationReport):
        raise TypeError('settings validators must return None or ValidationReport, and raise on invalid legacy settings')
    if result.level!=level:raise TypeError(f'provider returned validation level {result.level!r}; expected {level!r}')
    issues=[]
    for item in result.issues:
        if not isinstance(item,ValidationIssue):raise TypeError('provider report issues must be ValidationIssue objects')
        path='.'.join(part for part in (location,item.location) if part)
        issues.append(ValidationIssue(item.code,item.message,path))
    return ValidationReport(level,tuple(result.supported_levels),tuple(issues))


@dataclass(frozen=True)
class SettingsSpec:
    name: str
    schema: object
    validator: object
    asset_validator: object=None
    documentation: str=''

    def __post_init__(self):
        if not isinstance(self.name,str) or not self.name.strip():raise ValueError('SettingsSpec.name must be nonempty')
        if not isinstance(self.schema,Mapping):raise TypeError('SettingsSpec.schema must be a mapping')
        object.__setattr__(self,'schema',_freeze(self.schema))
        if not callable(self.validator):raise TypeError('SettingsSpec.validator must be callable')
        if self.asset_validator is not None and not callable(self.asset_validator):raise TypeError('asset_validator must be callable or None')
        if not isinstance(self.documentation,str):raise TypeError('documentation must be a string')

    @property
    def supported_levels(self):return ('configuration','assets') if self.asset_validator is not None else ('configuration',)

    def validate(self,settings,*,level='configuration',location=None):
        from rdfworkflow import ValidationIssue,ValidationReport
        location=self.name if location is None else location
        if level not in self.supported_levels:
            return ValidationReport(level,self.supported_levels,(ValidationIssue('unsupported_level',
                'Supported levels: '+', '.join(self.supported_levels)+'; native schema/compile execution is not supplied by this provider',location),))
        report=validate_provider_settings(self.validator,settings,location=location)
        if level=='assets' and report.passed:
            report=validate_provider_settings(self.asset_validator,settings,location=location,level='assets')
        return ValidationReport(level,self.supported_levels,report.issues)

    def describe(self):
        from rdfworkflow import WorkflowDescription
        return WorkflowDescription('settings-provider',('configuration-validation',),{
            'name':self.name,'schema':_plain(self.schema),'supported_levels':list(self.supported_levels),
            'validation':'provider-owned functions; schema is descriptive, not automatically enforced',
            'documentation':self.documentation,'discovery':'explicit; no builders or event execution'})


def discover_settings_specs():
    """Explicitly collect optional settings_specs() from existing config_schemas services."""
    from optional_extensions import load_extension_services
    specs=[];names=set()
    for provider in load_extension_services('config_schemas'):
        factory=getattr(provider,'settings_specs',None)
        if factory is None:continue  # Older PLUGIN_SCHEMAS providers remain valid.
        if not callable(factory):raise TypeError('settings_specs must be callable')
        for spec in factory():
            if not isinstance(spec,SettingsSpec):raise TypeError('settings_specs() must return SettingsSpec objects')
            if spec.name in names:raise ValueError('duplicate settings provider: '+spec.name)
            names.add(spec.name);specs.append(spec)
    return tuple(specs)


def main(argv=None):
    import argparse
    import json
    from pathlib import Path
    from rdfworkflow import ValidationIssue,ValidationReport
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    commands.add_parser('schemas')
    schema=commands.add_parser('schema');schema.add_argument('provider')
    validate=commands.add_parser('validate');validate.add_argument('provider');validate.add_argument('settings',type=Path)
    validate.add_argument('--level',default='configuration')
    args=parser.parse_args(argv);specs={spec.name:spec for spec in discover_settings_specs()}
    if args.command=='schemas':output=[spec.describe().as_dict() for spec in specs.values()];code=0
    else:
        if args.provider not in specs:parser.error('Unknown settings provider: '+args.provider)
        spec=specs[args.provider]
        if args.command=='schema':output=spec.describe().as_dict();code=0
        else:
            try:settings=json.loads(args.settings.read_text())
            except (OSError,json.JSONDecodeError) as error:
                path=str(args.settings.absolute())
                if isinstance(error,json.JSONDecodeError):path+=f':{error.lineno}:{error.colno}'
                report=ValidationReport(args.level,spec.supported_levels,(ValidationIssue('settings.parse',str(error),path),))
            else:report=spec.validate(settings,level=args.level,location=str(args.settings.absolute())+':settings')
            output=report.as_dict();code=0 if report.passed else 1
    print(json.dumps(output,indent=2));return code


if __name__=='__main__':
    # Providers import the canonical module's SettingsSpec. Keep that identity
    # when Python executes this file under the separate __main__ module name.
    from rdfvalidation import main as canonical_main
    raise SystemExit(canonical_main())
