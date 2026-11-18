"""Installed example: inspect, validate or explicitly run a native analysis spec."""
import argparse
import json
from pathlib import Path
from rdfworkflow import CppAnalysisSpec


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('spec', type=Path, help='analysis-owned compiled specification JSON')
    parser.add_argument('--mode', choices=('inspect', 'validate', 'run'), default='inspect')
    parser.add_argument('--validation-level',
        help='explicit advertised validation level (validate mode only; default: assets)')
    parser.add_argument('--logs', type=Path, help='parent for fresh build/run log directories')
    parser.add_argument('--build', action='store_true', help='explicitly build before running; never implicit')
    parser.add_argument('--progress-interval', type=float,
        help='optional controller snapshot interval in seconds (0.5 to 3600)')
    args = parser.parse_args(argv)
    if args.build and args.mode != 'run':
        parser.error('--build requires --mode run')
    if args.mode == 'run' and args.logs is None:
        parser.error('--mode run requires --logs')
    if args.progress_interval is not None and args.mode != 'run':
        parser.error('--progress-interval requires --mode run')
    if args.validation_level is not None and args.mode != 'validate':
        parser.error('--validation-level requires --mode validate')
    try:
        spec = CppAnalysisSpec.from_file(args.spec)
        spec.validate().raise_if_invalid()
        if args.mode == 'inspect':
            document, success = spec.describe().as_dict(), True
        elif args.mode == 'validate':
            report = spec.validate(args.validation_level or 'assets')
            document, success = report.as_dict(), report.passed
        else:
            if args.build:
                if spec.build_spec is None:
                    raise ValueError('--build requires build_spec in the compiled specification')
                build = spec.build_spec.build(args.logs/'build', check=False,
                    progress_interval_s=args.progress_interval)
                if not build.success:
                    print(json.dumps(build.to_run_result().as_dict(), indent=2))
                    return 1
            command = spec.run(args.logs/'run', check=False,
                progress_interval_s=args.progress_interval)
            document, success = command.to_run_result().as_dict(), command.success
    except (ValueError, TypeError, OSError) as error:
        document, success = {'success': False, 'error': str(error)}, False
    print(json.dumps(document, indent=2))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
