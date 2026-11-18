"""Inspect or explicitly run the same compiled Analyzer executable from Python."""
import argparse
import json
from pathlib import Path
from rdfworkflow import CppAnalysisSpec


def specification(executable,config,output):
    output=Path(output).resolve();config=Path(config).resolve()
    return CppAnalysisSpec(executable=executable,arguments=('--run',str(config),str(output)),
        working_directory=output.parent,assets={'configuration':config},outputs={'summary':output},timeout_s=300)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--executable',type=Path,required=True);p.add_argument('--config',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--run',action='store_true');p.add_argument('--logs',type=Path);a=p.parse_args()
    spec=specification(a.executable,a.config,a.output)
    if a.run:
        if not a.logs:p.error('--run requires --logs')
        result=spec.run(a.logs,check=False);print(json.dumps(result.to_run_result().as_dict(),indent=2));raise SystemExit(0 if result.success else 1)
    print(json.dumps(spec.describe().as_dict(),indent=2))
