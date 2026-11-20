#!/usr/bin/env python3
"""Explain collection requirements against supplied metadata; never open event data."""
import argparse
import json
from pathlib import Path

from rdfcolumns import CollectionRef, FieldMetadata
from rdfschema import compare_schema


def main():
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    parser.add_argument('--schema',type=Path,help='explicit JSON mapping of column names to reader-owned type spellings')
    args=parser.parse_args()
    jets=CollectionRef('jets',{'pt':'Jet_pt','eta':'Jet_eta'},metadata={
        'pt':FieldMetadata(dtype='ROOT::VecOps::RVec<float>',unit='GeV',description='selected jet transverse momentum'),
        'eta':FieldMetadata(dtype='ROOT::VecOps::RVec<float>',description='selected jet pseudorapidity')})
    columns={'Jet_pt':'ROOT::VecOps::RVec<float>','Jet_eta':'ROOT::VecOps::RVec<float>'}
    try:
        if args.schema is not None:
            columns=json.loads(args.schema.read_text())
        requirements=jets.schema_requirements()
        report=compare_schema(requirements,columns)
    except (OSError,TypeError,ValueError) as error:
        parser.error(str(error))
    print(json.dumps(dict(scope='supplied metadata only; no event input, ROOT, inference or unit conversion',
        collection=jets.describe(),requirements=[dict(name=item.name,type=item.type,location=item.location)
            for item in requirements],validation=report.as_dict()),indent=2))
    return 0 if report.passed else 1


if __name__=='__main__':
    raise SystemExit(main())
