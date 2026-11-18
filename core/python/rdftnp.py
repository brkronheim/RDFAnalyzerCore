"""Reusable tag-and-probe graph helpers on a ROOT RDataFrame/RNode.

Physics selections, weights and the measured probe decision belong to callers.
One event contributes at most one pair, with no pass-dependent tag fallback.
"""
from pathlib import Path
import re
import math

def _columns(mapping, required):
    if set(mapping)!=set(required):raise ValueError('Expected columns: '+', '.join(required))
    for value in mapping.values():
        if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z_]\w*',value,re.ASCII):
            raise ValueError('Pass column names, not C++ expressions')

def _declare():
    import ROOT
    for path in [Path(__file__).resolve().parents[1]/'interface',Path(__file__).resolve().parents[1]/'include/rdfanalyzer']:
        if (path/'TagAndProbe.h').is_file():ROOT.gInterpreter.AddIncludePath(str(path));break
    if not ROOT.gInterpreter.Declare('#include "TagAndProbe.h"'):
        raise RuntimeError('TagAndProbe.h is not available to ROOT')

def define_pairs(node,prefix,columns,*,min_mass=60.,max_mass=120.,reference_mass=91.1876,
                 min_delta_r=0.,opposite_charge=True,seed=1):
    keys=['pt','eta','phi','mass','charge','denominator','tags','run','lumi','event']
    _columns(columns,keys);_columns({'prefix':prefix},['prefix'])
    if type(opposite_charge) is not bool:raise ValueError('opposite_charge must be bool')
    values=list(map(float,[min_mass,max_mass,reference_mass,min_delta_r]))
    if not all(math.isfinite(v) for v in values) or min_mass<0 or max_mass<=min_mass or min_delta_r<0:
        raise ValueError('Invalid mass or separation cuts')
    if type(seed) is not int or not 0<=seed<2**64:raise ValueError('seed must be uint64')
    _declare()
    config='rdf::tagprobe::PairConfig{'+','.join(map(repr,values))+','+str(bool(opposite_charge)).lower()+f',{seed}ULL'+'}'
    args=','.join(columns[k] for k in keys)
    node=node.Define(prefix+'_pair',f'rdf::tagprobe::selectPair({args},{config})')
    return node.Define(prefix+'_valid',f'{prefix}_pair[0]>=0')

def define_trigger_matches(node,prefix,columns,*,absolute_id,required_bits,min_pt=0.,max_delta_r=.1):
    keys=['eta','phi','trigger_pt','trigger_eta','trigger_phi','trigger_id','trigger_bits','fired']
    _columns(columns,keys);_columns({'prefix':prefix},['prefix'])
    if type(absolute_id) is not int or type(required_bits) is not int:
        raise ValueError('absolute_id and required_bits must be integers')
    if absolute_id<=0 or not 0<=required_bits<2**32 or not math.isfinite(min_pt) or min_pt<0 or not math.isfinite(max_delta_r) or max_delta_r<=0:
        raise ValueError('Invalid trigger matching settings')
    _declare()
    config=f'rdf::tagprobe::TriggerMatchConfig{{{int(absolute_id)},{int(required_bits)}U,{float(min_pt)!r},{float(max_delta_r)!r}}}'
    return node.Define(prefix+'_match','rdf::tagprobe::matchTriggerObjects('+','.join(columns[k] for k in keys)+','+config+')')
