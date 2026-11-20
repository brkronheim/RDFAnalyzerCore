"""Installed Python-authored scalar example; run is always explicit."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import time


def run(manifest,output):
    from rdfanalysis import Analysis,AnalysisConfig
    from rdfresults import RunResult
    output=Path(output).resolve()
    if output.exists()or not output.parent.is_dir():raise ValueError('fresh output and existing parent required')
    rows=json.loads(Path(manifest).read_text())['files']
    path=Path(__file__).with_name('physics.py')
    spec=importlib.util.spec_from_file_location('_sdk_onboarding_physics',path)
    physics=importlib.util.module_from_spec(spec);sys.modules[spec.name]=physics;spec.loader.exec_module(physics)
    started=time.perf_counter()
    analysis=Analysis(AnalysisConfig(files=[row['url']for row in rows],output=str(output.parent/'unused.root'),
        threads=1,skim=False,systematics=False,options={'enableCounters':'true','batch':'true',
        'metaFile':str(output.parent/'metadata.root')}))
    analysis.count('input')
    analysis.define_numba('sdk_weight',physics.weight,['genWeight'],return_type='double',nominal=True,require_native=True)
    analysis.define_numba('sdk_value',physics.observable,['sdk_weight'],return_type='double',nominal=True,require_native=True)
    analysis.define_numba('sdk_keep',physics.selected,['sdk_value'],return_type='bool',nominal=True,require_native=True)
    analysis.sum('input_weight','sdk_weight')
    analysis.filter('sdk_nonzero','sdk_keep')
    analysis.count('selected');analysis.sum('selected_weight','sdk_weight')
    analysis.histogram('observable','sdk_value',4,-8.,8.,weight='sdk_weight')
    ready=time.perf_counter();analysis.run();executed=time.perf_counter()
    histogram=analysis.histograms['observable']
    if analysis.native.event_loops!=1:raise RuntimeError('one event loop required')
    result=RunResult('python-numba','succeeded','completed Python-authored Analyzer scalar example',
        counts=analysis.counts,sums=analysis.sums,event_loops=analysis.native.event_loops,
        histograms={'observable':dict(edges=[-8.,-4.,0.,4.,8.],sumw=histogram['sumw'],sumw2=histogram['sumw2'],flow=True)},
        timings={'setup_s':ready-started,'processing_s':executed-ready},
        details={'numba_bridges':analysis.numba_bridges,'physics_source':'independent original Python source; no C++ kernel replacement'})
    document=result.as_dict();document['timings']['detached_export_s']=time.perf_counter()-executed
    document['details']['export_scope']='detached getters and result snapshot; excludes JSON encoding/file write'
    with output.open('x')as stream:json.dump(document,stream,indent=2,allow_nan=False);stream.write('\n')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--run',action='store_true');p.add_argument('--input-list',type=Path);p.add_argument('--output',type=Path);a=p.parse_args()
    if a.run:
        if not a.input_list or not a.output:p.error('explicit input list and output required')
        run(a.input_list,a.output)
    else:print(json.dumps(dict(execution_requested=False,scope='Python-authored Analyzer scalar example',native_accepted=False,installed_accepted=False)))
