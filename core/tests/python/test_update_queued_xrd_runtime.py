import io
import json
import tarfile
from pathlib import Path

from core.python.update_queued_xrd_runtime import plan_edits, proc_expression, transfer_input_value, update_queued
from core.python.refresh_xrootd_runtime import archive_hashes


def test_only_idle_unstarted_jobs_and_exact_archive_are_eligible(tmp_path):
    run=tmp_path/'run'; old=run/'shared_inputs.tar.gz'; new=run/'immutable/shared_inputs.tar.gz'
    def job(proc, **extra):
        return dict(ClusterId=123,ProcId=proc,JobStatus=1,
                    TransferInput=f'{run}/job_{proc}/cfg,{old}',**extra)
    jobs=[job(0), job(1), dict(job(2),JobStatus=2), job(3,JobCurrentStartDate=10),
          job(4,TransferringInput=True), dict(job(5),TransferInput='/other/shared_inputs.tar.gz')]
    plans=plan_edits(jobs,run,new,'hash')
    assert len(plans)==1
    assert plans[0]['jobs']==['123.0','123.1']
    assert 'JobStatus == 1' in plans[0]['constraint']
    assert 'JobCurrentStartDate' in plans[0]['constraint']
    assert 'TransferInput ==' in plans[0]['constraint']
    command=plans[0]['command']
    assert command[command.index('TransferInput')+1] == proc_expression(f'{run}/job_0/cfg,{new}',0)
    assert str(new) in command[command.index('TransferInput')+1]


def test_deployment_preserves_originals_and_uses_an_immutable_verified_archive(tmp_path):
    run=tmp_path/'run';run.mkdir();old=run/'shared_inputs.tar.gz'
    with tarfile.open(old,'w:gz') as tar:
        for name,data in [('libRDFXrdAdaptor.so',b'\x7fELFold'),('analysis',b'executable'),('cfg/input',b'config')]:
            member=tarfile.TarInfo(name);member.size=len(data);tar.addfile(member,io.BytesIO(data))
    state={'datasets':{'sample':{'jobs':{'0':{'cluster':'123'}}}}}
    (run/'monitor_state.json').write_text(json.dumps(state))
    library=tmp_path/'new.so';library.write_bytes(b'\x7fELFnew')
    before=[dict(ClusterId=123,ProcId=0,JobStatus=1,TransferInput=f'{run}/job_0/cfg,{old}'),
            dict(ClusterId=123,ProcId=1,JobStatus=2,TransferInput=f'{run}/job_1/cfg,{old}')]
    calls=[]
    def query(command):
        calls.append(command)
        return json.dumps(before) if command[0]=='condor_q' else 'edited 1 job'
    old_bytes=old.read_bytes()
    preview=update_queued([run],library,query=query)
    assert preview['runs'][0]['eligible_jobs']==1
    assert not (run/'runtime_patches').exists()
    calls.clear()
    result=update_queued([run],library,apply=True,query=query)
    archive=Path(result['runs'][0]['archive'])
    hashes=archive_hashes(archive); original=archive_hashes(old)
    assert hashes['analysis']==original['analysis']
    assert hashes['cfg/input']==original['cfg/input']
    assert hashes['libRDFXrdAdaptor.so']!=original['libRDFXrdAdaptor.so']
    assert old.read_bytes()==old_bytes
    assert json.loads((run/'monitor_state.json').read_text())==state
    edits=[c for c in calls if c[0]=='condor_qedit']
    assert len(edits)==1
    assert 'JobStatus == 1' in edits[0][2]
    assert archive.stat().st_mode & 0o222 == 0


def test_jobs_starting_between_query_and_edit_are_safely_skipped(tmp_path):
    import subprocess
    run=tmp_path/'run';run.mkdir()
    (run/'monitor_state.json').write_text(json.dumps({'datasets':{'s':{'jobs':{'0':{'cluster':123}}}}}))
    old=run/'shared_inputs.tar.gz'
    with tarfile.open(old,'w:gz') as tar:
        member=tarfile.TarInfo('libRDFXrdAdaptor.so');member.size=7
        tar.addfile(member,io.BytesIO(b'\x7fELFold'))
    library=tmp_path/'new.so';library.write_bytes(b'\x7fELFnew')
    before=[dict(ClusterId=123,ProcId=0,JobStatus=1,TransferInput=str(old))]
    count=0
    def query(command):
        nonlocal count
        if command[0]=='condor_qedit':
            raise subprocess.CalledProcessError(1,command,stderr='Queue transaction failed. No attributes were set.')
        count+=1
        return json.dumps(before) if count==1 else ''
    report=update_queued([run],library,apply=True,query=query)
    assert report['patched_jobs_in_queue']==0
    assert report['runs'][0]['plans'][0]['result'].startswith('Skipped:')


def test_previous_immutable_runtime_and_condor_expression_can_be_upgraded(tmp_path):
    run=tmp_path/'run';old=run/'runtime_patches/located_0123456789abcdef/shared_inputs.tar.gz'
    new=run/'runtime_patches/located_fedcba9876543210/shared_inputs.tar.gz'
    value=f'{run}/job_7/submit_config.txt,{old},{run}/job_7/sites.json'
    encoded='/Expr('+proc_expression(value,7)+')/'
    assert transfer_input_value(encoded,7)==value
    assert transfer_input_value(encoded,8)==value.replace('/job_7/','/job_8/')
    jobs=[dict(ClusterId=1,ProcId=7,JobStatus=1,TransferInput=encoded)]
    plans=plan_edits(jobs,run,new,'new')
    assert len(plans)==1
    assert plans[0]['jobs']==['1.7']
    command=plans[0]['command']
    assert str(new) in command[command.index('TransferInput')+1]
    # Never evaluate arbitrary ClassAd expressions or rewrite another run.
    assert transfer_input_value('/Expr(system("anything"))/',7) is None
    assert transfer_input_value('/Expr(strcat("x",bad()))/',7) is None
    jobs[0]['TransferInput']=value.replace(str(run),str(tmp_path/'other'))
    assert not plan_edits(jobs,run,new,'new')


def test_retry_refresh_changes_only_library_with_active_monitor_and_running_job(tmp_path):
    import hashlib
    run=tmp_path/'run';shared=run/'shared_inputs';shared.mkdir(parents=True)
    loose=shared/'libRDFXrdAdaptor.so';loose.write_bytes(b'\x7fELFold')
    old=run/'shared_inputs.tar.gz'
    with tarfile.open(old,'w:gz') as tar:
        for name,data in [('libRDFXrdAdaptor.so',loose.read_bytes()),('analysis',b'keep-executable'),
                          ('x509',b'keep-proxy'),('cfg_bundle.tar.gz',b'keep-config')]:
            member=tarfile.TarInfo(name);member.size=len(data);tar.addfile(member,io.BytesIO(data))
    state={'datasets':{'s':{'shared_transfer_files':[str(old)],'jobs':{'0':{'cluster':123}}}}}
    state_path=run/'monitor_state.json';state_path.write_text(json.dumps(state))
    script=run/'condor_runscript.sh';script.write_text('keep running worker script')
    library=tmp_path/'new.so';library.write_bytes(b'\x7fELFnew')
    jobs=[dict(ClusterId=123,ProcId=0,JobStatus=2,TransferInput=str(old))]
    calls=[]
    def query(command):
        calls.append(command)
        return json.dumps(jobs)
    originals=archive_hashes(old);original_bytes=old.read_bytes()
    preview=update_queued([run],library,refresh_retries=True,query=query)
    assert preview['runs'][0]['retry_refresh']['changed']
    assert old.read_bytes()==original_bytes
    assert not (run/'runtime_patches').exists()
    result=update_queued([run],library,apply=True,refresh_retries=True,query=query)
    row=result['runs'][0];refresh=row['retry_refresh']
    assert refresh['verified']
    expected=dict(originals,**{'libRDFXrdAdaptor.so':hashlib.sha256(library.read_bytes()).hexdigest()})
    assert archive_hashes(old)==expected
    assert archive_hashes(Path(row['archive']))==expected
    assert loose.read_bytes()==library.read_bytes()
    backup=Path(refresh['backup_dir'])
    assert (backup/'shared_inputs.tar.gz').read_bytes()==original_bytes
    assert (backup/'libRDFXrdAdaptor.so').read_bytes()==b'\x7fELFold'
    assert json.loads(state_path.read_text())==state
    assert script.read_text()=='keep running worker script'
    assert not any(c[0]=='condor_qedit' for c in calls)
    again=update_queued([run],library,apply=True,refresh_retries=True,query=query)
    assert not again['runs'][0]['retry_refresh']['changed']
