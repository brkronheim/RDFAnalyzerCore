"""Prepare bounded sizing pilots and collect their verified measurements."""

from __future__ import annotations

from pathlib import Path
import hashlib
import json
import math
import re

from dataset_manifest import DatasetManifest
from job_sizing import EntryCache
from job_sizing import atomic_json
from job_sizing import collect_log
from job_sizing import condor_event_ads
from job_sizing import fit_pilot
from job_sizing import load_profile
from job_sizing import pilot_inputs
from job_sizing import seed_pilot_sizing
from job_sizing import select_sizing
from submission_backend import read_config
from run_bundle import file_digest
from .paths import SkimPaths
from . import job_config as _job_config


class PilotOperations(SkimPaths):
    @property
    def pilot_result_path(self):
        return self._pilots_dir / "result.json"

    @property
    def _pilots_dir(self):
        return Path(self._run_dir) / 'sizing_pilots'

    def _pilot_identity(self):
        if self.sizing_pilot_parallel < 1 or self.sizing_pilot_prefix_entries < 1:
            raise ValueError('Pilot parallelism and prefix entries must be positive')
        if not math.isfinite(self.sizing_pilot_safety_factor) or self.sizing_pilot_safety_factor < 1:
            raise ValueError('Pilot safety factor must be finite and at least one')
        if not self.file_source or self.partition not in {'auto', 'events', 'entry_range'}:
            raise ValueError('Sizing pilots require a file source and partition auto, events or entry_range')
        if str(read_config(self.submit_config).get('threads', '1')) != '1':
            raise ValueError('Paired sizing pilots currently require threads=1')
        inputs = dict(context=self._job_sizing_context(), manifest=file_digest(self.dataset_manifest),
                      discovery={p.name: file_digest(p) for p in sorted(Path(self._file_list_dir()).glob('*.json'))
                                 if not p.name.endswith('.perf.json')},
                      profile=self.partition_profile, metadata_seed=self.input_metadata_cache,
                      parallel=self.sizing_pilot_parallel, prefix=self.sizing_pilot_prefix_entries,
                      safety=self.sizing_pilot_safety_factor, max_runtime=self.max_runtime,
                      target_seconds=self.sizing_target_seconds,
                      pilot_target_seconds=self.sizing_pilot_target_seconds)
        for key, source in [('profile', self.partition_profile), ('metadata_seed', self.input_metadata_cache)]:
            if source and source != 'none':
                inputs[key+'_sha256'] = file_digest(source)
        return hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()

    def pilots_complete(self):
        if not self.pilot_result_path.is_file():
            return False
        try:
            result = json.loads(self.pilot_result_path.read_text())
            return (result['identity'] == self._pilot_identity() and
                    file_digest(result['profile']) == result['profile_sha256'] and
                    file_digest(result['metadata_seed']) == result['metadata_sha256'])
        except (OSError, ValueError, KeyError, TypeError):
            return False

    def _prepare_plan(self):
        identity = self._pilot_identity()
        path = self._pilots_dir / 'plan.json'
        if path.is_file():
            plan = json.loads(path.read_text())
            if plan['identity'] != identity:
                raise RuntimeError('Sizing pilot inputs changed; use a new run name')
            return plan
        context = self._job_sizing_context()
        selected = select_sizing(self._pilots_dir, context, profile=self.partition_profile,
                                 metadata_seed=self.input_metadata_cache,
                                 target_seconds=self.sizing_target_seconds,
                                 pilot_target_seconds=self.sizing_pilot_target_seconds)
        profile = load_profile(selected['profile'])
        manifest = DatasetManifest.load(self.dataset_manifest)
        missing = pilot_inputs(manifest, self._file_list_dir(), profile)
        base = _job_config._resolve_output_destination_base(self.submit_config, read_config(self.submit_config), self.output_directory)
        if missing and not base:
            raise ValueError('Sizing pilots require a remote --output-directory for diagnostic stage-out')
        batches = []
        for start in range(0, len(missing), self.sizing_pilot_parallel):
            number = len(batches)
            directory = self._pilots_dir / f'batch_{number:04d}'
            directory.mkdir(parents=True, exist_ok=True)
            child_manifest = DatasetManifest(datasets=missing[start:start+self.sizing_pilot_parallel],
                 lumi=manifest.lumi, lumi_by_year=manifest.lumi_by_year,
                 whitelist=manifest.whitelist, blacklist=manifest.blacklist)
            child_manifest.save_yaml(str(directory/'datasets.yaml'))
            batches.append(dict(name=f'{self.name}__sizing_pilot_{number:04d}',
                manifest=str((directory/'datasets.yaml').resolve()), datasets=[e.name for e in child_manifest.datasets],
                output_directory=base.rstrip('/') + f'_sizing_pilots/batch_{number:04d}'))
        plan = dict(schema_version=1, identity=identity, context=context, selection=selected, batches=batches)
        atomic_json(path, plan)
        return plan

    def collect_pilot_results(self, plan):
        profile = load_profile(plan['selection']['profile']) or {'schema_version': 1,
                                      'target_runtime_seconds': self.sizing_target_seconds}
        profile.setdefault('datasets', {})
        metadata = EntryCache(plan['selection']['metadata_seed'] or None)
        provisional = {}
        observations = {}
        for batch in plan['batches']:
            run = Path(self._workspace) / ('skimRun_' + batch['name'])
            logs = sorted(run.glob('condor_submissions/*/condor_logs/*out'))
            ads = condor_event_ads(list(run.glob('condor_submissions/*/condor_logs/*.log')))
            found = set()
            for log in logs:
                match = re.search(r'_(\d+)_(\d+)\.(?:stdout|out)$', log.name)
                if not match or (int(match[1]), int(match[2])) not in ads:
                    continue
                text = log.read_text(errors='replace')
                samples = re.findall(r'^RDF_JOB_SIZING_PILOT=(.+)$', text, re.M)
                if len(samples) != 1:
                    raise RuntimeError(f'Missing or ambiguous paired measurements in {log}')
                measurement = json.loads(samples[0])
                name = log.parent.parent.name
                if name not in batch['datasets'] or name in found:
                    raise RuntimeError(f'Ambiguous pilot dataset {name}')
                found.add(name)
                if measurement['full']['events'] == 0:
                    observations[name] = {'status': 'empty_file', 'measurement': measurement}
                else:
                    transfer = sum(float(v) for v in re.findall(r'^(?:Stage-(?:in|out)|Worker setup) time:\s*([\d.]+)s', text, re.M))
                    sizing = fit_pilot(measurement, self.sizing_pilot_safety_factor, transfer)
                    sizing['target_runtime_seconds'] = self.sizing_pilot_target_seconds
                    profile['datasets'][name] = sizing
                    provisional[name] = sizing
                    observations[name] = {'status': 'measured', 'sizing': sizing}
                entries, _ = collect_log(log, {})
                for url, n, tree in entries:
                    known = metadata.get(url, tree)
                    if known is not None and known != n:
                        raise ValueError(f'Conflicting pilot entry count for {url}')
                    metadata.put(url, n, tree)
            if found != set(batch['datasets']):
                raise RuntimeError(f"Missing successful measurements: {set(batch['datasets']) - found}")
        profile_path, metadata_path = self._pilots_dir/'profile.json', self._pilots_dir/'metadata.json'
        atomic_json(profile_path, profile)
        atomic_json(metadata_path, metadata.data)
        try:
            cache_update = seed_pilot_sizing(plan['context'], {'datasets': provisional}, metadata.data)
        except Exception as exc:
            cache_update = {'status': 'error', 'reason': str(exc)}
            self.publish_message(f'WARNING: Could not cache pilot estimates: {exc}')
        atomic_json(self.pilot_result_path, dict(schema_version=1, identity=plan['identity'],
            profile=str(profile_path.resolve()), profile_sha256=file_digest(profile_path),
            metadata_seed=str(metadata_path.resolve()), metadata_sha256=file_digest(metadata_path),
            observations=observations, batches=plan['batches'], cache_update=cache_update))
