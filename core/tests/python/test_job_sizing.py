import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'python'))
from job_sizing import EntryCache, event_budget, learn_rates, load_profile, collect_log
from job_sizing import (cached_sizing, condor_event_ads, select_sizing,
                        sizing_context, update_sizing_after_run)
from job_sizing import fit_pilot, seed_pilot_sizing


class JobSizingTest(unittest.TestCase):
    def test_rule_precedence_and_cpu_budget(self):
        sample = SimpleNamespace(name='DY_2J_2024', process='DY', era='2024', year=2024)
        profile = {'target_runtime_seconds': 2700,
                   'rules': [{'process': 'DY', 'cpu_seconds_per_event': .001},
                             {'dataset': '*2J*', 'entries_per_job': 800000}],
                   'datasets': {'DY_2J_2024': {'cpu_seconds_per_event': .004}}}
        self.assertEqual(event_budget(profile, sample, 100000), 675000)
        profile['datasets'] = {}
        self.assertEqual(event_budget(profile, sample, 100000), 800000)
        sample.name = 'DY_0J_2024'
        self.assertEqual(event_budget(profile, sample, 100000), 2700000)
        sample.process = 'Other'
        self.assertEqual(event_budget(profile, sample, 100000), 100000)

    def test_invalid_rates_and_typos_are_rejected(self):
        sample = SimpleNamespace(name='x')
        for rate in [0, -1, float('nan'), float('inf'), True]:
            with self.assertRaises(ValueError):
                event_budget({'defaults': {'cpu_seconds_per_event': rate}}, sample, 100)
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp, 'profile.json')
            p.write_text(json.dumps({'schema_version': 1, 'rules': [{'entry_per_job': 8}]}))
            with self.assertRaisesRegex(ValueError, 'Unknown'):
                load_profile(p)

    def test_cache_scopes_tree_and_redirector_and_preserves_zero(self):
        cache = EntryCache()
        cache.put('root://a//store/mc/file.root', 0)
        self.assertEqual(cache.get('root://b//store/mc/file.root'), 0)
        self.assertIsNone(cache.get('root://b//store/mc/file.root', 'Other'))
        cache.put('root://a//private.root', 20)
        self.assertIsNone(cache.get('root://b//private.root'))

    def test_local_file_cache_invalidates_when_file_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp, 'events.root');file.write_text('before')
            cache = EntryCache(Path(tmp, 'cache.json'))
            cache.put(str(file), 5);cache.save()
            self.assertEqual(EntryCache(cache.path).get(str(file)), 5)
            file.write_text('changed-and-longer')
            self.assertIsNone(EntryCache(cache.path).get(str(file)))

    def test_cached_queries_are_not_repeated(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = EntryCache(Path(tmp, 'cache.json'));calls=[]
            def query(url, tree):calls.append((url, tree));return 12
            cache.populate(['root://a//store/a', 'root://a//store/b'], query)
            cache.populate(['root://a//store/a', 'root://a//store/b'], query)
            self.assertEqual(len(calls), 2)
            self.assertEqual(EntryCache(cache.path).get('root://a//store/a'), 12)

    def test_fast_only_dataset_projects_to_slow_worker(self):
        records=[]
        for _ in range(4):
            for dataset, host, cpu in [('common', 'fast', 1), ('common', 'slow', 2), ('tail', 'fast', 5)]:
                records.append({'dataset': dataset, 'host': host, 'events': 1000, 'cpu_s': cpu})
        rates, factors = learn_rates(records)
        self.assertAlmostEqual(factors['slow']/factors['fast'], 2)
        self.assertAlmostEqual(rates['tail']['cpu_seconds_per_event'], .01)

    def test_failed_query_saves_successful_counts_for_next_preparation(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = EntryCache(Path(tmp, 'cache.json'))
            def query(url, tree):
                if url.endswith('/bad'):
                    raise OSError('unavailable')
                return 12
            with self.assertRaisesRegex(RuntimeError, 'unavailable'):
                cache.populate(['root://a//store/bad', 'root://a//store/good'], query)
            restored = EntryCache(cache.path)
            self.assertEqual(restored.get('root://b//store/good'), 12)
            self.assertIsNone(restored.get('root://b//store/bad'))

    def test_failed_attempts_supply_metadata_but_not_cost_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, 'log_100_2.stdout')
            path.write_text('[InputCache] reading statistics reason=end tree=Events file_index=0 file=root://a//store/f.root backend=RDFXrdFile tree_entries=123\nCounterService: sample=X entries=200000\nAnalysis time: 30\nDone!\n')
            metadata, record = collect_log(path, {(100, 2): {'JobStatus': 3, 'ExitCode': 2}})
            self.assertEqual(metadata, [('root://a//store/f.root', 123, 'Events')])
            self.assertIsNone(record)
            path.write_text(path.read_text() + 'RDF_JOB_SIZING_PILOT={}\n')
            _, record = collect_log(path, {(100, 2): {'JobStatus': 4, 'ExitCode': 0, 'RemoteUserCpu': 500}})
            self.assertIsNone(record)


class AutomaticSizingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.environment = patch.dict('os.environ', {'RDF_JOB_SIZING_CACHE': str(self.root/'cache'),
                                                     'RDF_JOB_SIZING_ORIGIN': 'cluster-a',
                                                     'RDF_JOB_SIZING_AUTO_UPDATE': '1'})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.context = {'workload': 'test-analysis', 'origin': 'cluster-a'}

    def test_paired_pilot_removes_fixed_cost_and_reserves_setup_in_budget(self):
        # CPU=10+.002*N, wall=50+.003*N; startup should not become a rate.
        measurement = {'prefix': {'events': 10000, 'cpu_s': 30, 'wall_s': 80},
                       'full': {'events': 100000, 'cpu_s': 210, 'wall_s': 350}}
        sizing = fit_pilot(measurement, safety_factor=1, transfer_seconds=5)
        self.assertAlmostEqual(sizing['cpu_seconds_per_event'], .002)
        self.assertAlmostEqual(sizing['wall_seconds_per_event'], .003)
        self.assertAlmostEqual(sizing['setup_seconds'], 55)
        self.assertEqual(event_budget({'target_runtime_seconds': 355, 'datasets': {'DY': sizing}},
                                     SimpleNamespace(name='DY'), 10), 100000)

    def test_short_or_noisy_pilot_uses_explicit_conservative_fallback(self):
        m = {'prefix': {'events': 20, 'cpu_s': 10, 'wall_s': 20},
             'full': {'events': 20, 'cpu_s': 12, 'wall_s': 18}}
        sizing = fit_pilot(m, safety_factor=1)
        self.assertEqual(sizing['measurement']['methods']['cpu'], 'full_average_fallback')
        self.assertEqual(sizing['cpu_seconds_per_event'], .6)
        self.assertEqual(sizing['setup_seconds'], 0)
        with self.assertRaises(ValueError):
            fit_pilot(m, safety_factor=.5)

    def test_pilot_prior_is_reusable_but_supported_measurements_replace_it(self):
        m = {'prefix': {'events': 100, 'cpu_s': 10, 'wall_s': 20},
             'full': {'events': 200000, 'cpu_s': 410, 'wall_s': 420}}
        prior = fit_pilot(m, safety_factor=1)
        seed_pilot_sizing(self.context, {'datasets': {'DY': prior}}, {'schema_version': 1, 'trees': {}})
        selection = select_sizing(self.root/'future', self.context)
        self.assertEqual(load_profile(selection['profile'])['datasets']['DY']['samples'], 1)
        run = self.root/'completed';self.logs(run, 100, [200]*3)
        update_sizing_after_run(run, self.context)
        learned = cached_sizing(self.context)['profile']['datasets']['DY']
        self.assertEqual(learned['samples'], 3)
        self.assertAlmostEqual(learned['cpu_seconds_per_event'], .001)

    def test_mixed_cached_evidence_uses_independent_targets_and_frozen_selections(self):
        m = {'prefix': {'events': 10000, 'cpu_s': 30, 'wall_s': 80},
             'full': {'events': 100000, 'cpu_s': 210, 'wall_s': 350}}
        # Persist an older pilot target: a new submission must apply its own
        # policy, even when reusing the prior without running another pilot.
        prior = dict(fit_pilot(m, safety_factor=1), target_runtime_seconds=9999)
        seed_pilot_sizing(self.context, {'datasets': {'pilot': prior}},
                          {'schema_version': 1, 'trees': {}})
        run = self.root/'completed';self.logs(run, 100, [600]*3, dataset='measured')
        update_sizing_after_run(run, self.context)
        selected = select_sizing(self.root/'defaults', self.context)
        profile = load_profile(selected['profile'])
        self.assertEqual(profile['datasets']['pilot']['target_runtime_seconds'], 1800)
        self.assertEqual(profile['datasets']['measured']['target_runtime_seconds'], 3600)
        self.assertEqual(event_budget(profile, SimpleNamespace(name='pilot'), 10), 583333)
        self.assertEqual(event_budget(profile, SimpleNamespace(name='measured'), 10), 1200000)
        changed = select_sizing(self.root/'custom', self.context, target_seconds=2400,
                                pilot_target_seconds=900)
        custom = load_profile(changed['profile'])
        self.assertEqual(event_budget(custom, SimpleNamespace(name='pilot'), 10), 283333)
        self.assertEqual(event_budget(custom, SimpleNamespace(name='measured'), 10), 800000)
        self.assertEqual(select_sizing(self.root/'defaults', self.context,
                                      target_seconds=2400, pilot_target_seconds=900), selected)
        self.assertEqual(load_profile(selected['profile']), profile)
        self.assertEqual(cached_sizing(self.context)['profile']['datasets']['pilot']['target_runtime_seconds'], 9999)

    def test_explicit_profile_targets_win_and_invalid_automatic_targets_fail(self):
        explicit = self.root/'explicit.json'
        explicit.write_text(json.dumps({'schema_version': 1, 'target_runtime_seconds': 120,
                                       'defaults': {'cpu_seconds_per_event': .002}}))
        selected = select_sizing(self.root/'explicit', self.context, profile=str(explicit),
                                  target_seconds=3600, pilot_target_seconds=1800)
        self.assertEqual(event_budget(load_profile(selected['profile']), SimpleNamespace(name='DY'), 10), 60000)
        for kwargs in ({'target_seconds': 0}, {'pilot_target_seconds': -1}):
            with self.assertRaises(ValueError):
                select_sizing(self.root/'bad', self.context, **kwargs)

    def logs(self, run, cluster, cpus, dataset='DY'):
        directory = run/'condor_submissions'/dataset/'condor_logs'
        directory.mkdir(parents=True)
        events = []
        for proc, cpu in enumerate(cpus):
            (directory/f'log_{cluster}_{proc}.stdout').write_text(
                f'[InputCache] reading statistics reason=end tree=Events file_index=0 '
                f'file=root://a//store/{cluster}_{proc}.root tree_entries=10000000\n'
                '[XrdAdaptor] context origin=cluster-a execution_host=worker\n'
                f'CounterService: sample={dataset} entries=200000\nAnalysis time: {cpu+10}s\nDone!\n')
            h, rem = divmod(cpu, 3600);m, s = divmod(rem, 60)
            events.append(f'005 ({cluster}.{proc:03}.000) 2026-11-06 10:00:00 Job terminated.\n'
                          '(1) Normal termination (return value 0)\n'
                          f'Usr 0 {h:02}:{m:02}:{s:02}, Sys 0 00:00:00 - Run Remote Usage\n'
                          'Usr 5 00:00:00, Sys 0 00:00:00 - Total Remote Usage\n...\n')
        (directory/'jobs.log').write_text(''.join(events))

    def test_first_run_learns_future_submission_and_selection_is_frozen(self):
        first = self.root/'run-first'
        selection = select_sizing(first, self.context)
        self.assertEqual(selection['source'], 'default')
        self.assertEqual(selection['profile'], '')
        self.assertFalse((self.root/'cache').exists())
        self.logs(first, 100, [200, 200, 200])
        result = update_sizing_after_run(first, self.context)
        self.assertEqual(result['datasets'], 1)
        second = self.root/'run-second'
        selected = select_sizing(second, self.context, target_seconds=900)
        self.assertEqual(selected['source'], 'learned')
        profile = load_profile(selected['profile'])
        self.assertEqual(event_budget(profile, SimpleNamespace(name='DY'), 10), 900000)
        self.assertEqual(EntryCache(selected['metadata_seed']).get('root://b//store/100_0.root'), 10000000)
        before = Path(selected['profile']).read_bytes()
        self.logs(second, 200, [800]*3)
        update_sizing_after_run(second, self.context)
        self.assertEqual(select_sizing(second, self.context), selected)
        self.assertEqual(Path(selected['profile']).read_bytes(), before)
        third = select_sizing(self.root/'run-third', self.context, target_seconds=900)
        self.assertEqual(event_budget(load_profile(third['profile']), SimpleNamespace(name='DY'), 10), 225000)

    def test_small_runs_accumulate_idempotently_and_concurrent_updates_keep_both(self):
        a, b = self.root/'a', self.root/'b'
        self.logs(a, 100, [200, 200]);self.logs(b, 200, [200])
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda r: update_sizing_after_run(r, self.context), [a, b]))
        self.assertTrue(all(r['status']=='updated' for r in results))
        state = cached_sizing(self.context)
        self.assertEqual(state['profile']['datasets']['DY']['samples'], 3)
        self.assertEqual(len(state['runs']), 2)
        path = Path(results[0]['cache']);before = path.stat().st_mtime_ns
        self.assertEqual(update_sizing_after_run(a, self.context)['status'], 'unchanged')
        self.assertEqual(path.stat().st_mtime_ns, before)

    def test_failed_attempts_only_learn_metadata_and_eviction_cpu_is_not_used(self):
        run = self.root/'run';self.logs(run, 100, [200, 200, 200])
        events = next(run.rglob('jobs.log'))
        events.write_text(events.read_text().replace('return value 0', 'return value 1'))
        result = update_sizing_after_run(run, self.context)
        self.assertEqual(result['datasets'], 0)
        self.assertEqual(result['cost_samples'], 0)
        self.assertEqual(result['cached_files'], 3)
        self.assertEqual(condor_event_ads([events]), {})
        events.write_text(events.read_text().replace('return value 1', 'return value 0'))
        self.assertEqual(condor_event_ads([events])[(100, 0)]['RemoteUserCpu'], 200)

    def test_explicit_profile_opt_out_context_and_expiry(self):
        run = self.root/'run';self.logs(run, 100, [200]*3)
        update_sizing_after_run(run, self.context)
        explicit = self.root/'manual.json'
        explicit.write_text(json.dumps({'schema_version': 1, 'defaults': {'entries_per_job': 42}}))
        chosen = select_sizing(self.root/'manual', self.context, profile=str(explicit))
        self.assertEqual(chosen['source'], 'explicit')
        self.assertEqual(load_profile(chosen['profile'])['defaults']['entries_per_job'], 42)
        with self.assertRaisesRegex(ValueError, 'inputs changed'):
            select_sizing(self.root/'manual', self.context, profile='none')
        with self.assertRaisesRegex(ValueError, 'workload changed'):
            select_sizing(self.root/'manual', dict(self.context, workload='changed'), profile=str(explicit))
        self.assertEqual(select_sizing(self.root/'disabled', self.context, profile='none')['profile'], '')
        self.assertIsNone(cached_sizing(dict(self.context, origin='cluster-b')))
        self.assertIsNone(cached_sizing(dict(self.context, workload='another-analysis')))
        with patch('job_sizing.time.time', return_value=10**12):
            self.assertIsNone(cached_sizing(self.context))
        with patch.dict('os.environ', {'RDF_JOB_SIZING_CACHE': 'none'}):
            self.assertEqual(update_sizing_after_run(run, self.context)['status'], 'disabled')

    def test_workload_matching_ignores_io_locations_but_includes_physics_and_code(self):
        exe = self.root/'analysis';exe.write_bytes(b'executable')
        asset = self.root/'selection.txt';asset.write_text('cut=20')
        cfg = dict(fileList='a.root', saveFile='old.root', sampleConfig='old.yaml',
                   intConfig=str(asset), systematics='true')
        context = sizing_context(cfg, exe, payloads=[asset])
        moved = dict(cfg, fileList='b.root', saveFile='new.root', sampleConfig='new.yaml')
        self.assertEqual(sizing_context(moved, exe, payloads=[asset]), context)
        self.assertNotEqual(sizing_context(dict(cfg, systematics='false'), exe, payloads=[asset]), context)
        asset.write_text('cut=30')
        self.assertNotEqual(sizing_context(cfg, exe, payloads=[asset]), context)
        exe.write_bytes(b'new executable')
        self.assertNotEqual(sizing_context(cfg, exe, payloads=[asset]), context)
