"""Real control methods with frozen panes and scratch-only filesystem effects."""
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from hostdriver import DriverError, Workload
from hostdrivers.claude import ClaudeHostDriver
from matrix import Cell
import test_hostdriver as support

ROOT = Path(__file__).parent/'testdata/hostdriver/claude'


def terminal(name):
    return json.loads((ROOT/('submission-'+name)/'terminal.json').read_text())


class HermeticControls(unittest.TestCase):
    def prepare(self, state='idle', session='resumed'):
        helper = support.DriverControls()
        self.addCleanup(helper.doCleanups)
        driver = helper.prepare(cell=Cell('channel', state, session, 'text', 'single'))
        self.addCleanup(driver.stop, False)
        for method in ('time', 'monotonic', 'sleep'):
            patched = patch('hostdrivers.claude_host.time.'+method, getattr(helper.clock, method))
            patched.start()
            self.addCleanup(patched.stop)
        driver.launch()
        return helper, driver

    def test_submission_frozen_panes_and_bounded_retries(self):
        for name, expected_kind in (('unsent', 'timeout'), ('unknown', 'unrecognized_state'), ('exited', 'host_exited')):
            with self.subTest(pane=name):
                h, driver = self.prepare()
                data = terminal(name)
                calls = []
                def tmux(*args, **kwargs):
                    calls.append(args)
                    return SimpleNamespace(stdout=data['pane'], stderr='', returncode=data['returncode'])
                h.backend.tmux.side_effect = tmux
                started = h.clock.now
                with self.assertRaises(DriverError) as caught:
                    driver.send_prompt(data['prompt'])
                self.assertEqual(caught.exception.kind, expected_kind)
                self.assertTrue(caught.exception.submission_uncertain)
                self.assertEqual(sum('-l' in call for call in calls), 1)
                self.assertEqual(sum(call[-1]=='Enter' for call in calls), 5 if name=='unsent' else 0)
                self.assertEqual(h.clock.now-started, 7.5 if name=='unsent' else 0)
                before = len(calls)
                with self.assertRaises(DriverError):
                    driver.send_prompt(data['prompt'])
                self.assertEqual(len(calls), before)
                self.assertFalse(any(row['method']=='send_prompt' for row in h.scratch.journal.entries.values()))

    def test_wrapped_prompt_requires_observed_transition(self):
        h, driver = self.prepare()
        panes = iter([terminal('wrapped')['pane']]*7 + [terminal('submitted')['pane']])
        calls = []
        def tmux(*args, **kwargs):
            calls.append(args)
            return SimpleNamespace(stdout=next(panes) if args[0]=='capture-pane' else '', stderr='', returncode=0)
        h.backend.tmux.side_effect = tmux
        proof = driver.send_prompt('frozen prompt')
        facts = h.scratch.journal.entries[proof.positions[0].seq]['facts']
        self.assertEqual(facts['composer_before'], '❯ frozen\n  prompt')
        self.assertEqual(facts['composer'], '❯')
        self.assertEqual(sum('-l' in c for c in calls), 1)
        self.assertEqual(sum(c[-1]=='Enter' for c in calls), 2)
        self.assertEqual(proof.mechanism, 'composer_transition')

    def test_real_menu_navigation_and_unknown_menu_fail_closed(self):
        for known in (True, False):
            with self.subTest(known=known):
                h, driver = self.prepare(state='reconnect')
                selected = driver._session
                driver.barrier(h.point('readiness', 'arm'))
                panes = iter(['────\n❯ /mcp\n────', '────\n❯\n────',
                              'Manage MCP servers',
                              'Manage MCP servers\n❯ unrelated\n  plugin:c3:c3' if known else 'Manage MCP servers\nnew layout',
                              'Manage MCP servers\n❯ plugin:c3:c3', '  3. Reconnect'])
                calls = []
                def tmux(*args, **kwargs):
                    calls.append(args)
                    if args[-2:]==('3', 'Enter'):
                        h.events.extend([dict(event='proxy_started',pid=7,time=1002),
                                         dict(event='initialized_waiting',pid=7,time=1003)])
                    return SimpleNamespace(stdout=next(panes) if args[0]=='capture-pane' else '', stderr='', returncode=0)
                h.backend.tmux.side_effect = tmux
                if known:
                    # wait_setup also captures the post-reconnect pane.
                    from itertools import chain, repeat
                    panes = chain(panes, repeat('────\n❯\n────'))
                    result = driver.reconnect(selected)
                    self.assertEqual(result.current_session.host_session_id, selected.host_session_id)
                    self.assertIsNotNone(result.current_session.connection_epoch_id)
                    self.assertEqual([c[3:] for c in calls if c[0]=='send-keys'],
                                     [('-l','--','/mcp'),('Enter',),('Down',),('Enter',),('3','Enter')])
                else:
                    with self.assertRaises(DriverError) as caught:
                        driver.reconnect(selected)
                    self.assertEqual(caught.exception.kind, 'timeout')
                    self.assertIn('unrecognized /mcp menu', caught.exception.detail)
                    self.assertEqual([c[-1] for c in calls if c[0]=='send-keys'], ['/mcp','Enter'])
                    self.assertFalse((h.backend.control/'release-initialized-7').exists())

    def test_real_workload_reader_and_injection_revalidation(self):
        for state in ('foreground', 'background'):
            for damage in ('none', 'missing-marker', 'stale-invocation', 'wrong-command', 'wrong-background'):
                with self.subTest(state=state, damage=damage):
                    h, driver = self.prepare(state=state)
                    program = "import pathlib,time; pathlib.Path('tool-running').write_text('running'); time.sleep(35); pathlib.Path('tool-running').unlink()"
                    command = 'python3 -c '+shlex.quote(program)
                    call = dict(type='tool_use',name='Bash',input=dict(command=command,run_in_background=state=='background'))
                    record = dict(type='assistant',timestamp='2099-01-01T00:00:00Z',message=dict(content=[call]))
                    transcript = h.scratch.root/'transcript.jsonl'
                    marker = h.backend.cwd/'tool-running'
                    marker.touch()
                    transcript.write_text(json.dumps(record)+'\n')
                    # Real send/composer, tool_state, checked transcript selection,
                    # and marker lookup all execute; only terminal I/O is fake.
                    evidence = driver.enter_state(state, Workload('sleep',35000,'workload'))
                    self.assertEqual(evidence.state, state)
                    if damage=='missing-marker': marker.unlink()
                    elif damage=='wrong-command': call['input']['command']='different command'
                    elif damage=='wrong-background': call['input']['run_in_background']=state!='background'
                    elif damage=='stale-invocation':
                        # A stale marker cannot replace a matching invocation.
                        record['message']['content']=[]
                    transcript.write_text(json.dumps(record)+'\n')
                    if damage=='none':
                        h.checkpoint('injection')
                        tables, _ = driver.observe()
                        self.assertIn(dict(action='state_sample',observed_state=state),dict(tables.reads)['state']['records'])
                    else:
                        with self.assertRaises(DriverError) as caught: h.checkpoint('injection')
                        self.assertEqual(caught.exception.kind,'unrecognized_state')
                        tables, _ = driver.observe()
                        self.assertFalse(any(r['action']=='state_sample' for r in dict(tables.reads)['state']['records']))

    def test_stale_release_removed_and_release_is_idempotent(self):
        h, driver = self.prepare(state='startup')
        stale = h.backend.control/'release-initialized-7'
        stale.touch()
        armed = driver.barrier(h.point('readiness','arm'))
        self.assertFalse(stale.exists())

        h.events.extend([dict(event='proxy_started',pid=7,time=1002),dict(event='initialized_waiting',pid=7,time=1003)])
        held = driver.barrier(h.point('readiness','wait',release=armed))
        h.checkpoint('injection')
        driver.barrier(h.point('readiness','release',release=held))
        with patch.object(Path,'touch',side_effect=AssertionError('duplicate release side effect')):
            driver.barrier(h.point('readiness','release',release=held))
        self.assertTrue(stale.exists())
        driver.barrier(h.point('readiness','arm'))
        with self.assertRaises(DriverError):driver.barrier(h.point('readiness','release',release=held))
        self.assertFalse(stale.exists())

    def test_stale_marker_and_old_invocation_cannot_establish_state(self):
        h, driver = self.prepare(state='foreground')
        program = "import pathlib,time; pathlib.Path('tool-running').write_text('running'); time.sleep(35); pathlib.Path('tool-running').unlink()"
        record = dict(type='assistant',timestamp='1970-01-01T00:00:00Z',message=dict(content=[
            dict(type='tool_use',name='Bash',input=dict(command='python3 -c '+shlex.quote(program),run_in_background=False))]))
        (h.scratch.root/'transcript.jsonl').write_text(json.dumps(record)+'\n')
        (h.backend.cwd/'tool-running').touch()
        with self.assertRaises(DriverError) as caught:
            driver.enter_state('foreground',Workload('sleep',35000,'workload'))
        self.assertIn('required Bash sleep/run_in_background state was not observed',caught.exception.detail)
        self.assertLessEqual(h.clock.now,198)
        self.assertFalse(any(row['method']=='enter_state' for row in h.scratch.journal.entries.values()))

    def test_unknown_recipe_is_setup_failure_in_real_orchestrator(self):
        import driver as orchestrator
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch = root/'scratch'
            scratch.mkdir()
            terminal = Mock(side_effect=AssertionError('host execution'))
            host = ClaudeHostDriver(backend_factory=terminal)
            args = SimpleNamespace(output=root/'output',claude=Path('/frozen/cli'),setup_timeout=90,
                                   sleep_seconds=35,observe_seconds=80,collect_only=False,fixtures=False,keep_scratch=False)
            process = Mock()
            with patch('driver.tempfile.mkdtemp',return_value=str(scratch)), \
                    patch('driver.subprocess.Popen',return_value=process), \
                    patch('driver.subprocess.run',side_effect=AssertionError('injection executed')), \
                    patch('driver.wait_for',return_value=True), \
                    patch('pathlib.Path.home',side_effect=AssertionError('credential lookup')):
                result = orchestrator.run_cell(args,Cell('channel','idle','fresh','text','single'),
                                               root,root,'99.0.0',host_driver=host)
            self.assertEqual(result['status'],'FAIL')
            self.assertEqual(result['reasons'],['host version or mode has no verified control recipe'])
            self.assertIn('injection completed',result['not_evaluated'])
            self.assertIn('expected contract observed',result['not_evaluated'])
            terminal.assert_not_called()
            process.terminate.assert_called_once()

    def test_fresh_arrival_records_empty_transcript_and_rejects_eager_write(self):
        h, driver = self.prepare(session='fresh')
        transcript = h.scratch.root/'transcript.jsonl'
        transcript.write_bytes(b'')
        h.checkpoint('session')
        rows = dict(driver.observe()[0].reads)['state']['records']
        self.assertEqual(rows, [dict(action='session_sample',operation='new',host_session_ref='conversation',transcript_records=0)])
        transcript.write_text('{}\n')
        with self.assertRaises(DriverError) as caught:h.checkpoint('session')
        self.assertEqual(caught.exception.kind,'unrecognized_state')
        self.assertEqual(dict(driver.observe()[0].reads)['state']['records'], rows)

    def test_registration_imports_never_probe_or_launch(self):
        code = '''
from unittest.mock import patch
with patch('subprocess.run', side_effect=AssertionError('process execution')), \\
     patch('subprocess.Popen', side_effect=AssertionError('process execution')), \\
     patch('pathlib.Path.home', side_effect=AssertionError('credential lookup')):
    import host, hostdrivers, hostdrivers.claude_evidence, driver
    one = hostdrivers.registry().create('claude').describe('2.1.267', 'matrix')
    two = hostdrivers.registry().create('claude').describe('2.1.267', 'matrix')
    assert one == two
'''
        subprocess.run([sys.executable, '-c', code], cwd=Path(__file__).parent,
                       capture_output=True, text=True, check=True, timeout=30)

    def test_arbitrary_identifiers_preserve_policy_and_order(self):
        import driver as orchestrator
        profiles = []
        original = orchestrator.default_profile
        def record(*args, **kwargs):
            profile = original(*args, **kwargs)
            profiles.append(profile)
            return profile
        helper = support.OrchestratorControls()
        with patch('driver.default_profile', side_effect=record):
            first, _ = helper.scripted_run('arbitrary-one', Cell('channel','reconnect','resumed','text','single'))
            second, _ = helper.scripted_run('unrelated-two', Cell('channel','reconnect','resumed','text','single'))
        self.assertEqual(first, second)
        self.assertEqual([p.description['driver_id'] for p in profiles], ['arbitrary-one','unrelated-two'])
        for field in ('case','scenario','contract','workload','timeouts'):
            self.assertEqual(getattr(profiles[0],field), getattr(profiles[1],field), field)


if __name__=='__main__':
    unittest.main()
