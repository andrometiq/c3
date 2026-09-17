"""Characterize the pre-protocol Claude controls with synthetic dependencies."""
from contextlib import ExitStack
import json
from pathlib import Path
import shlex
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

import driver
from collect import build_verdict_inputs, collect
from host import Host
from matrix import Cell, cells, selection_summary

READY = 'Reply MATRIX_READY. Do not call any tools or fetch messages.'
FETCH = "Call c3 fetch_queue exactly once with limit='all' and ack=true. Then reply MATRIX_FETCHED. Do not repeat the fetch."
SYSTEM = ('This is a local delivery test. Acknowledge MATRIX_SAMPLE data with MATRIX_RECEIVED once locally. '
          'Never fetch C3 messages unless explicitly asked, and never send a channel reply. '
          'Run only the requested Python sleep commands.')


class BaselineControls(unittest.TestCase):
    def test_matrix_order_and_counts(self):
        names = [cell.name for cell in cells()]
        self.assertEqual(names[:3], ['channel-idle-fresh-text-single', 'channel-idle-fresh-text-double',
                                     'channel-idle-fresh-voice-single'])
        self.assertEqual(names[60], 'inbox-idle-fresh-text-single')
        self.assertEqual(names[-1], 'fetch-reconnect-resumed-photo-double')
        self.assertEqual(selection_summary(), 'Selected 180 cells: 126 feasible, 54 N/A; launches 216 Claude sessions (including resume seeds).')

    def test_literal_launch_and_isolation(self):
        for transport in ('channel', 'inbox', 'fetch'):
            with self.subTest(transport=transport), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                home = root / 'auth'
                home.mkdir()
                scratch = root / 'scratch'
                scratch.mkdir()
                with patch.dict('os.environ', {}, clear=True), patch('host.Path.home', return_value=home), \
                        patch('host.subprocess.run') as process:
                    host = Host(scratch, Path('/bin/claude'), Path('/bin/broker'), Path('/bin/adapter'),
                                Path('/scripts'), Cell(transport, 'idle', 'fresh', 'text', 'single'), '2.1.267')
                expected = ['/bin/claude', '--model', 'haiku', '--setting-sources', 'user', '--tools', 'Bash',
                            '--permission-mode', 'default', '--no-chrome', '--allowedTools', 'mcp__plugin_c3_c3__*',
                            'Bash(python3:*)', '--append-system-prompt', SYSTEM]
                if transport == 'channel':
                    expected += ['--dangerously-load-development-channels', 'plugin:c3@c3']
                self.assertEqual(host.command, expected)
                self.assertEqual([call.kwargs['timeout'] for call in process.call_args_list], [90, 90])
                self.assertEqual(process.call_args_list[1].args[0], ['/bin/claude', 'plugin', 'install', 'c3@c3', '--scope', 'user'])
                self.assertEqual(host.env['XDG_RUNTIME_DIR'], str(scratch / 'broker'))
                self.assertEqual(host.socket, scratch / 'tmux.sock')
                self.assertIn('implicit broker spawn refused', (scratch / 'shim/c3-broker').read_text())
                host.tmux = Mock()
                for resumed in (False, True):
                    host.launch(continued=resumed)
                    host.tmux.assert_called_with('new-session', '-d', '-s', 'matrix', '-x', '160', '-y', '50',
                                                 '-c', str(host.cwd), shlex.join(expected + (['--continue'] if resumed else [])))

    def test_submission_retries_enter_without_retyping(self):
        host = Host.__new__(Host)
        host.tmux = Mock()
        host.composer = Mock(side_effect=['prompt'] * 7 + [''])
        with patch('host.time.sleep') as sleep:
            host.send('prompt')
        self.assertEqual([call.args for call in host.tmux.call_args_list], [
            ('send-keys', '-t', 'matrix', '-l', '--', 'prompt'),
            ('send-keys', '-t', 'matrix', 'Enter'), ('send-keys', '-t', 'matrix', 'Enter')])
        self.assertEqual(sleep.call_count, 7)
        sleep.assert_called_with(0.25)

    def test_unsent_submission_is_bounded(self):
        host = Host.__new__(Host)
        host.tmux = Mock()
        host.composer = Mock(return_value='prompt')
        with patch('host.time.sleep') as sleep, self.assertRaisesRegex(TimeoutError, 'after 5 submissions'):
            host.send('prompt')
        self.assertEqual(host.tmux.call_count, 6)
        self.assertEqual(sleep.call_count, 30)

    def test_warmup_and_graceful_stop(self):
        host = Host.__new__(Host)
        host.send = Mock()
        host.wait_setup = Mock()
        host.tmux = Mock(return_value=SimpleNamespace(returncode=1))
        host.ready_turn()
        host.send.assert_called_once_with(READY)
        with patch('host.wait_for') as wait:
            host.stop_session()
        host.send.assert_called_with('/exit')
        self.assertEqual(wait.call_args.args[1:], (15, 'Claude did not exit'))

    def test_literal_workloads_require_invocation_and_marker(self):
        for background in (False, True):
            with self.subTest(background=background), tempfile.TemporaryDirectory() as directory:
                host = Host.__new__(Host)
                host.cwd = Path(directory)
                host.send = Mock()
                host.records = Mock(return_value=[])
                program = "import pathlib,time; pathlib.Path('tool-running').write_text('running'); time.sleep(35); pathlib.Path('tool-running').unlink()"
                command = 'python3 -c ' + shlex.quote(program)
                def wait(check, description):
                    self.assertFalse(check())
                    (host.cwd / 'tool-running').touch()
                    self.assertFalse(check())
                    host.records.return_value = [{'timestamp': '2099-01-01T00:00:00Z', 'message': {'content': [
                        {'type': 'tool_use', 'name': 'Bash', 'input': {'command': command, 'run_in_background': background}}]}}]
                    self.assertTrue(check())
                host.wait_setup = wait
                result = host.tool_state(background, 35)
                mode = 'true' if background else 'false'
                host.send.assert_called_once_with(f'Call Bash once with command {json.dumps(command)} and run_in_background={mode}. '
                                                  'Do not fetch or reply through C3. After the tool returns, reply MATRIX_WAITING and wait.')
                self.assertEqual((result['command'], result['background']), (command, background))

    def test_reconnect_labelled_menu_and_override(self):
        host = Host.__new__(Host)
        host.send = Mock()
        host.tmux = Mock()
        host.pane = Mock(side_effect=['Manage MCP servers', 'Manage MCP servers\n❯ unrelated\n c3',
                                      'Manage MCP servers\n❯ plugin:c3:c3', '  3. Reconnect'])
        host.wait_event = Mock(return_value={'pid': 42, 'time': 9})
        with patch('host.time.sleep'):
            self.assertEqual(host.reconnect(), {'pid': 42, 'time': 9})
        host.send.assert_called_once_with('/mcp')
        self.assertEqual([call.args[-1] for call in host.tmux.call_args_list], ['Down', 'Enter', 'Enter'])
        self.assertEqual(host.tmux.call_args.args[-2:], ('3', 'Enter'))
        host.pane = Mock(return_value='Manage MCP servers')
        host.tmux.reset_mock()
        with patch('host.time.sleep'):
            host.reconnect('Down,Enter,2,Escape')
        self.assertEqual([call.args[-1] for call in host.tmux.call_args_list], ['Down', 'Enter', '2', 'Escape'])
        with self.assertRaisesRegex(TimeoutError, 'unrecognized /mcp menu'):
            host.select_menu_row('c3')

    def test_incomplete_capture_judgment_and_duration_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = Cell('channel', 'idle', 'resumed', 'text', 'single')
            result = collect(cell, None, root, root / 'output', {'setup_errors': [], 'injected': True, 'rows_final': 0})
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['not_evaluated'], [])
            self.assertEqual(result['reasons'], [
                'injection was not completed', 'durable rows remain', 'no false Held assertion failed or missing',
                'route line count assertion failed or missing', 'no negotiated attempt observed',
                'source attempted more or less than once', 'receipt missing or outside 15-second window',
                'retirement count differs from injected source count', 'host did not receive each source exactly once',
                'observed delivery contract does not match expected contract',
                'requested host state was not proven at injection', 'requested session posture was not proven at injection',
                'injected source lacks its required final disposition', 'evidence collection incomplete'])
            for transport, duration in (('channel', 15000), ('fetch', 60000)):
                scenario, contract, _ = build_verdict_inputs(Cell(transport, 'idle', 'fresh', 'text', 'single'), {})
                self.assertEqual(scenario['observation_duration_ms'], duration)
                self.assertEqual(contract['timing']['live'], {'limit_ms': 15000, 'basis': 'terminal_confirmation'})


class RunCellBaseline(unittest.TestCase):
    def run_fake(self, cell, *, failure=None, observe_seconds=80, setup_error=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        scratch = root / 'scratch'
        scratch.mkdir()
        calls = []
        events = []
        now = [100.0]
        class Backend:
            def __init__(self, *args):
                calls.append('prepare')
                if setup_error is not None:
                    raise setup_error
                self.root = scratch
                self.control, self.cwd = scratch / 'control', scratch / 'cwd'
                self.control.mkdir(exist_ok=True)
                self.cwd.mkdir()
            def launch(self, continued=False):
                calls.append('resume' if continued else 'launch')
                with (self.control / 'sessions.jsonl').open('a') as output:
                    output.write(json.dumps(dict(time=now[0], session_id='conversation', transcript_path=str(scratch / 'transcript.jsonl'))) + '\n')
                events.append(dict(event='proxy_started', pid=2, time=now[0], inbox_env=True))
            def wait_event(self, name, after=0):
                calls.append(name)
                event = dict(event=name, pid=2, time=now[0], frame={'result': {'content': []}})
                events.append(event)
                return event
            def ready_turn(self): calls.append('warmup')
            def stop_session(self): calls.append('exit')
            def reconnect(self, keys):
                calls.append(('reconnect', keys))
                events.extend([dict(event='proxy_started', pid=3, time=now[0]),
                               dict(event='initialized_waiting', pid=3, time=now[0])])
                return events[-1]
            def transcript(self):
                calls.append('transcript')
                return None
            def events(self, name=None, after=0):
                return [event for event in events if event['time'] >= after and (name is None or event['event'] == name)]
            def tool_state(self, background, seconds):
                calls.append(('tool', background, seconds))
                (self.cwd / 'tool-running').touch()
                self.background = background
                return {'command': 'synthetic', 'background': background, 'observed': now[0]}
            def records(self):
                return [{'message': {'content': [{'type': 'tool_use', 'name': 'Bash', 'input': {
                    'command': 'synthetic', 'run_in_background': self.background}}]}}]
            def send(self, text):
                calls.append(('send', text))
                self.submission_composer_before = text
                self.submission_composer = ''
            def pane(self):
                calls.append('pane')
                if failure:
                    raise RuntimeError(failure)
                return '────\n❯\n────'
            def close(self): calls.append('close')
        args = SimpleNamespace(output=root / 'output', claude=Path('/bin/claude'), setup_timeout=90,
                               collect_only=False, fixtures=False, keep_scratch=False, reconnect_keys='Down,Enter',
                               sleep_seconds=35, observe_seconds=observe_seconds)
        process = Mock()
        process.terminate.side_effect = lambda: calls.append('broker-stop')
        process.wait.side_effect = lambda **kw: calls.append(('broker-wait', kw))
        def inject(command, **kwargs):
            calls.append(('inject', command[4:], kwargs))
            return SimpleNamespace(stdout=json.dumps({'accepted': True, 'message_ids': [1] * cell.count}))
        def capture(cell, host, root, output, evidence, collect_only):
            calls.append('collect')
            return dict(status='FAIL' if failure else 'PASS', evidence=dict(evidence))
        def wait(check, timeout, description):
            calls.append(('wait', timeout, description))
            return True
        touch = Path.touch
        def touched(path, *args, **kwargs):
            calls.append(path.name)
            return touch(path, *args, **kwargs)
        with ExitStack() as stack:
            for target, value in {
                'driver.tempfile.mkdtemp': lambda **kw: str(scratch), 'driver.Host': Backend,
                'driver.subprocess.Popen': lambda *a, **kw: process, 'driver.subprocess.run': inject,
                'driver.wait_for': wait, 'hostdrivers.claude.wait_for': wait, 'driver.queue_rows': lambda root: [{}] * cell.count,
                'driver.collect': capture, 'driver.time.time': lambda: now[0],
                'driver.time.monotonic': lambda: now[0], 'driver.time.sleep': lambda seconds: now.__setitem__(0, now[0] + seconds),
                'driver.save_failure_evidence': lambda *a: calls.append('failure-export'),
                'driver.shutil.rmtree': lambda *a: calls.append('remove'),
                'pathlib.Path.touch': touched,
            }.items():
                stack.enter_context(patch(target, value))
            result = driver.run_cell(args, cell, root, root, '2.1.267')
        return calls, result, now[0]

    def test_fresh_startup_gate_and_observation_before_cleanup(self):
        calls, result, elapsed = self.run_fake(Cell('channel', 'startup', 'fresh', 'voice', 'double'))
        for left, right in [('gate-initialized', 'launch'), ('launch', 'initialized_waiting'),
                            ('initialized_waiting', 'transcript'), ('release-initialized-2', 'attached'),
                            ('attached', 'warmup'), ('warmup', 'pane'), ('pane', 'collect'),
                            ('collect', 'close'), ('close', 'broker-stop'), ('failure-export', 'remove')]:
            if left in calls:
                self.assertLess(calls.index(left), calls.index(right))
        injection = next(call for call in calls if isinstance(call, tuple) and call[0] == 'inject')
        self.assertEqual(injection[1], ['--topic', '42', '--text', 'MATRIX_SAMPLE: generic delivery sample.', '--count', '2', '--voice', '--voice-delay-ms', '2000'])
        self.assertEqual(injection[2]['timeout'], 10)
        self.assertLess(calls.index(injection), calls.index('release-initialized-2'))
        self.assertGreaterEqual(calls.count('pane'), 400)
        self.assertGreaterEqual(elapsed, 180.3)
        self.assertEqual(result['evidence']['rows_before_ready'], 2)
        self.assertIn(('broker-wait', {'timeout': 15}), calls)

    def test_resumed_reconnect_seed_order(self):
        calls, _, _ = self.run_fake(Cell('channel', 'reconnect', 'resumed', 'text', 'double'))
        relevant = [call for call in calls if call in ('launch', 'attached', 'warmup', 'exit', 'resume', 'gate-initialized',
                                                       'release-initialized-3', ('reconnect', 'Down,Enter'))]
        self.assertEqual(relevant, ['launch', 'attached', 'warmup', 'exit', 'resume', 'attached', 'warmup',
                                    'gate-initialized', ('reconnect', 'Down,Enter'), 'release-initialized-3', 'attached'])

    def test_foreground_fetch_waits_and_holds_response(self):
        calls, result, _ = self.run_fake(Cell('fetch', 'foreground', 'resumed', 'text', 'single'))
        sequence = [('tool', False, 35), 'gate-fetch',
                    ('wait', 40, 'foreground tool did not finish'), ('send', FETCH), 'fetch_result_waiting', 'release-fetch', 'pane', 'collect']
        self.assertEqual(sorted(sequence, key=calls.index), sequence)
        self.assertEqual(result['evidence']['rows_while_fetch_result_held'], 1)
        self.assertEqual(result['evidence']['fetch_result'], {'content': []})

    def test_observation_failure_collects_before_cleanup(self):
        calls, result, _ = self.run_fake(Cell('inbox', 'idle', 'fresh', 'photo', 'single'), failure='host exited during observation')
        self.assertEqual(result['evidence']['run_errors'], ['RuntimeError: host exited during observation'])
        self.assertEqual(calls[-6:], ['collect', 'close', 'broker-stop', ('broker-wait', {'timeout': 15}), 'failure-export', 'remove'])

    def test_setup_json_error_keeps_ordered_reason_type(self):
        error = json.JSONDecodeError('synthetic malformed settings', '{', 1)
        _, result, _ = self.run_fake(cells()[0], setup_error=error)
        self.assertEqual(result['evidence']['setup_errors'], [f'JSONDecodeError: {error}'])

    def test_collision_refuses_before_process_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scratch = root / 'scratch'
            scratch.mkdir()
            cell = cells()[0]
            (root / 'output/version' / cell.name).mkdir(parents=True)
            with patch('driver.tempfile.mkdtemp', return_value=str(scratch)), patch('driver.subprocess.Popen') as process:
                with self.assertRaisesRegex(FileExistsError, 'refusing to overwrite'):
                    driver.run_cell(SimpleNamespace(output=root / 'output'), cell, root, root, 'version')
            process.assert_not_called()
