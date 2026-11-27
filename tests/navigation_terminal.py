"""q exits outside input fields; Escape closes the visible layer and never exits."""
import http.server
import sqlite3
import threading
import os
import pty
import select
import shlex
import struct
import subprocess
import termios
import fcntl
from pathlib import Path
import tempfile
import time
from bookmarks_terminal import Terminal, VRSC, daemon
from defi_terminal import wait
from reorg_terminal import Daemon, A2, TXID

ESC = b'\x1b'
TX = b'/' + TXID.encode() + b'\r'


def dispose(terminal):
    if terminal.process.poll() is None:
        terminal.process.kill()
        terminal.process.wait(timeout=5)
    os.close(terminal.master)


def assert_quits(terminal):
    terminal.key(b'q', .1)
    terminal.process.wait(timeout=5)
    assert terminal.process.returncode == 0
    assert 'Shutting down' in terminal.read(.1), 'Quit did not display a shutdown notice'
    assert b'\x1b[?1006l' in terminal.captured, 'Quit did not release terminal mouse mode'
    assert b'\x1b[?2004l' in terminal.captured, 'Quit did not release bracketed paste'
    assert b'\x1b[23;0t' in terminal.captured, 'Quit did not request title restoration'


def run_shutdown():
    server = daemon(VRSC)
    balance_requested = threading.Event()
    release_balance = threading.Event()

    def balance(_):
        balance_requested.set()
        release_balance.wait(5)
        return dict(balance=0, received=0)

    server.rpc_handlers = {
        'getidentity': lambda _: dict(
            identity=dict(identityaddress=VRSC, name='VRSC', version=1, flags=1),
            fullyqualifiedname='VRSC@', blockheight=0, status='active',
        ),
        'getaddresstxids': lambda _: [],
        'getaddressbalance': balance,
    }
    try:
        with tempfile.TemporaryDirectory(prefix='carbide-shutdown-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            terminal = Terminal(root, conf)
            try:
                wait(terminal, 'Recent Blocks')
                cache = root / 'cache/verus-carbide/mainnet.sqlite3'
                terminal.key(b'/VRSC@\r', .25)
                wait(terminal, 'Primary addresses:')
                assert balance_requested.wait(5), 'Identity balance request did not reach the daemon'
                with sqlite3.connect(cache) as writer:
                    writer.execute('BEGIN IMMEDIATE')
                    release_balance.set()
                    terminal.read(.25)
                    assert "identity VRSC@ at height" in terminal.key(b'\t', .25)
                    assert "Primary addresses:" in terminal.key(b'\t', .25)
                    assert "Enter submit search" in terminal.key(b'/', .25)
                    assert "/ orderedq" in terminal.key(b'orderedq', .25)
                    assert "Primary addresses:" in terminal.key(ESC, .25)
                    assert "Loading currency data" in terminal.key(b'c', .25)
                    assert "Primary addresses:" in terminal.key(ESC, .25)
                    screen = terminal.key(b'q', .25)
                    assert 'Shutting down' in screen, screen
                    assert 'background work' in screen, screen
                    assert 'Primary addresses:' not in screen, 'Shutdown redrew the identity view'
                    assert terminal.process.poll() is None, 'The cache write was not held open'
                    assert b'\x1b[?1049l' not in terminal.captured, 'Terminal restored during cleanup'
                terminal.process.wait(timeout=5)
                screen = terminal.read(.1)
                assert terminal.process.returncode == 0
                assert 'Closing the local cache' in screen, screen
                assert b'\x1b[?1049l' in terminal.captured
            finally:
                dispose(terminal)
    finally:
        release_balance.set()
        server.shutdown()
        server.server_close()



def run_tmux():
    server = daemon(VRSC)
    server.rpc_handlers = {
        'getidentity': lambda _: dict(
            identity=dict(identityaddress=VRSC, name='VRSC', version=1, flags=1),
            fullyqualifiedname='VRSC@', blockheight=0, status='active',
        ),
        'getaddresstxids': lambda _: [],
        'getaddressbalance': lambda _: dict(balance=0, received=0),
    }
    try:
        with tempfile.TemporaryDirectory(prefix='carbide-tmux-input-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            trace = root / 'ui-timing.log'
            env = {key: value for key, value in os.environ.items() if not key.startswith('TMUX')}
            env.update(TERM='xterm-256color', HOME=str(root),
                       XDG_CONFIG_HOME=str(root / 'config'), XDG_CACHE_HOME=str(root / 'cache'),
                       XDG_DATA_HOME=str(root / 'data'), VCARBIDE_UI_TIMING=str(trace))
            tmux = ['tmux', '-S', str(root / 'socket'), '-f', '/dev/null']

            def command(*args):
                return subprocess.check_output([*tmux, *args], env=env, text=True, timeout=5).strip()

            pane = command('new-session', '-d', '-s', 'carbide', '-x', '130', '-y', '36',
                           '-P', '-F', '#{pane_id}', shlex.join([
                               str(Path('target/debug/vcarbide').resolve()), '--conf', str(conf),
                               '--poll-interval', '1', '--emoji', 'off',
                           ]))
            master, slave = pty.openpty()
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 36, 130, 0, 0))
            client = subprocess.Popen([*tmux, 'attach-session', '-t', 'carbide'],
                                      stdin=slave, stdout=slave, stderr=slave, env=env)
            os.close(slave)
            try:
                command('set-option', '-g', 'focus-events', 'on')
                other = command('split-window', '-h', '-d', '-p', '25', '-t', pane,
                                '-P', '-F', '#{pane_id}', 'cat')

                def await_screen(needle, seconds=4):
                    deadline = time.monotonic() + seconds
                    while True:
                        if select.select([master], [], [], 0)[0]:
                            os.read(master, 65536)
                        screen = command('capture-pane', '-p', '-t', pane)
                        if needle in screen:
                            return
                        assert time.monotonic() < deadline, screen
                        time.sleep(.015)

                await_screen('Recent Blocks')
                command('send-keys', '-t', pane, '-l', '/VRSC@')
                command('send-keys', '-t', pane, 'Enter')
                await_screen('Primary addresses:')
                before = server.calls.count('getblockchaininfo')
                for _ in range(3):
                    command('select-pane', '-t', other)
                    time.sleep(1.05)
                    command('select-pane', '-t', pane)
                    command('send-keys', '-t', pane, 'Tab')
                    await_screen('identity VRSC@ at height', .25)
                    command('send-keys', '-t', pane, 'Tab')
                    await_screen('Primary addresses:', .25)
                assert server.calls.count('getblockchaininfo') > before
                command('send-keys', '-t', pane, '-l', '/orderedq')
                await_screen('/ orderedq', .25)
                command('send-keys', '-t', pane, 'Escape')
                await_screen('Search block height', .25)
                samples = [line.split() for line in trace.read_text().splitlines()]
                assert any(stage == 'input_dispatch' for stage, _ in samples)
                assert all(stage in {'input_dispatch', 'input_queue', 'ui_update', 'redraw',
                                     'terminal_read', 'terminal_wait', 'background_event',
                                     'reconciliation', 'transaction_update', 'daemon_observation',
                                     'identity_update'} and duration.endswith('us')
                           and duration[:-2].isdigit() for stage, duration in samples)
                command('send-keys', '-t', pane, 'q')
                deadline = time.monotonic() + 4
                while pane in command('list-panes', '-F', '#{pane_id}').splitlines():
                    assert time.monotonic() < deadline, 'Carbide did not quit after tmux focus changes'
                    time.sleep(.02)
            finally:
                command('kill-server')
                client.wait(timeout=5)
                os.close(master)
    finally:
        server.shutdown()
        server.server_close()
    print('tmux pane switching, repeated refresh, ordered input and timing checks passed.')


def run_job_control():
    with tempfile.TemporaryDirectory(prefix='carbide-job-control-') as directory:
        root = Path(directory)
        conf = root / 'VRSC.conf'
        conf.write_text('rpcconnect=127.0.0.1\nrpcport=9\n')
        env = os.environ.copy()
        env.update(TERM='xterm-256color', HOME=str(root), PS1='carbide-test-shell> ',
                   XDG_CONFIG_HOME=str(root / 'config'), XDG_CACHE_HOME=str(root / 'cache'))
        shell, master = pty.fork()
        if shell == 0:
            os.execvpe('bash', ['bash', '--noprofile', '--norc', '-i'], env)
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack('HHHH', 34, 140, 0, 0))
        captured = bytearray()

        def until(needle, since=0, limit=5):
            deadline = time.monotonic() + limit
            while needle not in captured[since:] and time.monotonic() < deadline:
                if select.select([master], [], [], .05)[0]:
                    try:
                        captured.extend(os.read(master, 65536))
                    except OSError:
                        break
            assert needle in captured[since:], bytes(captured[-1000:])

        def send(keys):
            mark = len(captured)
            os.write(master, keys)
            return mark

        try:
            until(b'carbide-test-shell> ')
            command = shlex.join(['target/debug/vcarbide', '--conf', str(conf), '--emoji', 'off'])
            until(b'Recent Blocks', send(command.encode() + b'\n'))
            # With no paste end, normal key events cannot reach the dispatcher.
            send(b'\x1b[200~q/')
            time.sleep(.15)
            mark = send(b'\x1a')
            until(b'Stopped', mark, 2)
            until(b'carbide-test-shell> ', mark, 2)
            assert b'\x1b[?1049l' in captured[mark:]
            mark = send(b'fg\n')
            # The parser may still be waiting for the paste end after fg.
            time.sleep(.15)
            send(b'\x1b[201~')
            until(b'\x1b[?1049h', mark, 2)
            mark = send(b'q')
            until(b'Shutting down', mark, 2)
            until(b'carbide-test-shell> ', mark)
            send(b'exit\n')
        finally:
            os.close(master)
            os.waitpid(shell, 0)
    print('Ctrl-Z reached the shell during a stalled input parser, and fg restored Carbide.')

def run_dashboard():
    server = daemon(VRSC)
    first, earlier, older = 'd' * 64, 'e' * 64, 'f' * 64
    tip_height = 14
    reported_tip = tip_height
    release_old_blocks = threading.Event()
    blocks = {
        height: dict(height=height, hash=f'{height + 1:064x}', time=1700000000 + height * 60,
                     tx=[], size=200)
        for height in range(tip_height + 3)
    }
    for height in range(1, tip_height + 3):
        blocks[height]['previousblockhash'] = blocks[height - 1]['hash']
    blocks[tip_height].update(tx=[earlier, first], size=850)
    blocks[tip_height - 1].update(tx=[older], size=500)
    by_hash = {block['hash']: block for block in blocks.values()}
    by_txid = {txid: block for block in blocks.values() for txid in block['tx']}

    def block(args):
        key = args[0]
        return by_hash[key] if key in by_hash else blocks[int(key)]

    def block_with_delayed_history(args):
        value = block(args)
        if value['height'] < 3:
            release_old_blocks.wait(timeout=10)
        return value

    def transaction(args):
        txid = args[0]
        block = by_txid[txid]
        return dict(txid=txid, vin=[dict(coinbase='00')], vout=[], size=105,
                    blockhash=block['hash'], height=block['height'], blocktime=block['time'])

    server.rpc_handlers = {
        'getinfo': lambda _: dict(chainid=VRSC, testnet=False, blocks=reported_tip, longestchain=reported_tip, connections=1),
        'getblockchaininfo': lambda _: dict(blocks=reported_tip, bestblockhash=blocks[reported_tip]['hash']),
        'getblockhash': lambda args: blocks[int(args[0])]['hash'],
        'getblock': block_with_delayed_history,
        'getblockheader': block,
        'getrawtransaction': transaction,
    }
    try:
        with tempfile.TemporaryDirectory(prefix='explorer-dashboard-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            terminal = Terminal(root, conf, poll_interval=1)
            try:
                wait(terminal, 'ffffff...ffffff')
                screen = terminal.screen()
                heading = next(line for line in screen.splitlines() if 'Recent Blocks' in line)
                assert heading.index('Recent Blocks') < heading.index('Recent Transactions'), screen
                midpoint = len(heading) // 2
                assert heading.index('Recent Transactions') == midpoint + 2, screen
                assert heading[midpoint - 1:midpoint + 2] == '   ', screen
                tx_rows = [line for line in screen.splitlines() if 'txid ' in line]
                assert all(line[midpoint - 1:midpoint + 2] == '   ' for line in tx_rows), screen
                expected = ['dddddd...dddddd', 'eeeeee...eeeeee', 'ffffff...ffffff']
                assert len(tx_rows) == len(expected) and all(
                    short in line for short, line in zip(expected, tx_rows)
                ), screen
                assert '850 bytes' in screen and f'Block {tip_height}' in screen, screen
                terminal.key(b'\rl\x1b[C')
                screen = terminal.screen()
                assert '> Stream Blocks' in screen and 'Block Header' not in screen, screen
                reported_tip += 1
                wait(terminal, f'height {reported_tip}')
                lines = terminal.screen().splitlines()
                stream_row = next(index for index, line in enumerate(lines) if '> Stream Blocks' in line)
                assert f'height {reported_tip}' in lines[stream_row + 1], terminal.screen()
                terminal.key(b'\t')
                terminal.resize(65, 26)
                wait(terminal, 'Recent Transactions')
                assert 'Recent Blocks' not in terminal.screen()
                terminal.resize(160, 40)
                wait(terminal, 'Recent Blocks')
                terminal.key(b'\r')
                wait(terminal, 'Transaction Overview')
                wait(terminal, first)
                terminal.key(ESC)
                terminal.key(b'j\r')
                wait(terminal, 'Transaction Overview')
                wait(terminal, earlier)
                terminal.key(ESC)
                terminal.key(b'\x1b[Zj')
                selected_height = reported_tip
                reported_tip += 1
                wait(terminal, f'height {reported_tip}')
                terminal.key(b'\r')
                wait(terminal, 'Block Header')
                wait(terminal, f'Height: {selected_height}')
                terminal.key(ESC)
                terminal.key(b'\x1b[H')
                wait(terminal, '> Stream Blocks')
                terminal.key(b'\x1b[F')
                wait(terminal, 'loading older blocks...')
                frames = set()
                for _ in range(5):
                    screen = terminal.read(.13)
                    lines = screen.splitlines()
                    loading_row = next(index for index, line in enumerate(lines) if 'loading older blocks...' in line)
                    left_width = len(lines[loading_row]) // 2
                    assert loading_row > max(index for index, line in enumerate(lines) if 'height ' in line[:left_width])
                    frames.add(lines[loading_row].lstrip()[0])
                assert frames <= set('|/-\\') and len(frames) > 1, frames
                release_old_blocks.set()
                wait(terminal, 'genesis reached')
                deadline = time.monotonic() + 3
                screen = terminal.read(.1)
                while 'loading older blocks...' in screen and time.monotonic() < deadline:
                    screen = terminal.read(.1)
                assert 'loading older blocks...' not in screen, screen
                assert_quits(terminal)
            finally:
                dispose(terminal)
    finally:
        release_old_blocks.set()
        server.shutdown()
        server.server_close()


def run_mempool():
    server = daemon(VRSC)
    initial, newcomer = 'e' * 64, 'f' * 64
    entries = {initial: dict(size=450, fee=0.0002, time=1700000000, height=9, depends=[])}

    def transaction(txid):
        return dict(txid=txid, version=4, vin=[dict(coinbase='00')], vout=[], size=450)

    def mempool(params):
        return {
            txid: dict(entry, fulltx=transaction(txid)) if len(params) == 2 else entry
            for txid, entry in entries.copy().items()
        }

    server.rpc_handlers = {
        'getrawmempool': mempool,
        'getrawtransaction': lambda args: dict(transaction(args[0]), hex='aabbccddeeff'),
    }
    try:
        with tempfile.TemporaryDirectory(prefix='explorer-mempool-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            terminal = Terminal(root, conf)
            try:
                wait(terminal, 'Recent Blocks')
                terminal.key(b'm')
                wait(terminal, 'Coinbase')
                wait(terminal, 'fee 0.00020000')
                wait(terminal, initial)
                terminal.resize(60, 30)
                wait(terminal, 'Mempool')
                assert initial not in terminal.screen()
                assert 'eeeeee...' in terminal.screen()
                assert ' ago' in next(row for row in terminal.screen().splitlines() if 'eeeeee...' in row)
                terminal.resize(160, 40)
                wait(terminal, initial)
                bulk = ('getrawmempool', [True, {'fulltxes': True}])
                assert bulk in server.requests
                assert ('getrawtransaction', [initial, 1]) not in server.requests
                entries[newcomer] = dict(size=550, fee=0.0003, time=1700000001, height=9, depends=[initial])
                wait(terminal, '1 unconfirmed parent')
                assert ('getrawmempool', [True]) in server.requests
                assert 'Coinbase' in terminal.screen(), terminal.screen()
                assert ('getrawtransaction', [initial, 1]) not in server.requests
                assert server.requests.count(bulk) == 1
                terminal.key(b'\r')
                wait(terminal, 'Transaction Overview')
                assert ('getrawtransaction', [initial, 1]) in server.requests
                assert ('getrawtransaction', [newcomer, 1]) in server.requests
                terminal.key(b'\t')
                wait(terminal, 'aabbccddeeff')
                assert_quits(terminal)
            finally:
                dispose(terminal)
    finally:
        server.shutdown()
        server.server_close()


def run_peers():
    server = daemon(VRSC)
    address = '[2001:db8::1]:27486'
    bans = [dict(address='192.0.2.1/32')]

    def clear_bans(_):
        bans.clear()

    def ban(args):
        bans.append(dict(address=args[0]))

    server.rpc_handlers = {
        'getpeerinfo': lambda _: [dict(id=7, addr=address, version=170010, pingtime=0.012)],
        'listbanned': lambda _: list(bans),
        'getnettotals': lambda _: dict(totalbytesrecv=12345, totalbytessent=67890, timemillis=1700000000123),
        'addnode': lambda _: None,
        'clearbanned': clear_bans,
        'ping': lambda _: None,
        'disconnectnode': lambda _: None,
        'setban': ban,
    }
    try:
        with tempfile.TemporaryDirectory(prefix='explorer-peer-controls-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            terminal = Terminal(root, conf)
            try:
                wait(terminal, 'Recent Blocks')
                assert 'getnettotals' not in server.calls
                terminal.key(b'`')
                wait(terminal, '1 connected · 1 banned')
                wait(terminal, 'Network totals snapshot')
                screen = terminal.screen()
                snapshot_border = next(line for line in screen.splitlines() if 'Network totals snapshot' in line)
                assert snapshot_border.index('┐') - snapshot_border.index('┌') + 1 == 52, screen
                wait(terminal, '12345 bytes')
                wait(terminal, '67890 bytes')
                terminal.key(b'a')
                wait(terminal, 'Add node')
                terminal.key(b'\r')
                wait(terminal, 'Enter one node address')
                terminal.key(b'\x1b[200~quartz.example:27486\x1b[201~\r')
                wait(terminal, "Added quartz.example:27486 to the daemon's node list")
                assert ('addnode', ['quartz.example:27486', 'add']) in server.requests
                terminal.key(b'B')
                assert 'Clear all bans on this daemon?' not in terminal.screen()
                terminal.key(b'c')
                wait(terminal, 'Clear all bans on this daemon?')
                terminal.key(ESC)
                assert 'clearbanned' not in server.calls
                terminal.key(b'cy')
                wait(terminal, '1 connected · 0 banned')
                wait(terminal, 'p ping all')
                terminal.key(b'p')
                wait(terminal, 'Ping queued for all connected peers')
                assert ('ping', []) in server.requests
                terminal.key(b'\rp')
                wait(terminal, 'Protocol version:')
                wait(terminal, 'Traffic and latency')
                wait(terminal, 'Last ping:')
                wait(terminal, '12ms')
                log = root / 'debug.log'
                wait(terminal, f'Could not read {log}')
                wait(terminal, 'No such file or directory')
                log.write_text('unrelated log entry\n')
                wait(terminal, 'No recent debug.log lines contain')
                with log.open('a') as handle:
                    handle.write(f'connected to {address}\n')
                wait(terminal, f'connected to {address}')
                log.unlink()
                wait(terminal, 'Previously read entries')
                wait(terminal, f'Could not read {log}')
                assert 'p ping all' not in terminal.screen(), terminal.screen()
                assert server.calls.count('ping') == 1, server.calls
                terminal.key(b'x')
                wait(terminal, f'Disconnect {address}?')
                terminal.key(b'y')
                wait(terminal, 'Disconnect requested')
                assert ('disconnectnode', [address]) in server.requests
                terminal.key(b'\tb')
                wait(terminal, "Ban 2001:db8::1 for the daemon's default duration?")
                terminal.key(b'yh')
                wait(terminal, '1 connected · 1 banned')
                assert ('setban', ['2001:db8::1', 'add']) in server.requests
                assert server.calls.count('getnettotals') == 1, server.calls
                assert_quits(terminal)
            finally:
                dispose(terminal)
    finally:
        server.shutdown()
        server.server_close()


def run():
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Daemon)
    server.tip, server.missing, server.flip_after_tip = A2['hash'], None, None
    server.calls = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    cases = [
        ('dashboard', []),
        ('sources', [(b'\x1bOQ', 'Sources')]),
        ('source activity', [(b'\x1bOQ', 'Sources'), (b'a', 'Source activity')]),
        ('relationships', [(TX, 'Transaction Overview'), (b'\x1bOR', 'Relationships')]),
        ('bookmarks', [(b'\x1b[17~', 'Bookmarks')]),
        ('help', [(b'\x1bOP', 'Verus Carbide help')]),
        ('peers', [(b'`', 'Peers')]),
        ('option filters', [(b'cF', 'Currency option flags')]),
        ('launch filters', [(b'cF\t', 'Currency launch status')]),
        ('cache confirmation', [(b'?', 'Verus Carbide help'), (b'c', 'Clear Cache')]),
        ('witnesses', [(TX, 'Transaction Overview'), (b'\x1bOS', 'Witness investigation')]),
        ('inclusions', [(TX, 'Transaction Overview'), (b'\x1b[18~', 'Transaction inclusion history')]),
        ('DeFi', [(TX, 'Transaction Overview'), (b'\x1b[19~', 'DeFi trail')]),
    ]
    try:
        for label, steps in cases:
            with tempfile.TemporaryDirectory(prefix='explorer-quit-') as directory:
                root = Path(directory)
                conf = root/'VRSC.conf'
                conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\nrpcuser=test\nrpcpassword=test\n')
                terminal = Terminal(root, conf)
                try:
                    wait(terminal, 'tip 2/2')
                    assert 'Verus Carbide' in terminal.screen().splitlines()[0]
                    assert b'\x1b[22;0t\x1b]0;Verus Carbide\x07' in terminal.captured
                    for keys, needle in steps:
                        terminal.key(keys, .25)
                        wait(terminal, needle)
                    for width, height in [(76, 24), (44, 13), (12, 4), (160, 40)]:
                        terminal.resize(width, height)
                        assert terminal.process.poll() is None, f'{label} exited at {width}x{height}'
                    wait(terminal, steps[-1][1] if steps else 'Recent Blocks')
                    assert_quits(terminal)
                except Exception as error:
                    raise AssertionError(f'q failed in {label}:\n{terminal.screen()}') from error
                finally:
                    dispose(terminal)

        with tempfile.TemporaryDirectory(prefix='explorer-escape-') as directory:
            root = Path(directory)
            conf = root/'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\nrpcuser=test\nrpcpassword=test\n')
            terminal = Terminal(root, conf)
            try:
                wait(terminal, 'Recent Blocks')
                for _ in range(3):
                    terminal.key(ESC, .15)
                    assert terminal.process.poll() is None, 'Escape at Dashboard exited'
                terminal.key(b'/1\r', .3)
                wait(terminal, 'Height: 1 ')
                terminal.key(b'\t', .15)
                terminal.key(ESC, .2)
                wait(terminal, 'Recent Blocks')
                assert terminal.process.poll() is None, 'Escape at a JSON navigation root exited'

                terminal.key(b'/q', .2)
                assert ' / q' in terminal.screen(), terminal.screen()
                assert terminal.process.poll() is None
                terminal.key(ESC, .2)

                terminal.key(b'cF', .2)
                wait(terminal, 'Currency option flags')
                terminal.key(ESC, .2)
                assert 'Currency option flags' not in terminal.screen(), terminal.screen()
                assert 'Currencies' in terminal.screen(), terminal.screen()
                terminal.key(b'fq', .15)
                assert 'Filter: q_' in terminal.screen(), terminal.screen()
                terminal.key(ESC, .2)
                assert terminal.process.poll() is None
                terminal.key(ESC, .2)
                wait(terminal, 'Recent Blocks')

                terminal.key(b'/' + VRSC.encode() + b'@@\r', .4)
                wait(terminal, 'Currency VRSC')
                terminal.key(b'gq', .2)
                wait(terminal, 'Historical height / date:')
                assert terminal.process.poll() is None, 'q in numeric entry exited'
                terminal.key(ESC, .2)
                assert 'Historical height / date:' not in terminal.screen(), terminal.screen()
                assert 'Currency VRSC' in terminal.screen(), terminal.screen()
                terminal.key(b'/1\r', .2)
                wait(terminal, 'Height: 1 ')
                terminal.key(b'\ts', .2)
                wait(terminal, 'Save JSON')
                terminal.key(b'q', .2)
                assert 'filename q.json' in terminal.screen(), terminal.screen()
                assert terminal.process.poll() is None
                terminal.key(ESC, .2)
                terminal.key(b'\x1b[17~', .2)
                wait(terminal, 'Bookmarks')
                terminal.key(b'nq', .2)
                wait(terminal, 'Add bookmark')
                assert terminal.process.poll() is None
                terminal.key(ESC, .2)
                assert 'Bookmarks' in terminal.screen(), terminal.screen()
                terminal.key(ESC, .2)
                assert 'Bookmarks' not in terminal.screen(), terminal.screen()
                assert_quits(terminal)
            finally:
                dispose(terminal)
    finally:
        server.shutdown()
        server.server_close()
    print('Navigation PTY checks passed: q exits 13 interface contexts, input q stays text, Escape closes one layer and keeps roots open, terminal modes restored.')


def run_token_control():
    # A synthetic identity and NFT launch, followed by more spends than one pass.
    from hashlib import sha256

    payload = bytes([102]) + bytes([239]) * 20
    checksum = sha256(sha256(payload).digest()).digest()[:4]
    number = int.from_bytes(payload + checksum, 'big')
    alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    currency = ''
    while number:
        number, digit = divmod(number, 58)
        currency = alphabet[digit] + currency
    definition = dict(currencyid=currency, name='FixtureToken', options=0x820,
                      definitiontxid='d' * 64, definitiontxout=0, startblock=13)
    import_data = dict(importcurrencyid=currency, numoutputs=1)
    transaction_ids = [format(i, '064x') for i in range(1, 19)]
    points = [(txid, 2 if i == 0 else 0) for i, txid in enumerate(transaction_ids)]
    tip = (transaction_ids[-1], 0)
    token_script = dict(reserveoutput=dict(currencyvalues={currency: 0.00000001}),
                        reqSigs=1, addresses=[VRSC], hex='cc01')
    transactions = {
        definition['definitiontxid']: dict(
            txid=definition['definitiontxid'], vin=[], height=12,
            vout=[dict(n=0, scriptPubKey=dict(currencydefinition=definition))],
        ),
    }
    for i, (txid, output_number) in enumerate(points):
        output = dict(n=output_number, scriptPubKey=token_script)
        if i + 1 < len(points) and i % 2 == 0:
            output.update(spentTxId=points[i + 1][0], spentIndex=0)
        if i == 0:
            vout = [dict(n=0, scriptPubKey=dict(crosschainimport=import_data)),
                    dict(n=1, scriptPubKey=dict(acceptednotarization={})), output]
            vin = []
        else:
            vout = [output]
            vin = [dict(txid=points[i - 1][0], vout=points[i - 1][1])]
        transactions[txid] = dict(txid=txid, vin=vin, vout=vout)

    def identity(params):
        fixture = params[0] in ('FixtureToken@', 'FixtureToken', currency)
        name, address, flags = ('FixtureToken', currency, 5) if fixture else ('VRSC', VRSC, 0)
        return dict(identity=dict(identityaddress=address, name=name, version=3, flags=flags),
                    fullyqualifiedname=name + '@', blockheight=0, status='active')

    def spender(params):
        point = (params[0]['txid'], params[0]['index'])
        return dict(txid=points[points.index(point) + 1][0], index=0)

    launch = dict(importtxid=points[0][0], importvout=0, importheight=13,
                  **{'import': import_data},
                  importnotarization=dict(launchcleared=True, launchconfirmed=True,
                                          currencystate=dict(currencyid=currency, flags=24)))
    server = daemon(VRSC)
    server.rpc_handlers = {
        'getidentity': identity,
        'getaddresstxids': lambda _: [],
        'getaddressbalance': lambda _: dict(balance=0, received=0),
        'getcurrency': lambda params: definition if params[0] == currency
        else dict(currencyid=VRSC, name='VRSC', systemid=VRSC),
        'getblockcount': lambda _: 100,
        'getimports': lambda _: [launch],
        'getrawtransaction': lambda params: transactions.get(params[0]),
        'gettxout': lambda params: dict(scriptPubKey=dict(hex='cc01')) if tuple(params[:2]) == tip else None,
        'getspentinfo': spender,
    }
    try:
        with tempfile.TemporaryDirectory(prefix='carbide-token-control-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            terminal = Terminal(root, conf)
            try:
                wait(terminal, 'Recent Blocks')
                terminal.key(b'/FixtureToken@\r')
                wait(terminal, '16 hops checked')
                screen = terminal.read(.5)
                assert 'Tokenized Control' in screen
                assert 'Currency defined' in screen
                assert 'No flags set' not in screen
                assert 'Enter follows up to 16 more' in screen
                assert [(p[0], p[1]) for method, p in server.requests if method == 'gettxout'] == points[:16]
                terminal.key(b'\x1b[H\r')
                wait(terminal, '17 hops checked')
                assert 'Current token holder: VRSC@' in terminal.screen() or (
                    f'Current token holder: {VRSC}' in terminal.screen()
                )
                assert [(p[0], p[1]) for method, p in server.requests if method == 'gettxout'] == points
                terminal.key(b'\r')
                wait(terminal, 'No flags set')
                assert 'Tokenized Control' not in terminal.screen()
                assert_quits(terminal)
            finally:
                dispose(terminal)
    finally:
        server.shutdown()
        server.server_close()
    print('Tokenized control PTY check passed: launch output, bounded continuation, verified holder, and zero flags.')


def run_marketplace_discovery():
    funding, posting = 'a' * 64, 'c' * 64
    block_hash = lambda height: f'{1000 + height:064x}'
    # A full V4 offer template published as the last output of a different
    # posting transaction, without a spend of funding output 1.
    template = (
        struct.pack('<II', 0x80000004, 0x892f2085) + b'\x01'
        + bytes.fromhex(funding)[::-1] + struct.pack('<I', 0)
        + b'\x01\x51' + struct.pack('<I', 0xffffffff)
        + b'\x01' + struct.pack('<q', 25000) + b'\x01\x52'
        + struct.pack('<II', 0, 100) + bytes(11)
    )
    proof = bytes.fromhex('0500000003000101000000000100000000') + bytes([len(template)]) + template + bytes(4)
    offer_script = (bytes([0x6a, 0x4c, len(proof)]) + proof).hex()
    lock = dict(n=0, valueSat=50000, scriptPubKey=dict(commitmenthash=dict(currencyvalues={})))
    fund = dict(txid=funding, blockhash=block_hash(2), height=2, time=1700000002,
                confirmations=19, vin=[], vout=[lock, dict(n=1, valueSat=10000, scriptPubKey={})])
    publication = dict(txid=posting, vin=[], vout=[
        lock, dict(n=1, valueSat=0, scriptPubKey=dict(hex=offer_script, type='nulldata'))
    ])
    scanned = []

    def block(params):
        selector = str(params[0])
        height = int(selector, 16) - 1000 if len(selector) == 64 else int(selector)
        verbosity = params[1] if len(params) > 1 else 1
        if verbosity == 2:
            scanned.append(height)
        transactions = [publication] if height == 10 else [fund] if height == 2 else [
            dict(txid=f'{2000 + height:064x}', vin=[dict(coinbase='00')], vout=[])
        ]
        return dict(hash=block_hash(height), height=height, time=1700000000 + height,
                    size=100, confirmations=21-height, previousblockhash=block_hash(height-1),
                    tx=transactions if verbosity == 2 else [tx['txid'] for tx in transactions])

    server = daemon(VRSC)
    server.rpc_handlers = {
        'getinfo': lambda _: dict(chainid=VRSC, blocks=20, longestchain=20, connections=0),
        'getblockchaininfo': lambda _: dict(blocks=20, bestblockhash=block_hash(20)),
        'getblockcount': lambda _: 20,
        'getblockhash': lambda params: block_hash(int(params[0])),
        'getblock': block,
        'getblockheader': block,
        'getrawtransaction': lambda params: {funding: fund, posting: publication}.get(params[0]),
    }
    try:
        with tempfile.TemporaryDirectory(prefix='carbide-historical-offer-') as directory:
            root = Path(directory)
            conf = root / 'VRSC.conf'
            conf.write_text(f'rpcconnect=127.0.0.1\nrpcport={server.server_port}\n')
            terminal = Terminal(root, conf)
            try:
                wait(terminal, 'Recent Blocks')
                terminal.key(b'/' + funding.encode() + b'\r')
                wait(terminal, 'Outputs 2')
                terminal.key(b'e')
                wait(terminal, 'Press e to search from 10')
                assert [height for height in scanned if height <= 10] == list(range(2, 10)), scanned
                terminal.key(b'e')
                wait(terminal, 'Signed offer publication found')
                wait(terminal, 'Signed offer publication:')
                assert [height for height in scanned if height <= 10] == list(range(2, 11)), scanned
                terminal.key(b'\x1b[H\r')
                wait(terminal, 'Marketplace funding output:')
                assert_quits(terminal)
            finally:
                dispose(terminal)
    finally:
        server.shutdown()
        server.server_close()
    print('Historical marketplace PTY check passed: undiscovered embedded proof, bounded search, and resume.')


if __name__ == '__main__':
    import sys
    if '--job-control' in sys.argv:
        run_job_control()
    elif '--tmux' in sys.argv:
        run_tmux()
    elif '--shutdown-only' in sys.argv:
        run_shutdown()
    elif '--mempool-only' in sys.argv:
        run_mempool()
    elif '--peers-only' in sys.argv:
        run_peers()
    elif '--dashboard-only' in sys.argv:
        run_dashboard()
    elif '--token-control-only' in sys.argv:
        run_token_control()
    elif '--marketplace-discovery' in sys.argv:
        run_marketplace_discovery()
    else:
        run_shutdown()
        run()
        run_peers()
        run_mempool()
        run_dashboard()
        run_token_control()
        run_marketplace_discovery()
