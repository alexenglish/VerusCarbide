"""Exercise the real ZeroMQ subscriber against synthetic publishers and a local RPC daemon."""
import base64
import hashlib
import re
import struct
import tempfile
import time
from pathlib import Path

import zmq

from bookmarks_terminal import Terminal, VRSC, daemon
from mouse_terminal import click
from reorg_terminal import wait


def sha256d(data):
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def transaction(marker):
    data = (
        struct.pack("<IB", 1, 1)
        + bytes([marker]) * 32
        + struct.pack("<IBIB", 0, 0, 0xffffffff, 1)
        + struct.pack("<qBBI", 100_000_000 + marker, 1, 0x6a, 0)
    )
    return sha256d(data)[::-1].hex(), data


def block(height, parent, marker):
    txid, tx = transaction(marker)
    header = (
        struct.pack("<I", 1)
        + bytes.fromhex(parent)[::-1]
        + sha256d(tx)
        + bytes(32)
        + struct.pack("<II", 1700000000 + height * 60, 0x1f000fff)
        + bytes(32)
        + bytes([1, marker])
    )
    block_hash = sha256d(header)[::-1].hex()
    data = dict(
        height=height,
        hash=block_hash,
        time=1700000000 + height * 60,
        previousblockhash=parent if height else None,
        confirmations=1,
        tx=[txid],
        size=len(header) + 1 + len(tx),
    )
    return data, header, header + bytes([1]) + tx, txid, tx


def config_lines(topics, port):
    return "".join(f"zmqpub{topic}=tcp://127.0.0.1:{port}\n" for topic in topics)


def run(topics, split=False):
    context = zmq.Context()
    publishers = []
    ports = {}
    for topic in topics:
        if not publishers or split:
            publisher = context.socket(zmq.PUB)
            publisher.linger = 0
            port = publisher.bind_to_random_port("tcp://127.0.0.1")
            publishers.append(publisher)
        ports[topic] = (port, publisher)
    server = daemon(VRSC)
    genesis = block(0, "00" * 32, 1)
    first = block(1, genesis[0]["hash"], 2)
    replacement = block(1, genesis[0]["hash"], 3)
    confirmed = block(2, replacement[0]["hash"], 4)
    mem_txid, mem_raw = transaction(4)
    state = {"tip": genesis, "mempool": {}}
    blocks = {entry[0]["hash"]: entry for entry in (genesis, first, replacement, confirmed)}
    sequence = {}
    payloads = {
        "hashblock": lambda entry: bytes.fromhex(entry[0]["hash"]),
        "rawblock": lambda entry: entry[2],
        "checkedblock": lambda entry: entry[2],
        "hashtx": lambda _: bytes.fromhex(mem_txid),
        "rawtx": lambda _: mem_raw,
    }

    def send(topic, entry=None):
        sequence[topic] = sequence.get(topic, -1) + 1
        ports[topic][1].send_multipart([
            topic.encode(), payloads[topic](entry), struct.pack("<I", sequence[topic])
        ])

    def lookup(args):
        if len(str(args[0])) == 64:
            return blocks.get(args[0])
        return state["tip"] if int(args[0]) == state["tip"][0]["height"] else genesis

    def header(args):
        entry = lookup(args)
        if not entry:
            return None
        if len(args) > 1 and args[1] is False:
            return entry[1].hex()
        if split:
            raw = entry[1]
            return dict(entry[0], validationtype="work", version=1, bits="1f000fff",
                        merkleroot=raw[36:68][::-1].hex(), finalsaplingroot=raw[68:100][::-1].hex(),
                        nonce=raw[108:140][::-1].hex(), solution=raw[141:].hex())
        return dict(entry[0], validationtype="work")

    def detail(args):
        entry = lookup(args)
        return entry[0] if entry else None

    def raw_transaction(args):
        txid = args[0]
        return dict(txid=txid, vin=[], vout=[], size=len(mem_raw))

    server.rpc_handlers = {
        "getinfo": lambda _: dict(
            chainid=VRSC, testnet=False, blocks=state["tip"][0]["height"],
            longestchain=state["tip"][0]["height"], connections=1
        ),
        "getblockchaininfo": lambda _: dict(
            blocks=state["tip"][0]["height"], bestblockhash=state["tip"][0]["hash"]
        ),
        "getblockheader": header,
        "getblock": detail,
        "getrawmempool": lambda _: state["mempool"],
        "getrawtransaction": raw_transaction,
    }
    try:
        with tempfile.TemporaryDirectory(prefix="carbide-zmq-") as directory:
            root = Path(directory)
            conf = root / "VRSC.conf"
            rpc = f"rpcconnect=127.0.0.1\nrpcport={server.server_port}\n"
            conf.write_text(rpc)
            terminal = Terminal(root, conf, poll_interval=2)
            try:
                wait(terminal, lambda: "Recent Transactions" in terminal.screen())
                wait(terminal, lambda: "zmq off" in terminal.screen().splitlines()[0])
                if topics:
                    terminal.key(b"\x1bOQz")
                    wait(terminal, lambda: "zmqpubrawblock=" in terminal.screen())
                    rows = terminal.screen().splitlines()
                    expected = "\n".join(
                        re.search(r"zmqpub\w+=tcp://127\.0\.0\.1:\d+", row)[0]
                        for row in rows if "zmqpub" in row
                    )
                    assert [line.split("=")[0] for line in expected.splitlines()] == [
                        "zmqpubrawblock", "zmqpubrawtx"
                    ]
                    assert click(terminal, "[=] Copy", "[=] Copy") == expected
                    wait(terminal, lambda: "+ Copied" in terminal.screen())
                    assert click(terminal, "zmqpubrawblock=", "zmqpubrawblock=") == expected
                    click(terminal, "Click anywhere inside", "Click anywhere", copies=False)
                    before = len(terminal.captured)
                    terminal.key(b"c")
                    copied = re.findall(rb"\x1b\]52;c;([A-Za-z0-9+/=]+)", terminal.captured[before:])
                    assert any(expected == base64.b64decode(value).decode() for value in copied)
                    busy = next(iter(ports.values()))[0]
                    terminal.key(f"p{busy}\r".encode())
                    wait(terminal, lambda: "already configured or in use" in terminal.screen())
                    terminal.key(b"p\r")
                    wait(terminal, lambda: "zmqpubrawblock=" in terminal.screen())
                    conf.write_text(rpc + "".join(config_lines([topic], ports[topic][0]) for topic in topics))
                    terminal.key(b"r")
                    wait(terminal, lambda: "Connected" in terminal.screen() and all(
                        f":{ports[topic][0]} · Connected" in terminal.screen() for topic in topics
                    ))
                    terminal.key(b"\x1b")
                    wait(terminal, lambda: "Sources" in terminal.screen() and "ZMQ Setup ·" not in terminal.screen())
                    wait(terminal, lambda: "ZMQ  Connected · waiting for events" in terminal.screen())
                    send(topics[0], genesis)
                    wait(terminal, lambda: "ZMQ  Receiving · last" in terminal.screen()
                         and f"1/{len(topics)} topics seen" in terminal.screen())
                    terminal.key(b"\x1b")
                    wait(terminal, lambda: "zmq live" in terminal.screen().splitlines()[0])
                if any(topic in topics for topic in ("hashblock", "rawblock")):
                    assert "Dashboard  ·  Listening on ZMQ for blocks" in terminal.screen(), terminal.screen()
                    before = server.calls.count("getblockchaininfo")
                    terminal.read(2.1)
                    assert before == server.calls.count("getblockchaininfo"), server.requests
                else:
                    assert "Dashboard  ·  RPC polling" in terminal.screen(), terminal.screen()
                for candidate, previous in ((first, None), (replacement, first)):
                    state["tip"] = candidate
                    start = time.monotonic()
                    for topic in ("rawblock", "hashblock", "checkedblock"):
                        if topic in topics:
                            send(topic, candidate)
                    wait(terminal, lambda: candidate[0]["hash"][:6] in terminal.screen()
                         and candidate[3][:6] in terminal.screen())
                    latency = time.monotonic() - start
                    if any(topic in topics for topic in ("rawblock", "hashblock")):
                        assert latency < 1.5, f"Block latency {latency:.3f}s with {topics}"
                    if previous is not None:
                        assert previous[3][:6] not in terminal.screen(), terminal.screen()
                    if "rawblock" in topics:
                        assert ("getblock", [candidate[0]["hash"], 1]) not in server.requests
                        assert ("getrawtransaction", [candidate[3], 1]) not in server.requests
                terminal.key(b"m")
                wait(terminal, lambda: "Mempool is empty" in terminal.screen())
                if any(topic in topics for topic in ("rawtx", "hashtx")):
                    assert "Listening on ZMQ" in terminal.screen(), terminal.screen()
                    before = server.calls.count("getrawmempool")
                    terminal.read(2.1)
                    assert before == server.calls.count("getrawmempool"), server.requests
                state["mempool"] = {mem_txid: dict(size=len(mem_raw), height=1, time=int(time.time()))}
                start = time.monotonic()
                for topic in ("rawtx", "hashtx"):
                    if topic in topics:
                        send(topic)
                wait(terminal, lambda: mem_txid[:6] in terminal.screen())
                if any(topic in topics for topic in ("rawtx", "hashtx")):
                    assert time.monotonic() - start < 1.5, f"Mempool latency with {topics}"
                if "rawtx" in topics:
                    terminal.read(0.3)
                    assert ("getrawtransaction", [mem_txid, 1]) not in server.requests
                if topics == ("rawtx",):
                    # The daemon admits another transaction after the first snapshot.
                    # Simulate losing its notification entirely.
                    second_txid, second_raw = transaction(5)
                    state["mempool"][second_txid] = dict(size=len(second_raw), height=1, time=int(time.time()))
                    deadline = time.monotonic() + 4
                    while "Transactions 2" not in terminal.read(0.1) and time.monotonic() < deadline:
                        pass
                    assert second_txid[:6] in terminal.screen() and "Transactions 2" in terminal.screen(), terminal.screen()
                    before = server.calls.count("getrawmempool")
                    terminal.read(2.1)
                    assert before == server.calls.count("getrawmempool"), server.requests
                    third_txid, third_raw = transaction(6)
                    state["mempool"][third_txid] = dict(size=len(third_raw), height=1, time=int(time.time()))
                    terminal.key(b"\x1b[15~")  # F5
                    wait(terminal, lambda: third_txid[:6] in terminal.screen() and "Transactions 3" in terminal.screen())
                if len(topics) == 4:
                    # Leave a stale snapshot in the mock while the block arrives.
                    # Confirmation must remove the item before the daemon snapshot catches up.
                    state["tip"] = confirmed
                    send("rawblock", confirmed)
                    send("hashblock", confirmed)
                    wait(terminal, lambda: "Mempool is empty" in terminal.screen())
                    state["tip"] = replacement
                    send("rawblock", replacement)
                    send("hashblock", replacement)
                    wait(terminal, lambda: mem_txid[:6] in terminal.screen())

                    if not split:
                        port, publisher = ports["hashtx"]
                        publisher.close()
                        terminal.key(b"\x1bOQz")
                        wait(terminal, lambda: "Unreachable" in terminal.screen())
                        fresh = context.socket(zmq.PUB)
                        fresh.linger = 0
                        fresh.bind(f"tcp://127.0.0.1:{port}")
                        publishers.append(fresh)
                        state["mempool"] = {}
                        wait(terminal, lambda: "Connected" in terminal.screen())
                        terminal.key(b"\x1b")
                        terminal.key(b"\x1b")
                        wait(terminal, lambda: "Mempool is empty" in terminal.screen())
                if split:
                    assert not any(method == "getblockheader" and args[-1] is False
                                   for method, args in server.requests)
                assert "decoderawtransaction" not in server.calls
                print(f"ZMQ terminal check passed for {', '.join(topics) or 'RPC only'}"
                      + (" on split endpoints" if split else ""))
            finally:
                terminal.close()
    finally:
        server.shutdown()
        server.server_close()
        for publisher in publishers:
            publisher.close()
        context.term()


if __name__ == "__main__":
    import sys

    cases = [
        (),
        ("hashblock",),
        ("rawblock",),
        ("hashtx",),
        ("rawtx",),
        ("checkedblock",),
        ("hashblock", "rawblock", "hashtx", "rawtx"),
    ]
    only = sys.argv[1:]
    for case in cases:
        if not only or list(case) == only:
            run(case)
    if not only:
        run(cases[-1], split=True)
