"""运行 python -m tests.benchmark_warband [--baseline-ref Git引用]。"""

import argparse
import asyncio
import json
import logging
import statistics
import subprocess
import time
import types
from unittest.mock import patch

import warband_net as net
from tests.test_warband import XML, Event, LocalServer, Plugin
from warband_service import Record, WarbandService


async def benchmark(baseline_ref=None):
    modules = {"current": net}
    if baseline_ref:
        result = await asyncio.to_thread(
            subprocess.run,
            ["git", "show", f"{baseline_ref}:warband_net.py"],
            check=True,
            capture_output=True,
            encoding="utf-8",
        )
        source = result.stdout
        baseline = types.ModuleType("baseline_network")
        exec(compile(source, "baseline_network", "exec"), baseline.__dict__)  # noqa: S102 - 显式指定的本仓库历史版本用于对照
        modules["baseline"] = baseline
    results = {}

    async def handler(reader, writer):
        writer.write(XML)
        await writer.drain()
        await asyncio.sleep(2)

    async with LocalServer(handler) as server:
        for name, module in modules.items():
            durations = []
            for _ in range(5):
                start = time.perf_counter()
                assert await module.fetch_server_stats("127.0.0.1", server.port, 1)
                durations.append((time.perf_counter() - start) * 1000)
            results[f"xml_{name}_median_ms"] = round(statistics.median(durations), 3)

    config = {
        "servers": ["CN_X1"],
        "extra_endpoints": [],
        "seed_hosts": [],
        "probe_concurrency": 8,
    }
    plugin = Plugin.__new__(Plugin)
    plugin.config = config
    service = plugin.service = WarbandService(
        config.get, logging.getLogger("benchmark")
    )
    service._started = True
    now = time.time()
    # 除目标外放入 1000 条其他名称，防止只测空缓存的理想情况。
    for i in range(1001):
        service.records[("127.0.0.1", i + 1)] = Record(
            {"name": f"CN_Other{i}"}, now, now
        )
    service.records[("127.0.0.1", 1)] = Record(
        {"name": "CN_X1", "players": "7"}, now, now
    )
    service._reindex()
    durations = []
    for _ in range(200):
        start = time.perf_counter()
        await plugin._build_reply(Event(), "X1", silent_gate=False)
        durations.append((time.perf_counter() - start) * 1000)
    results["cached_reply_1001_records_p95_ms"] = round(sorted(durations)[189], 3)
    await service.stop()

    async def fetch(host, port, timeout):
        await asyncio.sleep(0.05 if port == 1 else 0.7)
        return {"name": "CN_Target" if port == 1 else "CN_Other"}

    addrs = [("127.0.0.1", 1), ("127.0.0.1", 2)]
    service = WarbandService(config.get, logging.getLogger("benchmark"))
    service.master, service.master_updated, service.master_ok = (
        addrs,
        time.monotonic(),
        True,
    )
    with patch.object(net, "fetch_server_stats", side_effect=fetch):
        start = time.perf_counter()
        assert await service.discover("CN_Target", 1) == "found"
        results["target_current_ms"] = round((time.perf_counter() - start) * 1000, 3)
        await service.stop()
    if "baseline" in modules:
        with patch.object(modules["baseline"], "fetch_server_stats", side_effect=fetch):
            start = time.perf_counter()
            await modules["baseline"].probe_many(addrs, 1)
            results["target_baseline_batch_ms"] = round(
                (time.perf_counter() - start) * 1000, 3
            )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(benchmark(args.baseline_ref)), indent=2))
