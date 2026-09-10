"""本地、可重复的协议、并发和消息回归；不访问公网。"""

import asyncio
import importlib.util
import logging
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import warband_net as net
from warband_service import Record, WarbandService, endpoint

XML = b"<ServerStats><Name>CN_Test</Name><NumberOfActivePlayers>7</NumberOfActivePlayers></ServerStats>"


class LocalServer:
    def __init__(self, handler):
        self.handler = handler
        self.tasks = set()

    async def __aenter__(self):
        async def connected(reader, writer):
            task = asyncio.current_task()
            self.tasks.add(task)
            try:
                await self.handler(reader, writer)
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                writer.close()
                writer.transport.abort()
                self.tasks.discard(task)

        self.server = await asyncio.start_server(connected, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class NetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_xml_does_not_wait_for_close(self):
        async def handler(reader, writer):
            writer.write(b"HTTP/1.0 200 OK\r\n\r\n" + XML + b"trailing bytes")
            await writer.drain()
            await asyncio.sleep(2)

        async with LocalServer(handler) as server:
            started = time.monotonic()
            stats = await net.fetch_server_stats("127.0.0.1", server.port, 1)
            self.assertEqual(stats["players"], "7")
            self.assertLess(time.monotonic() - started, 0.3)

    async def test_fragmented_xml_and_chinese_cleanup(self):
        raw = XML.replace(
            b"</ServerStats>", "<MapName>战 场</MapName></ServerStats>".encode()
        )

        async def handler(reader, writer):
            for i in range(0, len(raw), 7):
                writer.write(raw[i : i + 7])
                await writer.drain()
                await asyncio.sleep(0.001)

        async with LocalServer(handler) as server:
            result = await net.fetch_server_stats("127.0.0.1", server.port, 1)
            self.assertEqual(result["map_name"], "战场")

    async def test_drip_feed_has_total_deadline(self):
        async def handler(reader, writer):
            while True:
                writer.write(b" ")
                await writer.drain()
                await asyncio.sleep(0.02)

        async with LocalServer(handler) as server:
            started = time.monotonic()
            self.assertIsNone(
                await net.fetch_server_stats("127.0.0.1", server.port, 0.12)
            )
            self.assertLess(time.monotonic() - started, 0.4)

    async def test_connect_uses_total_budget(self):
        async def slow_connect(*args, **kwargs):
            await asyncio.sleep(5)

        with patch.object(net.asyncio, "open_connection", slow_connect):
            started = time.monotonic()
            self.assertIsNone(await net.fetch_server_stats("127.0.0.1", 1, 0.05))
            self.assertLess(time.monotonic() - started, 0.3)

    async def test_xml_invalid_and_oversized(self):
        for raw in (b"<ServerStats>broken</ServerStats>", b"x" * 200001):

            async def handler(reader, writer, raw=raw):
                writer.write(raw)
                await writer.drain()

            async with LocalServer(handler) as server:
                self.assertIsNone(
                    await net.fetch_server_stats("127.0.0.1", server.port, 1)
                )

    async def test_http_framing_without_connection_close(self):
        bodies = [
            b"Content-Length: 3\r\n\r\nabc",
            b"Transfer-Encoding: chunked\r\n\r\n1\r\na\r\n2;foo=bar\r\nbc\r\n0\r\nX-Info: ok\r\n\r\n",
        ]
        for body in bodies:

            async def handler(reader, writer, body=body):
                await reader.readuntil(b"\r\n\r\n")
                writer.write(b"HTTP/1.1 200 OK\r\n" + body)
                await writer.drain()
                await asyncio.sleep(2)

            async with LocalServer(handler) as server:
                started = time.monotonic()
                result = await net.http_get_text(f"http://127.0.0.1:{server.port}/", 1)
                self.assertEqual(result, "abc")
                self.assertLess(time.monotonic() - started, 0.3)

    async def test_http_error_size_and_bad_chunk(self):
        responses = [
            b"HTTP/1.1 503 Down\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 99999999\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nnope\r\n",
        ]
        for raw in responses:

            async def handler(reader, writer, raw=raw):
                await reader.readuntil(b"\r\n\r\n")
                writer.write(raw)
                await writer.drain()

            async with LocalServer(handler) as server:
                with self.assertRaises(ValueError):
                    await net.http_get_text(f"http://127.0.0.1:{server.port}/", 1)

    async def test_http_repeated_non_framing_headers_are_valid(self):
        async def handler(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 200 OK\r\nVary: Origin\r\nVary: Accept-Encoding\r\nContent-Length: 3\r\n\r\nabc"
            )
            await writer.drain()

        async with LocalServer(handler) as server:
            self.assertEqual(
                await net.http_get_text(f"http://127.0.0.1:{server.port}/", 1), "abc"
            )

    async def test_http_conflicting_lengths_are_rejected(self):
        async def handler(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabc"
            )
            await writer.drain()

        async with LocalServer(handler) as server:
            with self.assertRaises(ValueError):
                await net.http_get_text(f"http://127.0.0.1:{server.port}/", 1)

    async def test_http_total_timeout(self):
        async def handler(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\n\r\n")
            while True:
                writer.write(b"x")
                await writer.drain()
                await asyncio.sleep(0.01)

        async with LocalServer(handler) as server:
            with self.assertRaises(asyncio.TimeoutError):
                await net.http_get_text(f"http://127.0.0.1:{server.port}/", 0.1)

    def test_master_validation_and_deduplication(self):
        self.assertEqual(
            net.parse_master_list(
                "127.0.0.1:12|127.0.0.1:12|bad:3|127.0.0.2:99999|127.0.0.3"
            ),
            [("127.0.0.1", 12), ("127.0.0.3", 7240)],
        )


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    def make_service(self, **cfg):
        config = {"extra_endpoints": [], "servers": [], "seed_hosts": [], **cfg}
        service = WarbandService(config.get, logging.getLogger("test"))
        self.addAsyncCleanup(service.stop)
        return service

    async def test_shared_probe_survives_waiter_cancel(self):
        service = self.make_service()
        gate = asyncio.Event()

        async def fetch(*args):
            await gate.wait()
            return {"name": "CN_A"}

        with patch.object(net, "fetch_server_stats", side_effect=fetch) as mocked:
            first = asyncio.create_task(service.probe(("127.0.0.1", 1)))
            second = asyncio.create_task(service.probe(("127.0.0.1", 1)))
            await asyncio.sleep(0.02)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            gate.set()
            self.assertEqual((await second)["name"], "CN_A")
            self.assertEqual(mocked.call_count, 1)

    async def test_target_returns_before_scan_finishes(self):
        service = self.make_service(probe_concurrency=8)
        addrs = [("127.0.0.1", i) for i in range(1, 7)]

        async def fetch(host, port, timeout):
            await asyncio.sleep(0.05 if port == 1 else 2)
            return {"name": "CN_Target" if port == 1 else f"CN_Other{port}"}

        with (
            patch.object(
                net, "fetch_master_server_list", AsyncMock(return_value=addrs)
            ),
            patch.object(net, "fetch_server_stats", side_effect=fetch),
        ):
            started = time.monotonic()
            statuses = await asyncio.gather(
                service.discover("CN_Target", 1), service.discover("target", 1)
            )
            self.assertEqual(statuses, ["found", "found"])
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertFalse(service._discovery_task.done())
            await service.stop()
            self.assertFalse(service._tasks)

    async def test_fixed_endpoint_independent_of_master(self):
        service = self.make_service(extra_endpoints=["127.0.0.1:1"])

        async def master(*args, **kwargs):
            await asyncio.sleep(5)
            raise OSError("down")

        with (
            patch.object(net, "fetch_master_server_list", side_effect=master),
            patch.object(
                net, "fetch_server_stats", AsyncMock(return_value={"name": "CN_Fixed"})
            ),
        ):
            await service.start()
            await service.wait_initial([("127.0.0.1", 1)], 0.5)
            self.assertEqual(service.records[("127.0.0.1", 1)].name, "CN_Fixed")
            self.assertFalse(service._master_task.done())
            await service.stop()

    async def test_failed_master_preserves_old_list(self):
        service = self.make_service()
        service.master = [("127.0.0.1", 1)]
        with patch.object(
            net, "fetch_master_server_list", AsyncMock(side_effect=OSError("down"))
        ):
            self.assertEqual(await service.master_list(), service.master)
            self.assertFalse(service.master_ok)

    async def test_limits_concurrency_and_reserves_query_capacity(self):
        service = self.make_service(probe_concurrency=4)
        active = 0
        maximum = 0
        slow_started = asyncio.Event()
        gate = asyncio.Event()

        async def fetch(host, port, timeout):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            try:
                if port != 99:
                    slow_started.set()
                    await gate.wait()
                return {"name": f"CN_{port}"}
            finally:
                active -= 1

        with patch.object(net, "fetch_server_stats", side_effect=fetch):
            scan = asyncio.create_task(
                service._scan([("127.0.0.1", n) for n in range(1, 50)], background=True)
            )
            await slow_started.wait()
            result = await asyncio.wait_for(service.probe(("127.0.0.1", 99)), 0.2)
            self.assertEqual(result["name"], "CN_99")
            gate.set()
            await scan
            self.assertLessEqual(maximum, 4)

    async def test_per_record_failure_and_rename(self):
        service = self.make_service()
        pair = ("127.0.0.1", 1)
        with patch.object(
            net,
            "fetch_server_stats",
            AsyncMock(side_effect=[{"name": "CN_Old"}, None, {"name": "CN_New"}]),
        ):
            await service.probe(pair)
            success = service.records[pair].success
            service._attempt_mono.clear()
            await service.probe(pair)
            self.assertEqual(service.records[pair].success, success)
            self.assertEqual(service.state(service.records[pair]), "stale")
            service._attempt_mono.clear()
            await service.probe(pair)
            self.assertFalse(service.matching("CN_Old"))
            self.assertEqual(service.matching("CN_New"), [pair])

    async def test_persistence_restores_original_age(self):
        with tempfile.TemporaryDirectory() as directory:
            service = self.make_service()
            service.path = Path(directory) / "servers.json"
            old = time.time() - 120
            service.records[("127.0.0.1", 1)] = Record({"name": "CN_A"}, old, old)
            await service._save()
            other = self.make_service()
            other.path = service.path
            other._restore(other._read_snapshot())
            rec = other.records[("127.0.0.1", 1)]
            self.assertEqual(rec.success, old)
            self.assertEqual(other.state(rec), "stale")

    async def test_corrupt_cache_does_not_block_start(self):
        with tempfile.TemporaryDirectory() as directory:
            service = self.make_service()
            service.path = Path(directory) / "servers.json"
            service.path.write_text("{broken", encoding="utf-8")
            with patch.object(
                net, "fetch_master_server_list", AsyncMock(return_value=[])
            ):
                await service.start()
                self.assertTrue(service._started)
                await service.stop()

    async def test_unknown_is_not_offline_and_old_is_not_fresh(self):
        service = self.make_service()
        self.assertEqual(service.state(None), "unknown")
        self.assertEqual(
            service.state(Record(attempted=time.time(), failures=1)), "unresponsive"
        )
        old = time.time() - 1000
        self.assertEqual(
            service.state(Record({"name": "A"}, old, time.time())), "unresponsive"
        )

    async def test_live_cache_age_ignores_wall_clock_jump(self):
        service = self.make_service()
        with patch.object(
            net, "fetch_server_stats", AsyncMock(return_value={"name": "CN_A"})
        ):
            await service.probe(("127.0.0.1", 1))
        rec = service.records[("127.0.0.1", 1)]
        with patch("warband_service.time.time", return_value=rec.success + 86400):
            self.assertEqual(service.state(rec), "fresh")

    async def test_start_is_idempotent_and_stop_cleans_tasks(self):
        service = self.make_service()
        with patch.object(net, "fetch_master_server_list", AsyncMock(return_value=[])):
            await asyncio.gather(service.start(), service.start(), service.start())
            tasks = set(service._tasks)
            await service.start()
            self.assertEqual(tasks, service._tasks)
            await service.stop()
            self.assertFalse(service._tasks)
            await service.start()
            self.assertFalse(service._tasks)

    async def test_discovery_timeout_continues_and_next_request_reuses(self):
        service = self.make_service()
        gate = asyncio.Event()

        async def fetch(*args):
            await gate.wait()
            return {"name": "CN_Target"}

        with (
            patch.object(
                net,
                "fetch_master_server_list",
                AsyncMock(return_value=[("127.0.0.1", 1)]),
            ),
            patch.object(net, "fetch_server_stats", side_effect=fetch) as probe,
        ):
            self.assertEqual(await service.discover("CN_Target", 0.02), "pending")
            gate.set()
            self.assertEqual(await service.discover("CN_Target", 0.5), "found")
            self.assertEqual(probe.call_count, 1)

    async def test_prunes_records_and_snapshot_stays_readable(self):
        service = self.make_service()
        now = time.time()
        for i in range(2050):
            service.records[("127.0.0.1", i + 1)] = Record(
                {"name": f"CN_{i}", "map_name": "地" * 256, "module": "模" * 256},
                now,
                now,
            )
        service._reindex()
        service._prune()
        self.assertLessEqual(len(service.records), 2048)
        with tempfile.TemporaryDirectory() as directory:
            service.path = Path(directory) / "servers.json"
            await service._save()
            self.assertLessEqual(service.path.stat().st_size, 2_000_000)
            self.assertTrue(service._read_snapshot())

    async def test_failed_save_retries_without_breaking_cache(self):
        service = self.make_service()
        now = time.time()
        service.records[("127.0.0.1", 1)] = Record({"name": "CN_A"}, now, now)
        with patch.object(
            service, "_write_snapshot", side_effect=OSError("disk unavailable")
        ):
            await service._save()
        self.assertTrue(service._dirty)
        self.assertEqual(service.records[("127.0.0.1", 1)].name, "CN_A")

    async def test_discovery_failure_not_negative_cached(self):
        service = self.make_service()
        with (
            patch.object(
                net,
                "fetch_master_server_list",
                AsyncMock(return_value=[("127.0.0.1", 1)]),
            ),
            patch.object(net, "fetch_server_stats", AsyncMock(return_value=None)),
        ):
            self.assertEqual(await service.discover("CN_Missing", 1), "unavailable")
            self.assertFalse(service._discovery_success)

    async def test_completed_discovery_reuses_negative_result(self):
        service = self.make_service()
        with (
            patch.object(
                net,
                "fetch_master_server_list",
                AsyncMock(return_value=[("127.0.0.1", 1)]),
            ),
            patch.object(
                net, "fetch_server_stats", AsyncMock(return_value={"name": "CN_A"})
            ) as probe,
        ):
            self.assertEqual(await service.discover("CN_Missing", 1), "missing")
            self.assertEqual(await service.discover("CN_Missing2", 1), "missing")
            self.assertEqual(probe.call_count, 1)

    async def test_excluded_endpoint_hidden(self):
        service = self.make_service(exclude_servers=["CN_X4_zuikuai"])
        with patch.object(
            net, "fetch_server_stats", AsyncMock(return_value={"name": "cn_x4_ZUIKUAI"})
        ):
            await service.probe(("127.0.0.1", 1))
        self.assertFalse(service.visible(("127.0.0.1", 1)))
        self.assertFalse(service.matching("CN_X4_zuikuai"))

    async def test_bad_config_and_invalid_snapshot(self):
        service = self.make_service(
            probe_concurrency="wrong", probe_timeout=float("nan")
        )
        self.assertEqual(service.concurrency, 32)
        self.assertEqual(service.number("probe_timeout", 4, 0.1, 30), 4)
        service._restore(
            [
                {},
                {"addr": 3},
                {"addr": "127.0.0.1:1", "stats": {}, "success": "nan", "attempted": 0},
            ]
        )
        self.assertFalse(service.records)
        self.assertIsNone(endpoint(3))


def load_plugin_for_unit_tests():
    """仅替换框架边界；消息处理和服务均执行真实插件实现。"""
    api, event, star = (
        types.ModuleType(name)
        for name in ("astrbot.api", "astrbot.api.event", "astrbot.api.star")
    )
    api.AstrBotConfig = dict
    api.logger = logging.getLogger("test")

    class Filter:
        def __getattr__(self, name):
            return lambda *args, **kwargs: lambda function: function

    event.filter = Filter()
    event.AstrMessageEvent = object
    star.Context = object
    star.Star = object
    modules = {
        "astrbot": types.ModuleType("astrbot"),
        "astrbot.api": api,
        "astrbot.api.event": event,
        "astrbot.api.star": star,
    }
    spec = importlib.util.spec_from_file_location(
        "warband_test_plugin", Path(__file__).resolve().parents[1] / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module.WarbandServerStatusPlugin


Plugin = load_plugin_for_unit_tests()


class Event:
    is_at_or_wake_command = False

    def __init__(self, text="查服", private=False):
        self.text, self.private = text, private

    def get_sender_id(self):
        return "123"

    def get_group_id(self):
        return "456"

    def is_private_chat(self):
        return self.private

    def get_message_str(self):
        return self.text

    def plain_result(self, text):
        return text


class PluginTests(unittest.IsolatedAsyncioTestCase):
    def make_plugin(self, **config):
        plugin = Plugin.__new__(Plugin)
        plugin.config = {
            "servers": ["CN_X1"],
            "extra_endpoints": [],
            "seed_hosts": [],
            **config,
        }
        plugin.service = WarbandService(plugin._cfg, logging.getLogger("test"))
        plugin.service._started = True
        self.addAsyncCleanup(plugin.terminate)
        return plugin

    def add_record(self, plugin, port, name, **kwargs):
        now = time.time()
        plugin.service.records[("127.0.0.1", port)] = Record(
            {"name": name, "players": "7", "max_players": "20"}, now, now, **kwargs
        )
        plugin.service._reindex()

    async def test_hot_reply_never_waits_for_network(self):
        plugin = self.make_plugin()
        self.add_record(plugin, 1, "CN_X1")
        with patch.object(
            net, "fetch_server_stats", AsyncMock(side_effect=AssertionError("network"))
        ):
            durations = []
            for _ in range(100):
                started = time.monotonic()
                reply = await plugin._build_reply(Event(), "X1", silent_gate=False)
                durations.append(time.monotonic() - started)
            self.assertIn("7/20", reply)
            self.assertLess(sorted(durations)[94], 0.05)

    async def test_exact_name_before_fuzzy_and_port_not_suffix(self):
        plugin = self.make_plugin(extra_endpoints=["127.0.0.1:17240", "127.0.0.1:7240"])
        self.add_record(plugin, 1, "CN_X10")
        self.add_record(plugin, 2, "CN_X1")
        self.assertEqual(plugin._resolve_target("X1", plugin._ordered_keys()), "CN_X1")
        self.assertEqual(
            plugin._resolve_target("7240", plugin._ordered_keys()), "127.0.0.1:7240"
        )
        self.assertIsNone(plugin._resolve_target("240", plugin._ordered_keys()))

    async def test_duplicate_names_ask_for_endpoint(self):
        plugin = self.make_plugin()
        self.add_record(plugin, 1, "CN_X1")
        self.add_record(plugin, 2, "CN_X1")
        reply = await plugin._build_reply(Event(), "X1", silent_gate=False)
        self.assertIn("多个服务器", reply)
        self.assertIn("127.0.0.1:1", reply)
        direct = await plugin._build_reply(Event(), "127.0.0.1:2", silent_gate=False)
        self.assertIn("7/20", direct)

    async def test_migration_follows_live_address_but_fixed_old_address_stays_visible(
        self,
    ):
        plugin = self.make_plugin(extra_endpoints=["127.0.0.1:1"])
        self.add_record(plugin, 1, "CN_X1", failures=3)
        self.add_record(plugin, 2, "CN_X1")
        reply = await plugin._build_reply(Event(), "X1", silent_gate=False)
        self.assertIn("7/20", reply)
        self.assertIn("127.0.0.1:1", plugin._ordered_keys())
        reply = await plugin._build_reply(Event(), "127.0.0.1:1", silent_gate=False)
        self.assertIn("近期无响应", reply)

    async def test_case_normalization_does_not_duplicate_all_results(self):
        plugin = self.make_plugin()
        self.add_record(plugin, 1, "cn_x1")
        reply = await plugin._build_reply(Event(), "", silent_gate=False)
        self.assertEqual(reply.count("服务器名称"), 1)

    async def test_name_with_repeated_cn_prefix_keeps_exact_identity(self):
        plugin = self.make_plugin()
        self.add_record(plugin, 1, "CN_CNExample")
        self.add_record(plugin, 2, "CN_Example")
        self.assertEqual(
            plugin._resolve_target("CN_CNExample", plugin._ordered_keys()),
            "CN_CNExample",
        )

    async def test_newly_discovered_server_is_in_current_all_reply(self):
        plugin = self.make_plugin(extra_endpoints=["127.0.0.1:1"])

        async def initial(*args):
            self.add_record(plugin, 1, "CN_X1")
            self.add_record(plugin, 2, "CN_XNew")

        with patch.object(plugin.service, "wait_initial", side_effect=initial):
            reply = await plugin._build_reply(Event(), "", silent_gate=False)
        self.assertIn("CN_XNew", reply)

    async def test_no_reply_after_unload(self):
        plugin = self.make_plugin()
        self.add_record(plugin, 1, "CN_X1")
        await plugin.terminate()
        self.assertIsNone(await plugin._build_reply(Event(), "X1", silent_gate=False))

    async def test_stale_reply_and_offline_visibility(self):
        plugin = self.make_plugin(show_offline=False)
        self.add_record(plugin, 1, "CN_X1", failures=1)
        reply = await plugin._build_reply(Event(), "", silent_gate=False)
        self.assertIn("上次结果", reply)
        plugin.service.records[("127.0.0.1", 1)].failures = 3
        reply = await plugin._build_reply(Event(), "", silent_gate=False)
        self.assertNotIn("7/20", reply)
        direct = await plugin._build_reply(Event(), "X1", silent_gate=False)
        self.assertIn("近期无响应", direct)

    async def test_whitelist_and_private_gates(self):
        plugin = self.make_plugin(group_whitelist=["999"], enable_private=False)
        self.assertIsNone(await plugin._build_reply(Event(), "", silent_gate=True))
        self.assertIn(
            "未启用",
            await plugin._build_reply(Event(private=True), "", silent_gate=False),
        )

    async def test_keyword_glued_and_command(self):
        plugin = self.make_plugin()
        self.add_record(plugin, 1, "CN_X1")
        replies = [x async for x in plugin.keyword_query(Event("查服CN_X1"))]
        self.assertEqual(len(replies), 1)
        self.assertIn("7/20", replies[0])
        replies = [x async for x in plugin.cmd_query(Event(), "X1")]
        self.assertEqual(len(replies), 1)
        wake = Event("查服 X1")
        wake.is_at_or_wake_command = True
        self.assertEqual([x async for x in plugin.keyword_query(wake)], [])


if __name__ == "__main__":
    unittest.main()
