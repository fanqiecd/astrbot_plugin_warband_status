"""与 AstrBot 解耦的端点调度、发现和状态缓存。"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import math
import re
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

try:
    from . import warband_net as net
except ImportError:
    import warband_net as net

Endpoint = tuple[str, int]
MAX_RECORDS = 2048
RETENTION = 7 * 86400
HOT_TTL = 900


def normalize(text: str) -> str:
    return re.sub(r"[\s_-]", "", text).lower().removeprefix("cn")


def endpoint(text: str) -> Endpoint | None:
    """只接受有效 IPv4 端点，防止错误配置进入探测队列。"""
    if not isinstance(text, str):
        return None
    host, _, port = text.strip().partition(":")
    try:
        pair = str(ipaddress.IPv4Address(host)), int(port or net.DEFAULT_PORT)
        return pair if 0 < pair[1] < 65536 else None
    except (ValueError, TypeError):
        return None


def label(pair: Endpoint) -> str:
    return f"{pair[0]}:{pair[1]}"


@dataclass
class Record:
    stats: dict[str, Any] = field(default_factory=dict)
    success: float = 0
    attempted: float = 0
    failures: int = 0
    restored: bool = False
    success_mono: float | None = None

    @property
    def name(self) -> str:
        return str(self.stats.get("name") or "")


class WarbandService:
    def __init__(self, cfg: Callable, logger: Any, path: Path | None = None):
        self.cfg, self.logger, self.path = cfg, logger, path
        self.records: dict[Endpoint, Record] = {}
        self.name_index: dict[str, set[Endpoint]] = {}
        self.hot: dict[Endpoint, float] = {}
        self.metrics: Counter = Counter()
        self.master: list[Endpoint] = []
        self.master_updated = 0.0
        self.master_attempt = -math.inf
        self.master_ok = False
        self.master_complete = True
        self.changed = asyncio.Event()
        self.refresh_wake = asyncio.Event()
        self._start_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()
        self._inflight: dict[Endpoint, asyncio.Task] = {}
        self._attempt_mono: dict[Endpoint, float] = {}
        self._master_task: asyncio.Task | None = None
        self._discovery_task: asyncio.Task | None = None
        self._discovery_completed = -math.inf
        self._discovery_success = False
        self._started = False
        self._closed = False
        self._dirty = False
        self._sweep = 0
        self.concurrency = int(self.number("probe_concurrency", 32, 4, 128))
        self._sem = asyncio.Semaphore(self.concurrency)

    def number(self, key: str, default: float, low: float, high: float) -> float:
        try:
            value = float(self.cfg(key, default))
            return min(high, max(low, value)) if math.isfinite(value) else default
        except (ValueError, TypeError, OverflowError):
            return default

    def strings(self, key: str, default: list[str]) -> list[str]:
        value = self.cfg(key, default)
        if not isinstance(value, (list, tuple)):
            value = default
        return list(
            dict.fromkeys(
                str(x).strip()
                for x in value[:256]
                if isinstance(x, (str, int)) and str(x).strip()
            )
        )

    def excluded(self, name: str) -> bool:
        return normalize(name) in {
            normalize(x) for x in self.strings("exclude_servers", ["CN_X4_zuikuai"])
        }

    def extras(self) -> list[Endpoint]:
        return list(
            dict.fromkeys(
                p
                for x in self.strings(
                    "extra_endpoints", ["106.54.62.240:7240", "106.54.62.240:7242"]
                )
                if (p := endpoint(x))
            )
        )

    def configured(self) -> list[str]:
        defaults = ["CN_X1", "CN_X3_GK", "CN_X4"]
        return self.strings("servers", defaults) or defaults

    def _notify(self) -> None:
        previous, self.changed = self.changed, asyncio.Event()
        previous.set()

    def _spawn(self, coro, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=f"warband-{name}")
        self._tasks.add(task)

        def finished(done):
            self._tasks.discard(done)
            self._notify()
            if not done.cancelled() and done.exception():
                self.logger.warning("战团后台任务失败：%s", done.exception())

        task.add_done_callback(finished)
        return task

    async def start(self) -> None:
        async with self._start_lock:
            if self._started or self._closed:
                return
            if self.path:
                try:
                    rows = await asyncio.to_thread(self._read_snapshot)
                    self._restore(rows)
                except (OSError, ValueError, TypeError) as exc:
                    self.logger.warning("跳过不可用的战团缓存：%s", exc)
            self._started = True
            self._spawn(self._supervise(self._refresh_loop), "refresh")
            self._spawn(self._supervise(self._master_loop), "master")
            self._spawn(self._supervise(self._sweep_loop), "sweep")
            if self.path:
                self._spawn(self._supervise(self._save_loop), "save")

    async def _supervise(self, factory: Callable) -> None:
        """单轮意外失败可恢复；取消信号始终交还给生命周期管理。"""
        while not self._closed:
            try:
                await factory()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 长驻任务需从单轮意外错误恢复
                self.logger.warning("战团后台任务稍后重试：%s", exc)
                await asyncio.sleep(5)

    async def stop(self) -> None:
        async with self._start_lock:
            self._closed = True
            tasks = list(self._tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._inflight.clear()
            if self._dirty and self.path:
                await self._save()
            self._notify()

    def _reindex(self) -> None:
        self.name_index.clear()
        for pair, rec in self.records.items():
            if rec.name and not self.excluded(rec.name):
                self.name_index.setdefault(normalize(rec.name), set()).add(pair)

    def matching(self, name: str) -> list[Endpoint]:
        matches = sorted(
            p
            for p in self.name_index.get(normalize(name), ())
            if not self.excluded(self.records[p].name)
        )
        fresh = [p for p in matches if self.state(self.records[p]) == "fresh"]
        # 两台都新鲜时保留歧义；新地址正常且旧地址已经失效时自动跟随迁移。
        if len(fresh) == 1 and all(
            p in fresh or self.state(self.records[p]) == "unresponsive" for p in matches
        ):
            return fresh
        return matches

    def visible(self, pair: Endpoint) -> bool:
        rec = self.records.get(pair)
        return not (rec and rec.name and self.excluded(rec.name))

    def touch(self, pair: Endpoint) -> None:
        self.hot[pair] = time.monotonic()
        if len(self.hot) > 128:
            self.hot.pop(min(self.hot, key=self.hot.get))

    def priority_endpoints(self) -> list[Endpoint]:
        now = time.monotonic()
        self.hot = {p: t for p, t in self.hot.items() if now - t < HOT_TTL}
        pairs = self.extras() + list(self.hot)
        for name in self.configured():
            pairs.extend(self.matching(name))
        # 默认展示的 CN_X 系列仍保持刷新，其他发现结果不自动升级为高频目标。
        pairs.extend(
            p for p, rec in self.records.items() if rec.name.upper().startswith("CN_X")
        )
        seeds = set(self.strings("seed_hosts", ["116.62.36.206"]))
        pairs.extend(p for p in self.master if p[0] in seeds)
        return [p for p in dict.fromkeys(pairs) if self.visible(p)]

    async def probe(self, pair: Endpoint) -> dict | None:
        """同端点共享任务；取消某个等待者不会取消网络工作。"""
        if self._closed:
            return None
        task = self._inflight.get(pair)
        if task is None:
            if time.monotonic() - self._attempt_mono.get(pair, -math.inf) < 3:
                rec = self.records.get(pair)
                return rec.stats if rec and not rec.failures else None
            task = self._spawn(self._probe_one(pair), "probe")
            self._inflight[pair] = task

            def remove(done):
                if self._inflight.get(pair) is done:
                    self._inflight.pop(pair, None)

            task.add_done_callback(remove)
        else:
            self.metrics["shared_probes"] += 1
        return await asyncio.shield(task)

    async def _probe_one(self, pair: Endpoint) -> dict | None:
        async with self._sem:
            started = time.monotonic()
            stats = await net.fetch_server_stats(
                *pair, self.number("probe_timeout", 4, 0.1, 30)
            )
            self.metrics["probe_seconds"] += time.monotonic() - started
            self.metrics["probes"] += 1
            if self._closed:
                return None
            self._attempt_mono[pair] = time.monotonic()
            rec = self.records.setdefault(pair, Record())
            old_name = rec.name
            rec.attempted = time.time()
            if stats and stats.get("name"):
                stats = {
                    k: (str(v)[:256] if v is not None else None)
                    for k, v in stats.items()
                }
                if self.excluded(str(stats["name"])):
                    # 留下名称墓碑用于隐藏固定端点，不保存其状态内容。
                    rec.stats = {"name": str(stats["name"])}
                    rec.success = 0
                    rec.success_mono = None
                else:
                    rec.stats = stats
                    rec.success = rec.attempted
                    rec.success_mono = time.monotonic()
                rec.failures = 0
                rec.restored = False
            else:
                rec.failures += 1
                self.metrics["failed_probes"] += 1
            if old_name != rec.name:
                self._reindex()
            self._dirty = True
            self._prune()
            self._notify()
            return stats

    async def _scan(self, pairs: list[Endpoint], background: bool = False) -> bool:
        """固定数量工作者，逐条写入结果，不为整个列表创建成千任务。"""
        iterator = iter(dict.fromkeys(pairs))
        complete = True

        async def worker():
            nonlocal complete
            for pair in iterator:
                if not await self.probe(pair):
                    complete = False

        count = min(
            len(pairs),
            max(1, self.concurrency // 4) if background else self.concurrency,
        )
        tasks = [asyncio.create_task(worker()) for _ in range(count)]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return complete

    async def master_list(self) -> list[Endpoint]:
        now = time.monotonic()
        ttl = self.number("master_refresh_interval", 180, 30, 3600)
        if self.master and now - self.master_updated < ttl:
            return self.master
        if self._master_task is not None and not self._master_task.done():
            return await asyncio.shield(self._master_task)
        if self._closed or now - self.master_attempt < 30:
            return self.master
        self._master_task = self._spawn(self._fetch_master(), "master-request")
        return await asyncio.shield(self._master_task)

    async def _fetch_master(self) -> list[Endpoint]:
        self.master_attempt = time.monotonic()
        started = time.monotonic()
        try:
            addrs = await net.fetch_master_server_list(
                str(
                    self.cfg(
                        "master_url",
                        "https://warbandmain.taleworlds.com/handlerservers.ashx?type=list",
                    )
                ),
                timeout=12,
            )
            if not addrs:
                raise ValueError("主列表为空")
            unique = list(dict.fromkeys(addrs))
            self.master_complete = len(unique) <= 8192
            self.master = unique[:8192]
            self.master_ok = True
            self.master_updated = time.monotonic()
            self.refresh_wake.set()
        except (
            OSError,
            ValueError,
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ) as exc:
            self.master_ok = False
            self.logger.warning("战团主列表暂不可用，继续直查已知端点：%s", exc)
        finally:
            self.metrics["master_seconds"] += time.monotonic() - started
            self.metrics["master_requests"] += 1
            self._notify()
        return self.master

    async def _master_loop(self) -> None:
        while not self._closed:
            await self.master_list()
            await asyncio.sleep(30)

    async def _refresh_loop(self) -> None:
        while not self._closed:
            self.refresh_wake.clear()
            started = time.monotonic()
            await self._scan(self.priority_endpoints())
            self._notify()
            self.logger.debug("战团刷新：%s", dict(self.metrics))
            interval = self.number("refresh_interval", 45, 10, 3600)
            try:
                await asyncio.wait_for(
                    self.refresh_wake.wait(),
                    max(1, interval - (time.monotonic() - started)),
                )
            except asyncio.TimeoutError:
                pass

    async def _sweep_loop(self) -> None:
        while not self._closed:
            addrs = await self.master_list()
            if not self._discovery_task or self._discovery_task.done():
                priority = set(self.priority_endpoints())
                others = [p for p in addrs if p not in priority]
                if others:
                    count = min(len(others), int(self.number("max_probe", 60, 1, 512)))
                    selected = [
                        others[(self._sweep + i) % len(others)] for i in range(count)
                    ]
                    self._sweep = (self._sweep + count) % len(others)
                    await self._scan(selected, background=True)
            await asyncio.sleep(self.number("refresh_interval", 45, 10, 3600))

    async def _discover(self) -> bool:
        addrs = await self.master_list()
        if not addrs:
            return False
        now = time.monotonic()
        todo = [
            p
            for p in addrs
            if now - self._attempt_mono.get(p, -math.inf)
            >= (90 if self.records.get(p) and not self.records[p].failures else 15)
        ]
        success = await self._scan(todo, background=True)
        self._discovery_completed = time.monotonic()
        # 只有有效目录且所有候选有成功响应，才能短期复用确定的未找到结果。
        self._discovery_success = (
            success
            and self.master_ok
            and self.master_complete
            and all(self.records.get(p) and not self.records[p].failures for p in addrs)
        )
        return self._discovery_success

    async def discover(self, name: str, budget: float) -> str:
        if self.matching(name):
            return "found"
        if not self.cfg("discovery_enabled", True) or self._closed:
            return "disabled"
        age = time.monotonic() - self._discovery_completed
        if age < (45 if self._discovery_success else 3):
            return "missing" if self._discovery_success else "unavailable"
        if self._discovery_task is None or self._discovery_task.done():
            self._discovery_task = self._spawn(self._discover(), "discovery")
        deadline = time.monotonic() + budget
        while not self._closed:
            changed = self.changed
            if self.matching(name):
                return "found"
            if self._discovery_task.done():
                return "missing" if self._discovery_task.result() else "unavailable"
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return "pending"
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except asyncio.TimeoutError:
                return "pending"
        return "unavailable"

    async def wait_initial(self, pairs: list[Endpoint], budget: float) -> None:
        """只等待目标首次尝试；无目标地址时等待目录与刷新事件。"""
        for pair in pairs:
            self.touch(pair)
        self.refresh_wake.set()
        deadline = time.monotonic() + budget
        while not self._closed:
            changed = self.changed
            if pairs and all(p in self.records for p in pairs):
                return
            if not pairs and any(p in self.records for p in self.priority_endpoints()):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(changed.wait(), remaining)
            except asyncio.TimeoutError:
                return

    def state(self, rec: Record | None) -> str:
        if rec is None or not rec.attempted:
            return "unknown"
        if not rec.success:
            return "unresponsive"
        age = (
            max(0, time.monotonic() - rec.success_mono)
            if rec.success_mono is not None
            else max(0, time.time() - rec.success)
        )
        soft = self.number("max_cache_age", 60, 0, 3600)
        if age > max(300, soft * 5) or rec.failures >= 3:
            return "unresponsive"
        return "stale" if age > soft or rec.failures or rec.restored else "fresh"

    def _prune(self) -> None:
        protected = set(self.extras()) | set(self.hot)
        for name in self.configured():
            protected.update(self.matching(name))
        now = time.time()
        victims = sorted(
            (p for p in self.records if p not in protected),
            key=lambda p: self.records[p].attempted,
        )
        removed = False
        for pair in victims:
            if (
                len(self.records) > MAX_RECORDS
                or now - self.records[pair].attempted > RETENTION
            ):
                self.records.pop(pair)
                self._attempt_mono.pop(pair, None)
                removed = True
        # 即使大量重名或配置端点都被保护，内存也不能突破硬上限。
        while len(self.records) > MAX_RECORDS:
            pair = min(self.records, key=lambda p: self.records[p].attempted)
            self.records.pop(pair)
            self._attempt_mono.pop(pair, None)
            removed = True
        if removed:
            self._reindex()

    def _read_snapshot(self) -> list:
        if not self.path.exists():
            return []
        with self.path.open("rb") as handle:
            raw = handle.read(2_000_001)
        if len(raw) > 2_000_000:
            raise ValueError("缓存文件过大")
        data = json.loads(raw)
        if not isinstance(data, dict) or data.get("version") != 1:
            raise ValueError("不支持的缓存版本")
        rows = data.get("records")
        if not isinstance(rows, list) or len(rows) > MAX_RECORDS:
            raise ValueError("缓存记录数量无效")
        return rows

    def _restore(self, rows: list) -> None:
        now = time.time()
        for row in rows:
            try:
                pair = endpoint(row["addr"])
                stats = row["stats"]
                success, attempted = float(row["success"]), float(row["attempted"])
                if (
                    not pair
                    or not isinstance(stats, dict)
                    or not stats.get("name")
                    or self.excluded(str(stats["name"]))
                    or not all(
                        math.isfinite(t) and 0 <= t <= now + 5
                        for t in (success, attempted)
                    )
                    or now - attempted > RETENTION
                ):
                    continue
                # 只恢复有限长度的标量字段，不让损坏缓存扩大内存或回复。
                clean = {
                    k: str(v)[:256]
                    for k, v in stats.items()
                    if k
                    in {
                        "name",
                        "module",
                        "map_type",
                        "map_name",
                        "map_id",
                        "players",
                        "max_players",
                        "has_password",
                        "version",
                    }
                    and isinstance(v, (str, int, float))
                }
                self.records[pair] = Record(
                    clean,
                    success,
                    attempted,
                    max(0, min(100, int(row.get("failures", 0)))),
                    True,
                    time.monotonic() - max(0, now - success),
                )
            except (KeyError, ValueError, TypeError, OverflowError):
                continue
        self._reindex()

    def _write_snapshot(self, rows: list) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        # 与读取上限一致；极端长字段时优先保存近期成功记录。
        rows = sorted(rows, key=lambda r: r["success"], reverse=True)
        while True:
            data = json.dumps(
                {"version": 1, "records": rows}, ensure_ascii=False
            ).encode("utf-8")
            if len(data) <= 2_000_000:
                break
            rows = rows[: max(0, len(rows) * 3 // 4)]
        temporary.write_bytes(data)
        temporary.replace(self.path)

    async def _save(self) -> None:
        rows = [
            dict(
                addr=label(p),
                **{
                    k: v
                    for k, v in asdict(r).items()
                    if k not in {"restored", "success_mono"}
                },
            )
            for p, r in self.records.items()
            if r.success and not self.excluded(r.name)
        ][:MAX_RECORDS]
        self._dirty = False
        task = asyncio.create_task(asyncio.to_thread(self._write_snapshot, rows))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # 等待已经开始的原子写入结束，重载后不会留下旧写入者。
            try:
                await task
            except OSError as exc:
                self._dirty = True
                self.logger.warning("战团缓存保存失败：%s", exc)
            raise
        except OSError as exc:
            self._dirty = True
            self.logger.warning("战团缓存保存失败：%s", exc)

    async def _save_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(10)
            if self._dirty:
                await self._save()
