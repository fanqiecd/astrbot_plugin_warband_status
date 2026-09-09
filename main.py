"""AstrBot 插件：骑马与砍杀：战团联机服务器状态查询。

查询骑砍战团联机服务器的在线人数、游戏模式、当前地图与模块：
- 内置骑砍中文站 CN_X 系列（CN_X1 / CN_X3_GK / CN_X4 等，经官方主列表发现）；
- 支持固定端点（extra_endpoints），每轮直接探测并展示（如 CN_YJMD 服
  106.54.62.240:7240 / 106.54.62.240:7242）。

调用方式：
- 命令：@机器人 / 唤醒前缀 + 「查服 [目标]」，如「查服 X1」「查服 CN_X3_GK」「查服 全部」
- 关键字：直接发送「查服」「查询服务器」「服务器状态」「骑砍服务器」等（可跟目标，如「查服 X3」）
- 粘连写法：关键字与目标可无空格，如「查服CN_Xc_shanghai」；@ 机器人或唤醒后整句
  就是服务器名也可直查，如「@机器人CN_Xc_shanghai」

数据链路：后台定时抓取 TaleWorlds 官方主服务器列表，只对已知 CN 主机、固定端点
与抽样服务器做 TCP 探测（服务端直推 <ServerStats> XML），结果缓存在内存中；
收到查询时优先读缓存，保证回复速度快。
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star

try:
    from . import warband_net as wnet
except ImportError:  # 插件目录以普通目录加载时走绝对导入
    import warband_net as wnet

PLUGIN_NAME = "astrbot_plugin_warband_status"
DEFAULT_MASTER_URL = "https://warbandmain.taleworlds.com/handlerservers.ashx?type=list"
DEFAULT_SERVERS = ["CN_X1", "CN_X3_GK", "CN_X4"]
DEFAULT_SEED_HOSTS = ["116.62.36.206"]
DEFAULT_EXTRA_ENDPOINTS = ["106.54.62.240:7240", "106.54.62.240:7242"]
DEFAULT_EXCLUDE_SERVERS = ["CN_X4_zuikuai"]

NAME_RE = re.compile(r"^CN_X", re.IGNORECASE)
# 关键字触发：关键词后可跟目标（可无空格），如「查服X1」「查询服务器 CN_X4」；
# 或整句就是一个 CN_ 服务器名（供唤醒后直查，如 @机器人CN_Xc_shanghai）
KEYWORD_RE = re.compile(
    r"^\s*(?:(?:查服|查询服务器|服务器状态|骑砍服务器|骑砍状态|查骑砍)\s*(\S.*?)?"
    r"|(CN_[A-Za-z0-9_\-]{1,64}))\s*$",
    re.IGNORECASE,
)
BLOCK_SEP = "---------------------------------"


def endpoint_label(pair: tuple[str, int]) -> str:
    """把 (ip, port) 转成端点标签。"""
    return f"{pair[0]}:{pair[1]}"


class WarbandServerStatusPlugin(Star):
    """骑砍战团联机服务器状态查询。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._cache: dict[str, dict[str, Any]] = {}
        self._offline_since: dict[str, float] = {}
        self._endpoint_last: dict[str, str] = {}
        self._known_hosts: set[str] = set()
        self._last_refresh: float | None = None
        self._sweep_offset = 0
        self._refresh_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._discover_lock = asyncio.Lock()
        self._last_probe_all: dict[tuple[str, int], float] = {}
        self._bg_task: asyncio.Task | None = None

    # ---------- 配置 ----------

    def _cfg(self, key: str, default: Any) -> Any:
        try:
            value = self.config.get(key, default)
        except Exception:  # noqa: BLE001 - 配置对象异常时回退默认值
            return default
        return default if value is None else value

    def _whitelist(self, key: str) -> set[str]:
        raw = self._cfg(key, [])
        return {str(item).strip() for item in raw or [] if str(item).strip()}

    def _excluded(self) -> set[str]:
        """需要隐藏、不参与查询展示的服务器名（默认排除 CN_X4_zuikuai）。"""
        raw = self._cfg("exclude_servers", DEFAULT_EXCLUDE_SERVERS)
        return {str(item).strip() for item in raw or [] if str(item).strip()}

    def _servers(self) -> list[str]:
        raw = self._cfg("servers", DEFAULT_SERVERS)
        names: list[str] = []
        for item in raw or []:
            name = str(item).strip()
            if name and name not in names:
                names.append(name)
        return names or list(DEFAULT_SERVERS)

    def _extra_endpoints(self) -> list[tuple[str, int]]:
        """解析固定端点配置为 (ip, port) 列表，去重保序。"""
        raw = self._cfg("extra_endpoints", DEFAULT_EXTRA_ENDPOINTS) or []
        pairs: list[tuple[str, int]] = []
        for item in raw:
            text = str(item).strip()
            if not text:
                continue
            ip, _, port_s = text.partition(":")
            ip = ip.strip()
            if not ip:
                continue
            try:
                port = int(port_s) if port_s else wnet.DEFAULT_PORT
            except ValueError:
                continue
            if not 0 < port < 65536:
                continue
            pair = (ip, port)
            if pair not in pairs:
                pairs.append(pair)
        return pairs

    def _ordered_keys(self) -> list[str]:
        """可展示条目（有序）：配置服务器 → 固定端点（用最后已知名或端点）→ 发现的 CN_X*。

        返回的每个 key 要么是缓存中的服务器名，要么是 "ip:port" 端点标签，
        二者都可用作查询目标。
        """

        excluded = self._excluded()

        def _add(key: str) -> None:
            if key and key not in keys and key not in excluded:
                keys.append(key)

        keys: list[str] = []
        for name in self._servers():
            _add(name)
        for pair in self._extra_endpoints():
            label = endpoint_label(pair)
            _add(self._endpoint_last.get(label) or label)
        for name in sorted(self._cache):
            if NAME_RE.match(name):
                _add(name)
        for name in sorted(self._offline_since):
            if NAME_RE.match(name):
                _add(name)
        return keys

    # ---------- 权限开关 ----------

    def _allowed(self, event: AstrMessageEvent) -> bool:
        """按群聊/私聊开关与白名单判断该事件是否可用本功能。"""
        sender = str(event.get_sender_id() or "")
        if event.is_private_chat():
            if not bool(self._cfg("enable_private", True)):
                return False
            user_wl = self._whitelist("user_whitelist")
            return (not user_wl) or (bool(sender) and sender in user_wl)
        if not bool(self._cfg("enable_group", True)):
            return False
        group_id = str(event.get_group_id() or "")
        group_wl = self._whitelist("group_whitelist")
        if group_wl and (not group_id or group_id not in group_wl):
            return False
        user_wl = self._whitelist("user_whitelist")
        return (not user_wl) or (bool(sender) and sender in user_wl)

    # ---------- 数据刷新 ----------

    async def _ensure_bg_task(self) -> None:
        """确保后台定时刷新任务在运行（首次查询 / 启动 / 重载后都会触发）。"""
        async with self._start_lock:
            if self._bg_task is not None and not self._bg_task.done():
                return
            try:
                self._bg_task = asyncio.create_task(
                    self._bg_loop(),
                    name=f"{PLUGIN_NAME}-refresh",
                )
            except RuntimeError:  # pragma: no cover - 无事件循环的极端情况
                self.logger.warning("当前没有运行中的事件循环，跳过后台刷新任务。")

    async def _bg_loop(self) -> None:
        while True:
            try:
                await self._run_refresh(quick_only=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 后台任务需吞掉单次失败
                self.logger.warning("后台刷新服务器数据失败: %s", exc)
            interval = max(10, int(self._cfg("refresh_interval", 45)))
            await asyncio.sleep(interval)

    async def _run_refresh(self, quick_only: bool = True) -> bool:
        """执行一轮刷新。

        快速模式（回复路径用）只探测已知 CN 主机/种子主机的所有在列表中的地址；
        完整模式（后台用）额外按游标抽样探测其他服务器用于发现新主机。

        Args:
            quick_only: 是否只做快速探测。

        Returns:
            是否成功完成（主列表拉取成功）。
        """
        if self._refresh_lock.locked():
            return False
        async with self._refresh_lock:
            url = str(self._cfg("master_url", DEFAULT_MASTER_URL))
            try:
                addrs = await wnet.fetch_master_server_list(url, timeout=12.0)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("抓取战团主服务器列表失败: %s", exc)
                return False
            if not addrs:
                self.logger.warning("战团主服务器列表为空。")
                return False
            addr_strs = {f"{ip}:{port}" for ip, port in addrs}
            timeout = float(self._cfg("probe_timeout", 4))

            hosts = {
                str(item).strip()
                for item in self._cfg("seed_hosts", DEFAULT_SEED_HOSTS)
                if str(item).strip()
            }
            hosts |= self._known_hosts
            quick = [(ip, port) for ip, port in addrs if ip in hosts]
            if quick:
                for ip, port, stats in await wnet.probe_many(quick, timeout):
                    self._record_probe(ip, port, stats)

            # 固定端点：每轮直接探测（不依赖主服务器列表），无论名称是否 CN_X 都收录
            extra = self._extra_endpoints()
            if extra:
                for ip, port, stats in await wnet.probe_many(extra, timeout):
                    label = endpoint_label((ip, port))
                    self._record_probe(ip, port, stats)
                    name = (stats.get("name") or "").strip() if stats else ""
                    if name:
                        self._endpoint_last[label] = name
                    else:
                        # 无响应：移除该端点对应的缓存记录，保留最后已知名供离线展示
                        for cached_name, rec in list(self._cache.items()):
                            if rec.get("addr") == label:
                                self._cache.pop(cached_name, None)
                                self._offline_since[cached_name] = time.time()

            if not quick_only:
                quick_set = {f"{ip}:{port}" for ip, port in quick}
                others = [
                    (ip, port) for ip, port in addrs if f"{ip}:{port}" not in quick_set
                ]
                if others:
                    limit = min(max(1, int(self._cfg("max_probe", 60))), len(others))
                    offset = self._sweep_offset % len(others)
                    chosen = [others[(offset + i) % len(others)] for i in range(limit)]
                    self._sweep_offset = (offset + limit) % len(others)
                    for ip, port, stats in await wnet.probe_many(chosen, timeout):
                        self._record_probe(ip, port, stats)

            # 下线复核：缓存中但已不在主列表的地址直接复查一次
            for name, rec in list(self._cache.items()):
                addr = rec.get("addr", "")
                if not addr or addr in addr_strs:
                    continue
                ip_s, _, port_s = addr.partition(":")
                try:
                    port_i = int(port_s)
                except ValueError:
                    continue
                stats = await wnet.fetch_server_stats(ip_s, port_i, timeout)
                self._last_probe_all[(ip_s, port_i)] = time.time()
                if stats is None or (stats.get("name") or "") != name:
                    self._cache.pop(name, None)
                    self._offline_since[name] = time.time()

            self._last_refresh = time.time()
            return True

    def _record_probe(self, ip: str, port: int, stats: dict[str, Any] | None) -> None:
        """记录一次探测（供按需发现去重），并写入缓存。"""
        self._last_probe_all[(ip, port)] = time.time()
        self._apply_result(ip, port, stats)

    def _apply_result(self, ip: str, port: int, stats: dict[str, Any] | None) -> None:
        """写入一次探测结果。

        只要服务器返回了名称就收录（供按名称查询），但受 exclude_servers 排除。

        Args:
            ip: 服务器 IP。
            port: 服务器端口。
            stats: 解析出的服务器信息。
        """
        if not stats:
            return
        name = (stats.get("name") or "").strip()
        if not name:
            return
        if name in self._excluded():
            return  # 配置排除的服务器不缓存、不展示
        self._known_hosts.add(ip)
        self._cache[name] = {
            "name": name,
            "addr": f"{ip}:{port}",
            "ts": time.time(),
            **stats,
        }
        self._offline_since.pop(name, None)

    @staticmethod
    def _is_cn_like_name(raw: str) -> bool:
        """判断目标是否可能是 CN_ 前缀的服务器名（用于触发按需发现）。"""
        key = re.sub(r"[\s_\-]", "", raw).strip().lower().removeprefix("cn")
        return bool(re.fullmatch(r"[a-z0-9]{2,30}", key))

    async def _discover_names(self) -> None:
        """按需全量探测：用于查询尚未被收录的服务器名（如 CN_Swiss_02）。

        只探测近期未探过的地址，探测结果进入缓存后即可按名称查询。
        """
        if not bool(self._cfg("discovery_enabled", True)):
            return
        if self._refresh_lock.locked() or self._discover_lock.locked():
            return  # 已有刷新/发现在进行，稍后即可命中缓存
        async with self._discover_lock:
            url = str(self._cfg("master_url", DEFAULT_MASTER_URL))
            try:
                addrs = await wnet.fetch_master_server_list(url, timeout=12.0)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("按需发现失败（主服务器列表不可达）: %s", exc)
                return
            if not addrs:
                return
            now = time.time()
            todo = [a for a in addrs if now - self._last_probe_all.get(a, 0.0) > 90.0]
            if not todo:
                return
            timeout = float(self._cfg("probe_timeout", 4))
            self.logger.info("按名称查询触发在线发现，探测 %d 台服务器 ...", len(todo))
            for ip, port, stats in await wnet.probe_many(todo, timeout, concurrency=64):
                self._record_probe(ip, port, stats)

    # ---------- 目标解析与格式化 ----------

    def _resolve_target(self, raw: str, names: list[str]) -> str | None:
        """把用户输入解析成服务器名。

        支持：全名（CN_X3_GK / CN_Swiss_02）、简称（X3 / GK / swiss02）、
        别名（全部 / all）、端点端口号（7240）等；除传入的有序名单外，
        也会匹配缓存中已收录的任何服务器名。

        Args:
            raw: 用户输入。
            names: 可用的服务器名（有序）。

        Returns:
            服务器名；"all" 表示查询全部；无法识别返回 None。
        """
        key = re.sub(r"[\s_\-]", "", raw).strip().lower()
        if not key:
            return None
        key = key.removeprefix("cn")
        if key in ("全部", "所有", "all", "*"):
            return "all"
        # 固定端点：完整端点（ip:port）或纯端口号匹配
        for pair in self._extra_endpoints():
            label = endpoint_label(pair)
            label_lower = label.lower()
            if key == label_lower or (key.isdigit() and label_lower.endswith(key)):
                return label
        if key.isdigit() and len(key) <= 2:
            key = f"x{key}"
        search = list(names)
        for cached in sorted(self._cache):
            if cached not in search:
                search.append(cached)
        for name in search:
            nk = re.sub(r"[\s_\-]", "", name).lower().removeprefix("cn")
            if key == nk:
                return name
            if len(key) >= 2 and (
                nk.endswith(key) or (key.startswith("x") and nk.startswith(key))
            ):
                return name
        return None

    def _record_for(self, key: str) -> tuple[str, dict[str, Any] | None]:
        """按显示 key 找缓存记录。

        key 可能是服务器名，也可能是端点标签（此时返回实际服务器名 + 记录）。

        Args:
            key: 服务器名或 "ip:port" 端点标签。

        Returns:
            (展示用服务器名, 缓存记录或 None)。
        """
        rec = self._cache.get(key)
        if rec is not None:
            return key, rec
        for name, item in self._cache.items():
            if item.get("addr") == key:
                return name, item
        return key, None

    def _format_block(self, name: str, rec: dict[str, Any] | None) -> str:
        if rec is None:
            return f"服务器名称：{name}\n状态：未在线（服务器无响应或未开服）"
        return "\n".join(
            [
                f"服务器名称：{rec.get('name') or name}",
                f"游戏模式：{rec.get('map_type') or '未知'}",
                f"当前地图：{rec.get('map_name') or '未知'}",
                f"当前模块：{rec.get('module') or '未知'}",
                f"在线人数：{rec.get('players') or '0'}/{rec.get('max_players') or '未知'}",
            ]
        )

    async def _build_reply(
        self,
        event: AstrMessageEvent,
        target_text: str,
        *,
        silent_gate: bool,
    ) -> str | None:
        """构造查询回复文本；不满足开关/白名单时返回 None（静默）或提示。"""
        if not self._allowed(event):
            if silent_gate:
                return None
            if event.is_private_chat():
                return "⚠ 私聊查询功能未开启，或您不在白名单内。"
            return "⚠ 当前群未启用查询功能，或不在白名单内。"

        await self._ensure_bg_task()
        keys = self._ordered_keys()
        target = (target_text or "").strip()
        if target:
            resolved = self._resolve_target(target, keys)
            if resolved == "all":
                target = ""
            elif resolved is None and self._is_cn_like_name(target):
                # 可能是尚未收录的 CN_ 系列服务器：先在线全量发现一次再解析
                await self._discover_names()
                resolved = self._resolve_target(target, self._ordered_keys())
            if resolved == "all":
                target = ""
            elif resolved is None:
                avail = "、".join(keys) or "（暂无可查询的服务器）"
                return (
                    f"未识别到服务器「{target}」。\n"
                    f"可查询：{avail}\n"
                    "示例：查服 X1 / 查服 CN_Swiss_02 / 查服 全部"
                )
            elif target and resolved:
                target = resolved

        # 数据新鲜度：超过阈值才同步刷新一次（快速模式，通常 1~2 秒）
        max_age = max(0, int(self._cfg("max_cache_age", 60)))
        now = time.time()
        if self._last_refresh is None or (now - self._last_refresh) > max_age:
            await self._run_refresh(quick_only=True)

        show_offline = bool(self._cfg("show_offline", True))
        wanted = [target] if target else keys
        blocks: list[str] = []
        for key in wanted:
            display, rec = self._record_for(key)
            if rec is None and not target and not show_offline:
                continue
            blocks.append(self._format_block(display, rec))
        if not blocks:
            return "当前没有可展示的服务器信息，请稍后重试。"
        return f"\n{BLOCK_SEP}\n".join(blocks)

    # ---------- 生命周期 ----------

    @filter.on_astrbot_loaded()
    async def _on_astrbot_loaded(self) -> None:
        """AstrBot 启动完成后启动后台刷新以预热缓存。"""
        await self._ensure_bg_task()

    async def terminate(self) -> None:
        """插件卸载/重载时取消后台刷新任务。"""
        if self._bg_task is not None:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001,S110 - 取消后台任务无需上报
                pass
            self._bg_task = None

    # ---------- 指令 / 关键字 ----------

    @filter.command(
        "查服",
        alias={"查询服务器", "服务器状态", "骑砍服务器", "骑砍状态"},
    )
    async def cmd_query(self, event: AstrMessageEvent, target: str = ""):
        """查询骑砍战团联机服务器状态。

        可带目标参数：X1 / X3 / X4 / CN_X3_GK / CN_YJMD_X2 / 106.54.62.240:7240
        或 全部；不带参数默认查询全部。
        """
        reply = await self._build_reply(event, target, silent_gate=False)
        if reply:
            yield event.plain_result(reply)

    @staticmethod
    def _keyword_glued(text: str, m: re.Match) -> bool:
        """关键字与跟随目标之间是否无空格粘连（如「查服CN_Xc_shanghai」）。

        指令路径要求「命令 + 空格 + 参数」，无法命中粘连写法，需要关键字路径兜底。

        Args:
            text: 已去首尾空白的消息文本。
            m: 对该文本的 KEYWORD_RE 匹配结果。

        Returns:
            关键字与目标粘连时为 True；仅有关键字本身时为 False。
        """
        rest_start = m.start(1)
        if rest_start < 0:
            return False
        return not text[rest_start - 1].isspace()

    @filter.regex(KEYWORD_RE)
    async def keyword_query(self, event: AstrMessageEvent):
        """关键字触发查询，直接发送「查服」「服务器状态」等即可（无需 @ 或前缀）。

        已唤醒（@ / 唤醒前缀 / 私聊默认）时通常交给指令路径按空格分词处理，避免重复回复；
        仅当关键字与目标无空格粘连（如「查服CN_Xc_shanghai」）、或整句就是 CN_ 服务器名
        （如「@机器人CN_Xc_shanghai」）时才在此接管——这两种写法指令路径无法命中。
        """
        text = (event.get_message_str() or "").strip()
        match = KEYWORD_RE.match(text)
        if not match:
            return
        rest: str = (match.group(1) or "").strip()
        bare_name = match.group(2)
        if event.is_at_or_wake_command:
            # 已唤醒：空格分隔的关键字指令由指令路径回复，这里只兜底粘连/裸名写法
            if bare_name is not None:
                rest = bare_name
            elif not self._keyword_glued(text, match):
                return
        else:
            # 未唤醒：裸服务器名不触发；直接发关键字受 enable_keyword 开关约束
            if bare_name is not None or not bool(self._cfg("enable_keyword", True)):
                return
        if rest:
            resolved = self._resolve_target(rest, self._ordered_keys())
            if resolved is None and not self._is_cn_like_name(rest):
                return  # 如「查服 一下」这类闲聊，不回复
        reply = await self._build_reply(event, rest, silent_gate=True)
        if reply:
            yield event.plain_result(reply)
