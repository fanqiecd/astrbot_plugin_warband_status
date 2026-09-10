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

import re
import time
from pathlib import Path
from typing import Any

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star

try:
    from .warband_service import WarbandService, endpoint, label, normalize
except ImportError:  # 插件目录以普通目录加载时走绝对导入
    from warband_service import WarbandService, endpoint, label, normalize

PLUGIN_NAME = "astrbot_plugin_warband_status"

NAME_RE = re.compile(r"^CN_X", re.IGNORECASE)
# 关键字触发：关键词后可跟目标（可无空格），如「查服X1」「查询服务器 CN_X4」；
# 或整句就是一个 CN_ 服务器名（供唤醒后直查，如 @机器人CN_Xc_shanghai）
KEYWORD_RE = re.compile(
    r"^\s*(?:(?:查服|查询服务器|服务器状态|骑砍服务器|骑砍状态|查骑砍)\s*(\S.*?)?"
    r"|(CN_[A-Za-z0-9_\-]{1,64}))\s*$",
    re.IGNORECASE,
)
BLOCK_SEP = "---------------------------------"


class WarbandServerStatusPlugin(Star):
    """骑砍战团联机服务器状态查询。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        if not hasattr(self, "logger"):
            self.logger = logger
        self.config = config
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path

        data_path = (
            Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME / "servers.json"
        )
        self.service = WarbandService(self._cfg, self.logger, data_path)

    # ---------- 配置 ----------

    def _cfg(self, key: str, default: Any) -> Any:
        try:
            value = self.config.get(key, default)
        except Exception:  # noqa: BLE001 - 配置对象异常时回退默认值
            return default
        return default if value is None else value

    def _whitelist(self, key: str) -> set[str]:
        return set(self.service.strings(key, []))

    def _servers(self) -> list[str]:
        return self.service.configured()

    def _extra_endpoints(self) -> list[tuple[str, int]]:
        return self.service.extras()

    def _ordered_keys(self) -> list[str]:
        """配置顺序优先，重名时展示端点，排除项在所有入口一致生效。"""
        keys = []

        def add(key):
            if key not in keys:
                keys.append(key)

        def add_name(name):
            if self.service.excluded(name):
                return
            matches = self.service.matching(name)
            if len(matches) > 1:
                for pair in matches:
                    add(label(pair))
            else:
                add(self.service.records[matches[0]].name if matches else name)

        for name in self._servers():
            add_name(name)
        for pair in self._extra_endpoints():
            if not self.service.visible(pair):
                continue
            rec = self.service.records.get(pair)
            if rec and rec.name and self.service.matching(rec.name) == [pair]:
                add_name(rec.name)
            else:
                add(label(pair))
        for rec in sorted(self.service.records.values(), key=lambda r: r.name):
            if NAME_RE.match(rec.name):
                add_name(rec.name)
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

    # ---------- 查询与状态展示 ----------

    @staticmethod
    def _is_cn_like_name(raw: str) -> bool:
        return bool(re.fullmatch(r"[a-z0-9]{2,64}", normalize(raw)))

    def _candidates(self, raw: str, names: list[str], fuzzy: bool = True) -> list[str]:
        key = normalize(raw)
        if key in {"全部", "所有", "all", "*"}:
            return ["all"]
        # 完整端点和端口号只能匹配已知地址，不把用户输入变成任意网络探测。
        available = set(self._extra_endpoints()) | set(self.service.records)
        if ":" in raw:
            pair = endpoint(raw)
            return (
                [label(pair)]
                if pair in available and self.service.visible(pair)
                else []
            )
        if key.isdigit() and len(key) > 2:
            return [
                label(p)
                for p in sorted(available)
                if p[1] == int(key) and self.service.visible(p)
            ]
        if key.isdigit():
            key = "x" + key
        # matching 接受原始名称；补回前缀，防止 CN_CNxxx 被重复剥离。
        exact = self.service.matching("CN_" + key)
        if exact:
            return (
                [label(p) for p in exact]
                if len(exact) > 1
                else [self.service.records[exact[0]].name]
            )
        search = list(
            dict.fromkeys(
                names + [r.name for r in self.service.records.values() if r.name]
            )
        )
        search = [
            name
            for name in search
            if ":" not in name and not self.service.excluded(name)
        ]
        exact_names = [name for name in search if normalize(name) == key]
        if exact_names:
            return exact_names
        if not fuzzy or len(key) < 2:
            return []
        matches = [
            name
            for name in search
            if normalize(name).endswith(key)
            or (key.startswith("x") and normalize(name).startswith(key))
        ]
        result = []
        for name in matches:
            pairs = self.service.matching(name)
            result.extend([label(p) for p in pairs] if len(pairs) > 1 else [name])
        return list(dict.fromkeys(result))

    def _resolve_target(self, raw: str, names: list[str]) -> str | None:
        matches = self._candidates(raw, names)
        return matches[0] if len(matches) == 1 else None

    def _record_for(self, key: str) -> tuple[str, dict[str, Any] | None]:
        pairs = [endpoint(key)] if ":" in key else self.service.matching(key)
        pair = pairs[0] if len(pairs) == 1 else None
        rec = self.service.records.get(pair)
        if rec is None:
            return key, None
        display = rec.name or key
        if ":" in key and rec.name or len(self.service.matching(rec.name)) > 1:
            display += f"（{label(pair)}）"
        return display, dict(
            rec.stats, ts=rec.success, state=self.service.state(rec), addr=label(pair)
        )

    def _format_block(self, name: str, rec: dict[str, Any] | None) -> str:
        state = rec.get("state") if rec else "unknown"
        if state in {"unknown", "unresponsive"}:
            message = (
                "暂无结果，正在查询，请稍后重试"
                if state == "unknown"
                else "近期无响应（无法确认是否开服）"
            )
            return f"服务器名称：{name}\n状态：{message}"
        lines = [
            f"服务器名称：{name}",
            f"游戏模式：{rec.get('map_type') or '未知'}",
            f"当前地图：{rec.get('map_name') or '未知'}",
            f"当前模块：{rec.get('module') or '未知'}",
            f"在线人数：{rec.get('players') or '0'}/{rec.get('max_players') or '未知'}",
        ]
        if state == "stale":
            stamp = time.strftime("%m-%d %H:%M:%S", time.localtime(rec["ts"]))
            lines.append(f"数据时间：{stamp}（上次结果，正在后台更新）")
        return "\n".join(lines)

    async def _build_reply(
        self, event: AstrMessageEvent, target_text: str = "", *, silent_gate: bool
    ) -> str | None:
        if not self._allowed(event):
            if silent_gate:
                return None
            return "⚠ 当前会话未启用查询功能，或您不在白名单内。"
        started = time.monotonic()
        await self.service.start()
        if self.service._closed:
            return None
        budget = self.service.number("query_wait_timeout", 3, 0.1, 15)
        deadline = time.monotonic() + budget
        target = (target_text or "").strip()
        discovery_status = None
        if target and normalize(target) not in {"all", "全部", "所有", "*"}:
            if self.service.excluded(target):
                return f"服务器「{target}」已被排除。"
            matches = self._candidates(target, self._ordered_keys())
            if not matches and self._is_cn_like_name(target):
                discovery_status = await self.service.discover(target, budget)
                # 扫描中只采用精确名称，完整结束后才使用模糊结果。
                matches = self._candidates(
                    target, self._ordered_keys(), fuzzy=discovery_status != "pending"
                )
            if len(matches) > 1:
                return "匹配到多个服务器，请指定完整名称或端点：\n" + "、".join(matches)
            if not matches:
                if discovery_status == "pending":
                    return (
                        f"正在查询服务器「{target}」，请稍后重试；后续查询将复用结果。"
                    )
                if discovery_status == "unavailable":
                    return f"暂未定位到服务器「{target}」，部分端点或主列表无响应，请稍后重试。"
                return f"未识别到服务器「{target}」。\n可查询：{'、'.join(self._ordered_keys())}"
            target = matches[0]
        else:
            target = ""
        wanted = [target] if target else self._ordered_keys()
        pairs = []
        for key in wanted:
            pair = endpoint(key) if ":" in key else None
            candidates = [pair] if pair else self.service.matching(key)
            for pair in candidates:
                self.service.touch(pair)
                pairs.append(pair)
        # 热缓存路径不等待任何探测；无缓存时只等待一次有界的首次结果。
        if not any(self._record_for(key)[1] for key in wanted):
            remaining = max(0.01, deadline - time.monotonic())
            if target and not pairs and self._is_cn_like_name(target):
                await self.service.discover(target, remaining)
            else:
                await self.service.wait_initial(pairs, remaining)
        wanted = [target] if target else self._ordered_keys()
        blocks = []
        if self.service._closed:
            return None
        stale = False
        for key in wanted:
            display, rec = self._record_for(key)
            state = rec.get("state") if rec else "unknown"
            stale |= state != "fresh"
            if (
                state == "unresponsive"
                and not target
                and not bool(self._cfg("show_offline", True))
            ):
                continue
            blocks.append(self._format_block(display, rec))
        if stale:
            self.service.refresh_wake.set()
        self.service.metrics["replies"] += 1
        self.service.metrics["reply_seconds"] += time.monotonic() - started
        return (
            f"\n{BLOCK_SEP}\n".join(blocks)
            or "当前没有可展示的服务器信息，请稍后重试。"
        )

    async def initialize(self) -> None:
        """每次加载或重载后预热；查询入口另有幂等兜底。"""
        await self.service.start()

    @filter.on_astrbot_loaded()
    async def _on_astrbot_loaded(self) -> None:
        await self.service.start()

    async def terminate(self) -> None:
        await self.service.stop()

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
