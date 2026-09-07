"""骑马与砍杀：战团联机服务器查询——网络层。

纯 asyncio + 标准库实现，不依赖 AstrBot，可独立运行自测（`python warband_net.py`）。

数据来源：
- 主服务器列表：TaleWorlds 官方接口 `warbandmain.taleworlds.com/handlerservers.ashx?type=list`，
  返回以 `|` 分隔的 `ip:port`（缺省端口为 7240）。
- 服务器详情：对每台服务器发起 TCP 连接后，服务端会直接推送一段
  `<ServerStats>` XML（含服务器名 / 模块 / 模式 / 地图 / 人数等）；
  部分服务器会在 XML 前附加 HTTP 头，解析时统一截取 XML 起始位置。
"""

from __future__ import annotations

import asyncio
import re
import ssl
import urllib.parse
import xml.etree.ElementTree as ET
from typing import Any

DEFAULT_PORT = 7240

_WS_RE = re.compile(r"\s+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


def clean_stat_name(value: str | None) -> str | None:
    """清理服务器上报的字段文本。

    TaleWorlds 会在中文字符之间插入空格（如“战 场 模 式”“邪 教 祭 祀”），
    需要去掉；英文多词名（如 The Cage）保留单词间隔。

    Args:
        value: 原始字段文本。

    Returns:
        清理后的文本；输入为 None 时返回 None。
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return value
    if _CJK_RE.search(value):
        return _WS_RE.sub("", value)
    return _WS_RE.sub(" ", value).strip()


def parse_master_list(text: str) -> list[tuple[str, int]]:
    """解析主服务器列表文本。

    Args:
        text: 接口返回的原始文本。

    Returns:
        (ip, port) 列表，缺省端口取 DEFAULT_PORT。
    """
    addrs: list[tuple[str, int]] = []
    for item in text.split("|"):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            ip, _, port_s = item.partition(":")
            ip = ip.strip()
            if not ip:
                continue
            try:
                port = int(port_s)
            except ValueError:
                port = DEFAULT_PORT
        else:
            ip, port = item, DEFAULT_PORT
        addrs.append((ip, port))
    return addrs


def parse_server_stats(xml_text: str) -> dict[str, Any] | None:
    """解析 <ServerStats> XML。

    Args:
        xml_text: 服务器返回的原始文本（可能带 HTTP 头）。

    Returns:
        字段字典；解析失败返回 None。字段含 name/module/map_type/map_name/
        map_id/players/max_players/has_password/version。
    """
    idx = xml_text.find("<ServerStats")
    if idx < 0:
        return None
    try:
        root = ET.fromstring(xml_text[idx:])
    except ET.ParseError:
        return None

    def _text(tag: str) -> str | None:
        el = root.find(tag)
        return el.text.strip() if el is not None and el.text else None

    return {
        "name": _text("Name"),
        "module": clean_stat_name(_text("ModuleName")),
        "map_type": clean_stat_name(_text("MapTypeName")),
        "map_name": clean_stat_name(_text("MapName")),
        "map_id": _text("MapID"),
        "players": _text("NumberOfActivePlayers"),
        "max_players": _text("MaxNumberOfPlayers"),
        "has_password": _text("HasPassword"),
        "version": _text("MultiplayerVersionNo"),
    }


async def http_get_text(url: str, timeout: float = 12.0) -> str:
    """极简异步 HTTP GET（支持 HTTPS），返回响应体文本。

    Args:
        url: 请求地址。
        timeout: 整体超时（秒）。

    Returns:
        响应体文本。

    Raises:
        OSError / asyncio.TimeoutError: 网络错误或超时。
    """
    parts = urllib.parse.urlsplit(url)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    ssl_ctx = ssl.create_default_context() if parts.scheme == "https" else None
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(parts.hostname, port, ssl=ssl_ctx),
        timeout=timeout,
    )
    try:
        path = parts.path or "/"
        if parts.query:
            path = f"{path}?{parts.query}"
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {parts.hostname}\r\n"
            "User-Agent: astrbot-warband-status/1.0\r\n"
            "Accept: */*\r\n"
            "Connection: close\r\n\r\n"
        )
        writer.write(request.encode("ascii"))
        await writer.drain()
        data = bytearray()
        while True:
            chunk = await asyncio.wait_for(reader.read(65536), timeout)
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > 8 * 1024 * 1024:
                break
        text = bytes(data).decode("utf-8", "replace")
        if text.startswith("HTTP/"):
            _, _, body = text.partition("\r\n\r\n")
            return body
        return text
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:  # noqa: BLE001,S110 - 关闭连接尽力而为
            pass


async def fetch_server_stats(
    host: str,
    port: int,
    timeout: float = 4.0,
) -> dict[str, Any] | None:
    """连接战团服务器并收取其推送的 <ServerStats> XML。

    Args:
        host: 服务器 IP。
        port: 服务器端口。
        timeout: 单台服务器查询超时（秒）。

    Returns:
        解析后的字段字典；连接失败或解析失败返回 None。
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=max(timeout, 3.0),
        )
    except Exception:  # noqa: BLE001 - 探测失败视为无响应
        return None
    data = bytearray()
    try:
        while True:
            try:
                chunk = await asyncio.wait_for(
                    reader.read(65536),
                    min(2.0, timeout),
                )
            except Exception:  # noqa: BLE001 - 超时/中断视为本次读取结束
                break
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > 200_000:
                break
            if b"</ServerStats>" in data:
                try:
                    more = await asyncio.wait_for(reader.read(65536), 0.4)
                    if more:
                        data.extend(more)
                except Exception:  # noqa: BLE001,S110 - 尾部残余数据尽力而为
                    pass
                break
        return parse_server_stats(bytes(data).decode("utf-8", "replace"))
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:  # noqa: BLE001,S110 - 关闭连接尽力而为
            pass


async def fetch_master_server_list(
    url: str,
    timeout: float = 12.0,
) -> list[tuple[str, int]]:
    """抓取并解析主服务器列表。

    Args:
        url: 主服务器列表接口地址。
        timeout: 抓取超时（秒）。

    Returns:
        (ip, port) 列表。

    Raises:
        OSError / asyncio.TimeoutError: 网络错误或超时。
    """
    text = await http_get_text(url, timeout)
    return parse_master_list(text)


async def probe_many(
    addrs: list[tuple[str, int]],
    timeout: float,
    concurrency: int = 32,
) -> list[tuple[str, int, dict[str, Any] | None]]:
    """并发探测一组服务器。

    Args:
        addrs: (ip, port) 列表。
        timeout: 单台服务器超时（秒）。
        concurrency: 最大并发数。

    Returns:
        (ip, port, 解析结果或 None) 列表，与输入顺序一致。
    """
    sem = asyncio.Semaphore(concurrency)

    async def _one(addr: tuple[str, int]) -> tuple[str, int, dict[str, Any] | None]:
        ip, port = addr
        async with sem:
            stats = await fetch_server_stats(ip, port, timeout)
        return ip, port, stats

    return list(await asyncio.gather(*(_one(a) for a in addrs)))


async def _self_test() -> None:
    """连接真实服务器做链路自测（仅开发用）。"""
    url = "https://warbandmain.taleworlds.com/handlerservers.ashx?type=list"
    print("抓取主服务器列表...")
    addrs = await fetch_master_server_list(url)
    print(f"在线服务器 {len(addrs)} 台")
    seed = {"116.62.36.206"}
    targets = [(ip, port) for ip, port in addrs if ip in seed]
    print(f"种子主机 {seed} 共 {len(targets)} 个地址，开始探测...")
    for ip, port, stats in await probe_many(targets, timeout=4.0):
        if stats and stats.get("name"):
            print(
                f"{ip}:{port} | {stats['name']} | {stats['map_type']} | "
                f"{stats['map_name']} | {stats['players']}/{stats['max_players']} | "
                f"{stats['module']}"
            )
        else:
            print(f"{ip}:{port} | 无响应")


if __name__ == "__main__":
    asyncio.run(_self_test())
