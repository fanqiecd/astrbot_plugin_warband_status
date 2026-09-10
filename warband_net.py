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
import functools
import ipaddress
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
    seen: set[tuple[str, int]] = set()
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
                continue
        else:
            ip, port = item, DEFAULT_PORT
        try:
            ip = str(ipaddress.IPv4Address(ip))
        except ValueError:
            continue
        if 0 < port < 65536 and (ip, port) not in seen:
            seen.add((ip, port))
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
        end = xml_text.find("</ServerStats>", idx)
        if end < 0:
            return None
        root = ET.fromstring(xml_text[idx : end + len("</ServerStats>")])
    except ET.ParseError:
        return None

    def _text(tag: str) -> str | None:
        el = root.find(tag)
        return el.text.strip() if el is not None and el.text else None

    if root.tag != "ServerStats" or not _text("Name"):
        return None

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


@functools.lru_cache(maxsize=1)
def _ssl_context() -> ssl.SSLContext:
    """复用证书配置；首次创建在工作线程执行。"""
    return ssl.create_default_context()


def _close(writer: asyncio.StreamWriter) -> None:
    """响应已读取或请求失败后立即释放连接，不等待对端关闭。"""
    writer.close()
    writer.transport.abort()


async def http_get_text(url: str, timeout: float = 12.0) -> str:
    """HTTP/HTTPS GET；连接和完整正文共享总超时，不接受错误状态。"""

    async def request() -> str:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            raise ValueError("主列表地址必须是 HTTP/HTTPS URL")
        if parts.username or parts.password or any(c in url for c in "\r\n"):
            raise ValueError("主列表地址含不支持的认证信息或换行")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        ctx = await asyncio.to_thread(_ssl_context) if parts.scheme == "https" else None
        reader, writer = await asyncio.open_connection(parts.hostname, port, ssl=ctx)
        limit = 8 * 1024 * 1024
        try:
            path = urllib.parse.urlunsplit(("", "", parts.path or "/", parts.query, ""))
            host = parts.netloc
            writer.write(
                (
                    f"GET {path} HTTP/1.1\r\nHost: {host}\r\n"
                    "User-Agent: astrbot-warband-status/1.1\r\n"
                    "Accept: */*\r\nConnection: close\r\n\r\n"
                ).encode("ascii")
            )
            await writer.drain()
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode("iso-8859-1").split("\r\n")
            status = lines[0].split()
            if (
                len(status) < 2
                or not status[0].startswith("HTTP/")
                or status[1] != "200"
            ):
                raise ValueError("主列表 HTTP 状态不是 200")
            headers = {}
            for line in lines[1:]:
                if line:
                    key, sep, value = line.partition(":")
                    if not sep:
                        raise ValueError("无效 HTTP 响应头")
                    key = key.strip().lower()
                    value = value.strip().lower()
                    if key in {"content-length", "transfer-encoding"}:
                        if key in headers and (
                            key == "transfer-encoding" or headers[key] != value
                        ):
                            raise ValueError("冲突的 HTTP 正文分帧响应头")
                        headers[key] = value
            data = bytearray()
            transfer = headers.get("transfer-encoding")
            if transfer:
                if transfer != "chunked":
                    raise ValueError("不支持的 HTTP 传输编码")
                trailer_size = 0
                while True:
                    line = await reader.readuntil(b"\r\n")
                    size = int(line.split(b";", 1)[0].strip(), 16)
                    if size < 0 or len(data) + size > limit:
                        raise ValueError("主列表正文过大")
                    if not size:
                        while True:
                            trailer = await reader.readuntil(b"\r\n")
                            trailer_size += len(trailer)
                            if trailer_size > 65536:
                                raise ValueError("HTTP 尾部过大")
                            if trailer == b"\r\n":
                                break
                        break
                    data.extend(await reader.readexactly(size))
                    if await reader.readexactly(2) != b"\r\n":
                        raise ValueError("无效 HTTP 分块")
            elif "content-length" in headers:
                size = int(headers["content-length"])
                if not 0 <= size <= limit:
                    raise ValueError("主列表正文大小无效")
                data.extend(await reader.readexactly(size))
            else:
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        break
                    data.extend(chunk)
                    if len(data) > limit:
                        raise ValueError("主列表正文过大")
            return data.decode("utf-8", "replace")
        finally:
            _close(writer)

    return await asyncio.wait_for(request(), timeout=max(0.01, timeout))


async def fetch_server_stats(
    host: str,
    port: int,
    timeout: float = 4.0,
) -> dict[str, Any] | None:
    """在总预算内读取完整 XML；收齐即返回，无额外尾部等待。"""

    async def request() -> dict[str, Any] | None:
        reader, writer = await asyncio.open_connection(host, port)
        data = bytearray()
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    return None
                data.extend(chunk)
                if len(data) > 200_000:
                    return None
                if b"</ServerStats>" in data:
                    return parse_server_stats(data.decode("utf-8", "replace"))
        finally:
            _close(writer)

    try:
        return await asyncio.wait_for(request(), timeout=max(0.01, timeout))
    except (OSError, ValueError, asyncio.TimeoutError):
        return None


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
