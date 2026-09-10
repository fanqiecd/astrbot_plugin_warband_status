"""使用已安装的真实 AstrBot API 做隔离加载与事件验证，不发送平台消息。"""

import asyncio
import logging
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace

from tests.test_warband import XML, LocalServer


async def verify():
    from astrbot.api.event import AstrMessageEvent
    from astrbot.api.platform import AstrBotMessage, PlatformMetadata
    from astrbot.core.platform.astrbot_message import MessageMember
    from astrbot.core.platform.message_type import MessageType

    import main

    async def handler(reader, writer):
        writer.write(XML)
        await writer.drain()
        await asyncio.sleep(1)

    async def master(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        body = f"127.0.0.1:{game.port}".encode()
        writer.write(
            f"HTTP/1.1 200 OK\r\nContent-Length: {len(body)}\r\n\r\n".encode() + body
        )
        await writer.drain()

    def event(text):
        message = AstrBotMessage()
        message.type = MessageType.GROUP_MESSAGE
        message.group_id = "test-group"
        message.sender = MessageMember("test-user")
        message.self_id = "test-bot"
        message.message = []
        message.message_str = text
        return AstrMessageEvent(
            text,
            message,
            PlatformMetadata("aiocqhttp", "offline test", "test"),
            "test-session",
        )

    async with LocalServer(handler) as game, LocalServer(master) as directory:
        config = {
            "servers": ["CN_Test"],
            "extra_endpoints": [f"127.0.0.1:{game.port}"],
            "seed_hosts": [],
            "master_url": f"http://127.0.0.1:{directory.port}/",
        }
        plugin = main.WarbandServerStatusPlugin(SimpleNamespace(), config)
        await plugin.initialize()
        try:
            results = [r async for r in plugin.cmd_query(event("查服"), "")]
            assert len(results) == 1
            assert (
                "7/0" in results[0].chain[0].text
                or "7/未知" in results[0].chain[0].text
            )
            results = [r async for r in plugin.keyword_query(event("查服CN_Test"))]
            assert len(results) == 1 and "CN_Test" in results[0].chain[0].text
            config["enable_group"] = False
            assert [r async for r in plugin.keyword_query(event("查服CN_Test"))] == []
            config["enable_group"] = True
        finally:
            await plugin.terminate()
        assert not plugin.service._tasks
        assert plugin.service.path.exists()
        restored = main.WarbandServerStatusPlugin(SimpleNamespace(), config)
        await restored.initialize()
        try:
            assert any(r.restored for r in restored.service.records.values())
        finally:
            await restored.terminate()
        print(
            "PASS: real AstrBot API load, command, keyword, permission gate, unload and reload"
        )


if __name__ == "__main__":
    root = Path.cwd()
    with tempfile.TemporaryDirectory() as directory:
        os.chdir(directory)
        try:
            asyncio.run(verify())
        finally:
            logging.shutdown()
            os.chdir(root)
