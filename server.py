"""
TCP/IP Chat Server - Багийн ажил №7
WebSocket-based server (runs in browser)
OOP: ChatMessage, ChatHistory, ChatServer
"""

import asyncio
import json
import datetime
import websockets
import sys

# ─── Classes ───────────────────────────────────────────────

class ChatMessage:
    """Мессежийн обьект"""
    def __init__(self, sender: str, content: str, msg_type: str = "message"):
        self.sender = sender
        self.content = content
        self.timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.msg_type = msg_type

    def to_dict(self) -> dict:
        return {
            "type": self.msg_type,
            "sender": self.sender,
            "content": self.content,
            "timestamp": self.timestamp,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def __str__(self):
        return f"[{self.timestamp}] {self.sender}: {self.content}"


class ChatHistory:
    """Чатын түүх хадгалах обьект"""
    def __init__(self, max_size: int = 200):
        self.messages: list[ChatMessage] = []
        self.max_size = max_size

    def add(self, msg: ChatMessage):
        self.messages.append(msg)
        if len(self.messages) > self.max_size:
            self.messages.pop(0)

    def get_recent(self, count: int = 30) -> list[dict]:
        return [m.to_dict() for m in self.messages[-count:]]


class ChatServer:
    """WebSocket чатын сервер"""
    def __init__(self, host: str = "localhost", port: int = 9090):
        self.host = host
        self.port = port
        self.clients: dict = {}   # websocket -> username
        self.history = ChatHistory()

    async def broadcast(self, message: ChatMessage, exclude=None):
        if not self.clients:
            return
        data = message.to_json()
        targets = [ws for ws in self.clients if ws != exclude]
        if targets:
            await asyncio.gather(*[ws.send(data) for ws in targets], return_exceptions=True)

    async def broadcast_all(self, message: ChatMessage):
        if not self.clients:
            return
        data = message.to_json()
        await asyncio.gather(*[ws.send(data) for ws in self.clients], return_exceptions=True)

    async def handle_client(self, websocket):
        username = None
        try:
            # First message = username
            raw = await websocket.recv()
            obj = json.loads(raw)
            username = obj.get("username", "Зочин")
            self.clients[websocket] = username
            print(f"[+] {username} нэгдлээ  (нийт: {len(self.clients)})")

            # Send history
            history_payload = json.dumps({
                "type": "history",
                "messages": self.history.get_recent(30)
            })
            await websocket.send(history_payload)

            # Announce join
            join_msg = ChatMessage("System", f"{username} чатад нэгдлээ!", "join")
            self.history.add(join_msg)
            await self.broadcast(join_msg, exclude=websocket)

            # Online list to everyone
            await self.send_online_list()

            # Message loop
            async for raw in websocket:
                obj = json.loads(raw)
                if obj.get("type") == "message":
                    msg = ChatMessage(username, obj.get("content", ""))
                    print(str(msg))
                    self.history.add(msg)
                    await self.broadcast_all(msg)

        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            print(f"[Error] {e}")
        finally:
            if websocket in self.clients:
                del self.clients[websocket]
            if username:
                print(f"[-] {username} гарлаа  (нийт: {len(self.clients)})")
                leave_msg = ChatMessage("System", f"{username} чатаас гарлаа.", "leave")
                self.history.add(leave_msg)
                await self.broadcast(leave_msg)
                await self.send_online_list()

    async def send_online_list(self):
        payload = json.dumps({
            "type": "online",
            "users": list(self.clients.values())
        })
        if self.clients:
            await asyncio.gather(*[ws.send(payload) for ws in self.clients], return_exceptions=True)

    async def run(self):
        print(f"[Server] Сервер эхэллээ → ws://{self.host}:{self.port}")
        print(f"[Server] index.html-ийг браузерт нээнэ үү")
        print(f"[Server] Зогсоохын тулд Ctrl+C дарна уу\n")
        async with websockets.serve(self.handle_client, self.host, self.port):
            await asyncio.Future()  # run forever


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9090
    server = ChatServer(host, port)
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:
        print("\n[Server] Зогсоолоо.")