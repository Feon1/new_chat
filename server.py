import asyncio
import json
import os
import sys
import websockets
from fastmcp import FastMCP
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response
from starlette.requests import Request
import uvicorn
from dotenv import load_dotenv

# ---- Загрузка переменных окружения ----
load_dotenv()

# ---- Создание MCP-сервера ----
mcp = FastMCP("Xiaozhi Direct Adapter")
app = mcp.http_app()

# ---- Настройка CORS ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["mcp-session-id"],
)

# ---- Явный обработчик OPTIONS для /mcp (чтобы избежать 405) ----
async def options_mcp(request: Request):
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Accept, mcp-session-id",
            "Access-Control-Expose-Headers": "mcp-session-id",
        }
    )

app.add_route("/mcp", options_mcp, methods=["OPTIONS"])

# ---- Health check ----
async def root(request: Request):
    return JSONResponse({"status": "ok", "service": "Xiaozhi Adapter"})

app.add_route("/", root, methods=["GET", "HEAD"])

# ---- Диагностика окружения ----
print(f"🐍 Python version: {sys.version}")
print(f"📦 websockets version: {websockets.__version__}")

# ---- Конфигурация Xiaozhi ----
XIAOZHI_WS_URL = os.getenv("XIAOZHI_WS_URL", "wss://api.tenclass.net/xiaozhi/v1/")
XIAOZHI_TOKEN = os.getenv("XIAOZHI_TOKEN", "")
if not XIAOZHI_TOKEN:
    print("⚠️  XIAOZHI_TOKEN не задан!")
else:
    print(f"✅ XIAOZHI_TOKEN загружен: {XIAOZHI_TOKEN[:10]}...")

DEVICE_ID = os.getenv("DEVICE_ID", "e0:2e:0b:ae:79:ea")
CLIENT_ID = os.getenv("CLIENT_ID", "9cc3e5e4-adcf-4eff-8d23-95d4eaa21020")
print(f"📱 Device ID: {DEVICE_ID}")
print(f"📱 Client ID: {CLIENT_ID}")

# ---- Функция отправки сообщения в Xiaozhi ----
async def send_to_xiaozhi(message: str) -> str:
    print(f"📨 send_to_xiaozhi called with: {message}")
    headers = {
        "Device-Id": DEVICE_ID,
        "Client-Id": CLIENT_ID,
        "Protocol-Version": "1",
    }
    ws_url = f"{XIAOZHI_WS_URL}?token={XIAOZHI_TOKEN}"
    print(f"🔗 Connecting to: {ws_url[:60]}...")

    try:
        # Универсальный способ: передаём заголовки как список кортежей
        headers_list = list(headers.items())
        async with websockets.connect(ws_url, extra_headers=headers_list) as websocket:
            print("✅ WebSocket connected to Xiaozhi")
            hello = {
                "type": "hello",
                "version": 1,
                "transport": "websocket",
                "audio_params": {
                    "format": "opus",
                    "sample_rate": 16000,
                    "channels": 1,
                    "frame_duration": 60
                }
            }
            await websocket.send(json.dumps(hello))
            print("📤 Hello sent")

            try:
                resp = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                print(f"📩 Received: {resp[:100]}...")
                data = json.loads(resp)
                if data.get("type") != "hello":
                    return f"Ошибка: ожидался hello, получено {data.get('type')}"
                session_id = data.get("session_id")
                if not session_id:
                    return "Ошибка: не получен session_id"
                print(f"✅ Получен session_id: {session_id}")
            except asyncio.TimeoutError:
                return "⏰ Таймаут: сервер не ответил на hello"
            except Exception as e:
                return f"❌ Ошибка при получении hello: {e}"

            text_msg = {
                "type": "listen",
                "state": "detect",
                "text": message,
                "source": "text"
            }
            await websocket.send(json.dumps(text_msg))
            print("📤 Text message sent")

            full_reply = ""
            while True:
                raw = await websocket.recv()
                if isinstance(raw, bytes):
                    print("📩 Бинарные данные (аудио) пропущены")
                    continue
                try:
                    data = json.loads(raw)
                    print(f"📩 JSON: {data}")
                except json.JSONDecodeError:
                    continue
                msg_type = data.get("type")
                if msg_type == "stt":
                    continue
                elif msg_type == "llm":
                    if "text" in data and data["text"].strip():
                        full_reply += data["text"]
                elif msg_type == "tts":
                    if data.get("state") == "sentence_start":
                        if "text" in data and data["text"].strip():
                            full_reply += data["text"]
                    elif data.get("state") in ("end", "stop"):
                        break
                elif msg_type == "error":
                    return f"Ошибка от Xiaozhi: {data.get('message', 'неизвестная')}"
            print(f"✅ Full reply: {full_reply[:100]}...")
            return full_reply if full_reply else "Ответ не получен"

    except Exception as e:
        print(f"❌ Ошибка подключения к Xiaozhi: {e}")
        return f"❌ Ошибка подключения к Xiaozhi: {e}"

# ---- Инструмент MCP ----
@mcp.tool()
def send_message(message: str) -> str:
    print(f"🔧 send_message вызван с: {message}")
    return asyncio.run(send_to_xiaozhi(message))

# ---- Тестовый инструмент (для диагностики) ----
@mcp.tool()
def ping() -> str:
    return "pong"

# ---- Вывод зарегистрированных инструментов ----
print("📋 Зарегистрированные инструменты:")
if hasattr(mcp, '_tools'):
    for tool in mcp._tools:
        print(f"  - {tool.name}: {tool.description}")
else:
    print("  (список не получен)")

print("✅ Инициализация завершена, запускаю сервер...")

# ---- Запуск ----
if __name__ == "__main__":
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
