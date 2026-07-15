import asyncio
import json
import os
import uuid
import websockets
from openai import AsyncOpenAI
from fastapi import FastAPI, Request
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, Response
import uvicorn
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["mcp-session-id"],
)

sessions = {}

XIAOZHI_MCP_URL = os.getenv("XIAOZHI_MCP_URL", "wss://api.xiaozhi.me/mcp/")
XIAOZHI_MCP_TOKEN = os.getenv("XIAOZHI_MCP_TOKEN", "")
if not XIAOZHI_MCP_TOKEN:
    print("⚠️ XIAOZHI_MCP_TOKEN не задан!")
else:
    print("✅ XIAOZHI_MCP_TOKEN загружен")

POLZA_API_KEY = os.getenv("POLZA_API_KEY", "")
POLZA_BASE_URL = "https://polza.ai/api/v1"
POLZA_MODEL = "deepseek/deepseek-v4-flash"

polza_client = None
if POLZA_API_KEY:
    polza_client = AsyncOpenAI(api_key=POLZA_API_KEY, base_url=POLZA_BASE_URL)

async def call_mcp_search_knowledge(query: str) -> str:
    if not XIAOZHI_MCP_TOKEN:
        return ""

    ws_url = f"{XIAOZHI_MCP_URL}?token={XIAOZHI_MCP_TOKEN}"
    print(f"🔗 Подключение: {ws_url[:80]}...")

    try:
        async with websockets.connect(ws_url) as websocket:
            print("✅ WebSocket подключен")

            # 1. Отправить initialize
            init_msg = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "Adapter", "version": "1.0"}
                },
                "id": 1
            }
            await websocket.send(json.dumps(init_msg))
            print("📤 initialize отправлен")
            try:
                resp = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                print(f"📩 Ответ на initialize: {resp[:200]}")
            except asyncio.TimeoutError:
                print("⏰ Таймаут initialize")
                return ""

            # 2. Отправить notifications/initialized
            notify_msg = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {}
            }
            await websocket.send(json.dumps(notify_msg))
            print("📤 notifications/initialized отправлен")

            # 3. Вызвать search_knowledge
            call_msg = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "search_knowledge",
                    "arguments": {"query": query}
                },
                "id": 2
            }
            await websocket.send(json.dumps(call_msg))
            print("📤 tools/call (search_knowledge) отправлен")

            # 4. Читаем ответы
            while True:
                try:
                    resp = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                except asyncio.TimeoutError:
                    print("⏰ Таймаут ожидания ответа search_knowledge")
                    break
                try:
                    data = json.loads(resp)
                    print(f"📩 Получено: {data}")
                except json.JSONDecodeError:
                    continue
                # Проверяем, что это ответ на наш вызов (id=2)
                if data.get("id") == 2:
                    if "error" in data:
                        print(f"❌ Ошибка: {data['error']}")
                        return ""
                    result = data.get("result", {})
                    content = result.get("content", [])
                    fragments = [item.get("text", "") for item in content if isinstance(item, dict) and item.get("text")]
                    if fragments:
                        return "\n\n".join(fragments)
                    return ""
                else:
                    # Игнорируем другие сообщения
                    continue
            return ""
    except Exception as e:
        print(f"⚠️ Ошибка: {e}")
        return ""

async def call_polza(prompt: str, context: str) -> str:
    if not context or not context.strip():
        return "❌ Не удалось найти информацию в базе знаний."
    system = "Ты — полезный ассистент. Отвечай, используя контекст.\n\nКонтекст:\n" + context
    try:
        response = await polza_client.chat.completions.create(
            model=POLZA_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": prompt}
            ],
            temperature=0.6,
            max_tokens=2000,
        )
        return response.choices[0].message.content or "Ответ не получен"
    except Exception as e:
        return f"⚠️ Ошибка Polza: {e}"

async def send_to_xiaozhi(message: str) -> str:
    print(f"📨 Запрос: {message[:100]}...")
    if not XIAOZHI_MCP_TOKEN:
        return "⚠️ XIAOZHI_MCP_TOKEN не задан!"
    context = await call_mcp_search_knowledge(message)
    return await call_polza(message, context)

# ... (остальной код FastAPI без изменений)
