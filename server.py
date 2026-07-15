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

            # 1. Получаем session_id через hello
            hello_msg = {
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
            await websocket.send(json.dumps(hello_msg))
            resp = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            data = json.loads(resp)
            session_id = data.get("session_id")
            if not session_id:
                print("❌ Не получен session_id")
                return ""
            print(f"✅ session_id: {session_id}")

            # 2. Инициализация MCP
            init_payload = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "Adapter", "version": "1.0"}
                },
                "id": 1
            }
            await websocket.send(json.dumps({
                "session_id": session_id,
                "type": "mcp",
                "payload": init_payload
            }))
            await asyncio.wait_for(websocket.recv(), timeout=10.0)

            # 3. Вызов search_knowledge
            call_payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": "search_knowledge",
                    "arguments": {"query": query}
                },
                "id": 2
            }
            await websocket.send(json.dumps({
                "session_id": session_id,
                "type": "mcp",
                "payload": call_payload
            }))
            print("📤 search_knowledge отправлен")

            # 4. Чтение ответа
            while True:
                resp = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                data = json.loads(resp)
                if data.get("type") != "mcp":
                    continue
                payload = data.get("payload", {})
                if payload.get("id") == 2:
                    if "error" in payload:
                        print(f"❌ Ошибка: {payload['error']}")
                        return ""
                    result = payload.get("result", {})
                    content = result.get("content", [])
                    fragments = [item.get("text", "") for item in content if isinstance(item, dict) and item.get("text")]
                    if fragments:
                        return "\n\n".join(fragments)
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

# --- FastAPI handlers (без изменений) ---
@app.options("/mcp")
async def options_mcp():
    return Response(status_code=200, headers={
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Accept, mcp-session-id",
        "Access-Control-Expose-Headers": "mcp-session-id",
    })

@app.get("/")
async def root():
    return JSONResponse({"status": "ok", "service": "Xiaozhi Adapter (RAG + Polza)"})

@app.post("/mcp")
async def mcp_handler(request: Request):
    try:
        body = await request.json()
        method = body.get("method")
        session_id = request.headers.get("mcp-session-id")

        if method == "initialize":
            new_session_id = str(uuid.uuid4()).replace("-", "")
            sessions[new_session_id] = {"active": True}
            response = JSONResponse({
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "Xiaozhi Adapter", "version": "1.0"}
                }
            })
            response.headers["mcp-session-id"] = new_session_id
            return response

        if method == "notifications/initialized":
            return Response(status_code=200)

        if not session_id or session_id not in sessions:
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "error": {"code": -32000, "message": "Bad Request: No valid session ID"}
            }, status_code=400)

        if method == "tools/call":
            params = body.get("params", {})
            if params.get("name") == "send_message":
                message = params.get("arguments", {}).get("message", "")
                result = await send_to_xiaozhi(message)
                sse_data = {
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "result": {
                        "content": [{"type": "text", "text": result}],
                        "structuredContent": {"result": result}
                    }
                }
                return Response(
                    content=f"event: message\ndata: {json.dumps(sse_data)}\n\n",
                    media_type="text/event-stream"
                )
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "error": {"code": -32602, "message": f"Unknown tool: {params.get('name')}"}
            }, status_code=400)

        return JSONResponse({
            "jsonrpc": "2.0",
            "id": body.get("id"),
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }, status_code=400)
    except Exception as e:
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": body.get("id") if 'body' in locals() else None,
            "error": {"code": -32603, "message": str(e)}
        }, status_code=500)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
