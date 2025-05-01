import os
import logging
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

# Логгирование
logging.basicConfig(level=logging.INFO)

@app.post("/webhook_v2")
async def webhook_v2(request: Request):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logging.info(f"Принят сигнал v2: {payload}")

    return JSONResponse({"status": "ok"})