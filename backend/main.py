from fastapi import FastAPI
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Global Bad Day Index API starting...")
    yield

app = FastAPI(
    title="Global Bad Day Index API",
    version="0.1.0",
    lifespan=lifespan
)

@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}
