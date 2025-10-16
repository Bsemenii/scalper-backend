import pytest
import httpx
from bot.api.app import app

@pytest.mark.asyncio
async def test_root():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/")
    assert r.status_code == 200
    assert r.json().get("ok") is True

@pytest.mark.asyncio
async def test_status():
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        r = await ac.get("/status")
    assert r.status_code == 200
    assert "symbols" in r.json()
