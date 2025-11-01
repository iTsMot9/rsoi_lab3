import pytest
from main import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_health():
    response = client.get("/manage/health")
    assert response.status_code == 200
    assert response.json() == {"status": "OK"}

def test_create_payment():
    response = client.post("/api/v1/payment", json={"price": 1000})
    assert response.status_code == 200
    data = response.json()
    assert "paymentUid" in data
    assert data["status"] == "PAID"
    assert data["price"] == 1000
